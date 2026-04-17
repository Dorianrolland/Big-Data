"""Smoke test end-to-end for local FleetStream copilot stack.

Checks:
- Docker stack is reachable (optional auto-start)
- API health/cold path/hot path
- realtime offers and zone recommendations
- replay query from cold parquet
- latency KPI on /copilot/score-offer
- DLQ file count

Writes a JSON report under data/reports/.
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
EVENTS_DIR = DATA_DIR / "parquet_events"
DLQ_DIR = DATA_DIR / "dlq"
REPORTS_DIR = DATA_DIR / "reports"


def percentile(values: list[float], pct: int) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = max(0, min(len(ordered) - 1, math.ceil(len(ordered) * pct / 100) - 1))
    return round(ordered[idx], 3)


def run_cmd(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        cmd,
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
    )
    if check and result.returncode != 0:
        stderr = (result.stderr or "").strip()
        stdout = (result.stdout or "").strip()
        details = stderr or stdout or "unknown error"
        raise RuntimeError(f"command failed ({' '.join(cmd)}): {details}")
    return result


def request_json(url: str, method: str = "GET", payload: dict[str, Any] | None = None, timeout: float = 10.0) -> dict:
    data = None
    headers: dict[str, str] = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, method=method, data=data, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as response:
        body = response.read().decode("utf-8")
        return json.loads(body) if body else {}


def wait_for_json(url: str, predicate, timeout_s: int, step_s: float = 2.0) -> dict:
    deadline = time.time() + timeout_s
    last_error: str | None = None

    while time.time() < deadline:
        try:
            payload = request_json(url)
            if predicate(payload):
                return payload
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
        time.sleep(step_s)

    raise TimeoutError(f"timeout waiting for {url}. last_error={last_error}")


def count_parquet_files(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for _ in path.rglob("*.parquet"))


def dlq_stats(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"files": 0, "size_mb": 0.0}

    files = list(path.rglob("*.jsonl"))
    total_bytes = sum(f.stat().st_size for f in files if f.exists())
    return {
        "files": len(files),
        "size_mb": round(total_bytes / (1024 * 1024), 4),
    }


def benchmark_score_offer(base_url: str, requests_n: int, concurrency: int, timeout: float) -> dict[str, Any]:
    endpoint = f"{base_url.rstrip('/')}/copilot/score-offer"
    payload_bytes = json.dumps(
        {
            "estimated_fare_eur": 14.2,
            "estimated_distance_km": 3.4,
            "estimated_duration_min": 18.0,
            "demand_index": 1.4,
            "supply_index": 0.8,
            "weather_factor": 1.0,
            "traffic_factor": 1.1,
        }
    ).encode("utf-8")

    latencies_ms: list[float] = []
    errors = 0

    def one_call() -> float:
        req = urllib.request.Request(
            endpoint,
            method="POST",
            data=payload_bytes,
            headers={"Content-Type": "application/json"},
        )
        t0 = time.perf_counter()
        with urllib.request.urlopen(req, timeout=timeout) as response:
            _ = response.read()
        return (time.perf_counter() - t0) * 1000

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        futures = [pool.submit(one_call) for _ in range(max(1, requests_n))]
        for fut in as_completed(futures):
            try:
                latencies_ms.append(fut.result())
            except Exception:  # noqa: BLE001
                errors += 1

    return {
        "requests": requests_n,
        "success": len(latencies_ms),
        "errors": errors,
        "p50_ms": percentile(latencies_ms, 50),
        "p95_ms": percentile(latencies_ms, 95),
        "p99_ms": percentile(latencies_ms, 99),
        "mean_ms": round(statistics.mean(latencies_ms), 3) if latencies_ms else 0.0,
    }


def compose_up(build: bool) -> None:
    cmd = ["docker", "compose", "up", "-d"]
    if build:
        cmd.append("--build")
    run_cmd(cmd, check=True)


def compose_down() -> None:
    run_cmd(["docker", "compose", "down"], check=True)


def ensure_docker_available() -> None:
    try:
        run_cmd(["docker", "version"], check=True)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            "Docker indisponible. Lance Docker Desktop puis reessaie."
        ) from exc


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke E2E local FleetStream Copilot")
    parser.add_argument("--url", default="http://localhost:8001", help="base URL API")
    parser.add_argument(
        "--driver",
        default=None,
        help="driver id used for offers/replay (default: auto-discover from /livreurs-proches)",
    )
    parser.add_argument("--up", action="store_true", help="run docker compose up -d before checks")
    parser.add_argument("--build", action="store_true", help="with --up, also rebuild images")
    parser.add_argument("--down", action="store_true", help="run docker compose down after checks")
    parser.add_argument("--timeout", type=int, default=180, help="max wait time for dynamic checks")
    parser.add_argument("--score-requests", type=int, default=240)
    parser.add_argument("--score-concurrency", type=int, default=30)
    parser.add_argument("--score-timeout", type=float, default=10.0)
    args = parser.parse_args()

    started_at = datetime.now(timezone.utc)
    base_url = args.url.rstrip("/")

    try:
        ensure_docker_available()

        if args.up:
            compose_up(build=args.build)

        api_health = wait_for_json(
            f"{base_url}/health",
            lambda x: x.get("status") == "ok",
            timeout_s=args.timeout,
        )

        copilot_health = wait_for_json(
            f"{base_url}/copilot/health",
            lambda x: "feature_columns" in x,
            timeout_s=args.timeout,
        )

        driver_id = args.driver
        if driver_id is None:
            nearby = wait_for_json(
                f"{base_url}/livreurs-proches?lat=40.7580&lon=-73.9855&rayon=10",
                lambda x: len(x.get("livreurs", [])) > 0,
                timeout_s=args.timeout,
            )
            driver_id = nearby["livreurs"][0]["livreur_id"]
            print(f"auto-discovered driver_id={driver_id}")

        offers_payload = wait_for_json(
            f"{base_url}/copilot/driver/{driver_id}/offers?limit=20",
            lambda x: int(x.get("count", 0)) > 0,
            timeout_s=args.timeout,
        )

        zone_payload = wait_for_json(
            f"{base_url}/copilot/driver/{driver_id}/next-best-zone?top_k=5",
            lambda x: isinstance(x, dict),
            timeout_s=args.timeout,
        )

        # Wait until cold path has at least one parquet file in events lake.
        deadline = time.time() + args.timeout
        event_parquet_files = count_parquet_files(EVENTS_DIR)
        while event_parquet_files == 0 and time.time() < deadline:
            time.sleep(2)
            event_parquet_files = count_parquet_files(EVENTS_DIR)

        # Anchor replay window on virtual replay clock (offer ts), not real wall clock.
        sample_offer = offers_payload.get("offers", [{}])[0]
        offer_ts_str = sample_offer.get("ts")
        if offer_ts_str:
            offer_ts = datetime.fromisoformat(offer_ts_str)
            if offer_ts.tzinfo is None:
                offer_ts = offer_ts.replace(tzinfo=timezone.utc)
            replay_to_ts = offer_ts + timedelta(minutes=10)
            replay_from_ts = offer_ts - timedelta(minutes=10)
        else:
            replay_to_ts = datetime.now(timezone.utc)
            replay_from_ts = replay_to_ts - timedelta(minutes=20)

        replay_url = (
            f"{base_url}/copilot/replay?"
            f"from={urllib.parse.quote(replay_from_ts.isoformat())}&"
            f"to={urllib.parse.quote(replay_to_ts.isoformat())}&"
            f"limit=100"
        )
        replay_payload = wait_for_json(
            replay_url,
            lambda x: isinstance(x, dict),
            timeout_s=max(30, args.timeout),
        )

        hot_perf = wait_for_json(
            f"{base_url}/health/performance?samples=200",
            lambda x: isinstance(x, dict),
            timeout_s=args.timeout,
        )
        score_perf = benchmark_score_offer(
            base_url=base_url,
            requests_n=args.score_requests,
            concurrency=args.score_concurrency,
            timeout=args.score_timeout,
        )
        stats_payload = wait_for_json(
            f"{base_url}/stats",
            lambda x: isinstance(x, dict),
            timeout_s=args.timeout,
        )
        dlq = dlq_stats(DLQ_DIR)

        checks = {
            "api_health_ok": api_health.get("status") == "ok",
            "offers_available": int(offers_payload.get("count", 0)) > 0,
            "zones_available": int(zone_payload.get("count", 0)) > 0,
            "events_parquet_written": event_parquet_files > 0,
            "replay_available": int(replay_payload.get("count", 0)) > 0,
            "model_quality_gate_exposed": isinstance(copilot_health.get("model_quality_gate"), dict),
            "model_gate_consistent_with_loaded": bool(copilot_health.get("model_loaded", False))
            == bool((copilot_health.get("model_quality_gate") or {}).get("accepted", False)),
            "hot_path_p99_lt_10ms": float(hot_perf.get("geosearch_benchmark", {}).get("p99_ms", 9999)) < 10.0,
            "score_offer_p95_lt_150ms": float(score_perf.get("p95_ms", 9999)) < 150.0,
            "score_offer_no_errors": int(score_perf.get("errors", 1)) == 0,
        }
        passed = all(checks.values())

        finished_at = datetime.now(timezone.utc)
        report = {
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_s": round((finished_at - started_at).total_seconds(), 2),
            "base_url": base_url,
            "driver_id": driver_id,
            "checks": checks,
            "passed": passed,
            "kpi": {
                "hot_path": hot_perf.get("geosearch_benchmark", {}),
                "score_offer": score_perf,
                "offers_count": offers_payload.get("count", 0),
                "replay_count": replay_payload.get("count", 0),
                "events_parquet_files": event_parquet_files,
                "model_loaded": bool(copilot_health.get("model_loaded", False)),
                "model_quality_gate": copilot_health.get("model_quality_gate", {}),
                "dlq": dlq,
            },
            "snapshots": {
                "api_health": api_health,
                "copilot_health": copilot_health,
                "stats": stats_payload,
            },
        }

        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        report_path = REPORTS_DIR / f"smoke_e2e_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

        print("Smoke E2E result:")
        print(f"  passed: {passed}")
        for key, value in checks.items():
            print(f"  {key}: {value}")
        print(f"  hot_path_p99_ms: {hot_perf.get('geosearch_benchmark', {}).get('p99_ms')}")
        print(f"  score_offer_p95_ms: {score_perf.get('p95_ms')}")
        print(f"  report: {report_path}")

        return 0 if passed else 2

    except urllib.error.HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8")[:400]
        except Exception:  # noqa: BLE001
            body = ""
        details = f" | url={getattr(exc, 'url', 'unknown')}"
        if body:
            details += f" | body={body}"
        print(f"HTTP error: {exc.code} {exc.reason}{details}", file=sys.stderr)
        return 3
    except Exception as exc:  # noqa: BLE001
        print(f"Smoke E2E failed: {exc}", file=sys.stderr)
        return 3
    finally:
        if args.down:
            try:
                compose_down()
            except Exception as exc:  # noqa: BLE001
                print(f"Warning: docker compose down failed: {exc}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
