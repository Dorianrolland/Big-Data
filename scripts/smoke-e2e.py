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
import os
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
DEFAULT_COMPOSE_ENV_FILE = PROJECT_ROOT / "env" / "fleet_demo.env"
DEFAULT_FALLBACK_DRIVER_IDS = ("drv_demo_001", "L001")
LOCAL_HTTP_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))
REQUIRED_DEMO_SERVICES = {"api", "hot-consumer", "tlc-replay", "redis", "redpanda"}


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


def _parse_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _read_env_file_values(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    out: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip()
    return out


def resolve_compose_env_file(compose_env_file: str | None = None) -> str:
    candidate = str(compose_env_file or "").strip()
    if not candidate:
        candidate = str(os.getenv("COPILOT_SMOKE_COMPOSE_ENV_FILE", "")).strip()
    if not candidate and DEFAULT_COMPOSE_ENV_FILE.exists():
        candidate = str(DEFAULT_COMPOSE_ENV_FILE)
    if not candidate:
        return ""
    path = Path(candidate)
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    return str(path)


def resolve_default_min_drivers(compose_env_file: str | None = None) -> int:
    env_value = str(os.getenv("FLEET_DEMO_WARMUP_MIN_DRIVERS", "")).strip()
    if env_value:
        return max(0, _parse_int(env_value, 1))

    resolved = resolve_compose_env_file(compose_env_file)
    if resolved:
        file_value = _read_env_file_values(Path(resolved)).get("FLEET_DEMO_WARMUP_MIN_DRIVERS", "")
        if file_value:
            return max(0, _parse_int(file_value, 1))
    return 1


def running_compose_services(compose_env_file: str | None = None) -> set[str]:
    cmd = ["docker", "compose"]
    resolved_env = resolve_compose_env_file(compose_env_file)
    if resolved_env:
        cmd.extend(["--env-file", resolved_env])
    cmd.extend(["ps", "--services", "--filter", "status=running"])
    try:
        result = run_cmd(cmd, check=False)
    except Exception:
        return set()
    if result.returncode != 0:
        return set()
    return {line.strip() for line in (result.stdout or "").splitlines() if line.strip()}


def allow_replay_active_fallback() -> bool:
    raw = str(os.getenv("COPILOT_ALLOW_REPLAY_ACTIVE_FALLBACK", "0") or "0").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def request_json(url: str, method: str = "GET", payload: dict[str, Any] | None = None, timeout: float = 10.0) -> dict:
    data = None
    headers: dict[str, str] = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, method=method, data=data, headers=headers)
    with LOCAL_HTTP_OPENER.open(req, timeout=timeout) as response:
        body = response.read().decode("utf-8")
        return json.loads(body) if body else {}


def wait_for_json(
    url: str,
    predicate,
    timeout_s: int,
    step_s: float = 2.0,
    request_timeout_s: float = 10.0,
) -> dict:
    deadline = time.time() + timeout_s
    last_error: str | None = None

    while time.time() < deadline:
        try:
            payload = request_json(url, timeout=request_timeout_s)
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


def fallback_driver_ids() -> list[str]:
    candidates: list[str] = []
    raw = str(os.getenv("COPILOT_SMOKE_DRIVER_FALLBACKS", "")).strip()
    if raw:
        candidates.extend(token.strip() for token in raw.split(",") if token.strip())

    single_driver = str(os.getenv("TLC_SINGLE_DRIVER_ID", "")).strip()
    if single_driver:
        candidates.append(single_driver)

    candidates.extend(DEFAULT_FALLBACK_DRIVER_IDS)

    deduped: list[str] = []
    for item in candidates:
        if item not in deduped:
            deduped.append(item)
    return deduped


def driver_has_offers(base_url: str, driver_id: str, timeout_s: float = 10.0) -> bool:
    url = f"{base_url.rstrip('/')}/copilot/driver/{urllib.parse.quote(str(driver_id))}/offers?limit=1"
    payload = request_json(url, timeout=timeout_s)
    if int(payload.get("count", 0)) > 0:
        return True
    offers = payload.get("offers")
    return isinstance(offers, list) and len(offers) > 0


def discover_driver_id(base_url: str, timeout_s: int) -> str:
    nearby_payload: dict[str, Any] = {}
    nearby_url = f"{base_url.rstrip('/')}/livreurs-proches?lat=40.7580&lon=-73.9855&rayon=10"
    try:
        nearby_payload = wait_for_json(
            nearby_url,
            lambda x: isinstance(x, dict),
            timeout_s=max(8, min(int(timeout_s), 45)),
            request_timeout_s=15.0,
        )
    except Exception:
        nearby_payload = {}

    nearby_ids: list[str] = []
    for row in nearby_payload.get("livreurs", []) if isinstance(nearby_payload, dict) else []:
        if not isinstance(row, dict):
            continue
        driver_id = str(row.get("livreur_id", "")).strip()
        if driver_id and driver_id not in nearby_ids:
            nearby_ids.append(driver_id)

    for driver_id in nearby_ids:
        try:
            if driver_has_offers(base_url, driver_id, timeout_s=8.0):
                return driver_id
        except Exception:
            continue

    fallback_ids = fallback_driver_ids()
    for fallback_id in fallback_ids:
        if fallback_id in nearby_ids:
            continue
        try:
            if driver_has_offers(base_url, fallback_id, timeout_s=8.0):
                return fallback_id
        except Exception:
            continue

    if nearby_ids:
        return nearby_ids[0]
    if fallback_ids:
        return fallback_ids[0]
    raise RuntimeError(
        "unable to auto-discover driver_id: no nearby drivers and no fallback driver configured"
    )


def wait_for_active_drivers(base_url: str, min_drivers: int, timeout_s: int) -> dict[str, Any]:
    if min_drivers <= 0:
        return {}
    deadline = time.time() + timeout_s
    peak_active = 0
    while time.time() < deadline:
        active = active_couriers_count(base_url, timeout_s=12.0)
        peak_active = max(peak_active, active)
        if peak_active >= int(min_drivers):
            return {"active_couriers": peak_active}
        time.sleep(2.0)
    raise TimeoutError(
        f"timeout waiting active_couriers >= {min_drivers} (peak_active={peak_active})"
    )


def active_couriers_count(base_url: str, timeout_s: float = 10.0) -> int:
    def _safe_int(value: Any) -> int:
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            return 0

    active = 0
    stats_url = f"{base_url.rstrip('/')}/stats"
    try:
        stats_payload = request_json(stats_url, timeout=min(float(timeout_s), 12.0))
        active = _safe_int(stats_payload.get("active_couriers", 0))
    except Exception:
        active = 0

    if active > 0:
        return active

    nearby_url = (
        f"{base_url.rstrip('/')}/livreurs-proches"
        "?lat=40.7580&lon=-73.9855&rayon=20&limit=500"
    )
    try:
        nearby_payload = request_json(nearby_url, timeout=min(float(timeout_s), 12.0))
    except Exception:
        nearby_payload = {}
    rows = nearby_payload.get("livreurs")
    if isinstance(rows, list):
        active = max(active, len(rows))
    if active > 0:
        return active

    if not allow_replay_active_fallback():
        return active

    health_url = f"{base_url.rstrip('/')}/copilot/health"
    try:
        health_payload = request_json(health_url, timeout=min(float(timeout_s), 15.0))
    except Exception:
        return active
    replay_payload = (health_payload.get("tlc_replay") or {}) if isinstance(health_payload, dict) else {}
    active_trips = _safe_int(replay_payload.get("active_trips", 0))
    return max(active, active_trips)


def parse_offer_ts(offer_ts_str: str | None) -> datetime | None:
    if not offer_ts_str:
        return None
    try:
        parsed = datetime.fromisoformat(str(offer_ts_str))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def compute_replay_window(offer_ts_str: str | None) -> tuple[datetime, datetime]:
    offer_ts = parse_offer_ts(offer_ts_str)
    if offer_ts is not None:
        return offer_ts - timedelta(minutes=5), offer_ts + timedelta(minutes=5)
    replay_to_ts = datetime.now(timezone.utc)
    replay_from_ts = replay_to_ts - timedelta(minutes=10)
    return replay_from_ts, replay_to_ts


def _normalize_limit_candidates(limit: int, candidates: list[int] | None = None) -> list[int]:
    out: list[int] = []
    raw = candidates if candidates else [int(limit)]
    for value in raw:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            continue
        if parsed <= 0:
            continue
        if parsed in out:
            continue
        out.append(parsed)
    return out or [max(1, int(limit))]


def build_replay_url(
    *,
    base_url: str,
    replay_from_ts: datetime,
    replay_to_ts: datetime,
    driver_id: str | None,
    limit: int = 50,
) -> str:
    params = {
        "from": replay_from_ts.isoformat(),
        "to": replay_to_ts.isoformat(),
        "limit": str(int(limit)),
    }
    if driver_id:
        params["driver_id"] = str(driver_id)
    return f"{base_url.rstrip('/')}/copilot/replay?{urllib.parse.urlencode(params)}"


def fetch_replay_payload(
    *,
    base_url: str,
    driver_id: str,
    offer_ts_str: str | None,
    virtual_ts_str: str | None = None,
    timeout_s: int,
    limit: int = 50,
    limit_candidates: list[int] | None = None,
) -> tuple[dict[str, Any], str, tuple[datetime, datetime], str]:
    windows: list[tuple[str, tuple[datetime, datetime], str | None]] = [
        ("offer_ts_window", compute_replay_window(offer_ts_str), str(driver_id)),
    ]
    virtual_ts = parse_offer_ts(virtual_ts_str)
    if virtual_ts is not None:
        virtual_window = (virtual_ts - timedelta(minutes=5), virtual_ts + timedelta(minutes=5))
        windows.append(("virtual_clock_window", virtual_window, str(driver_id)))
        windows.append(("virtual_clock_global_window", virtual_window, None))
    windows.append(("recent_window", compute_replay_window(None), str(driver_id)))
    replay_limits = _normalize_limit_candidates(limit, limit_candidates)
    total_attempts = max(1, len(windows) * len(replay_limits))
    per_attempt_timeout = max(12, min(45, int(max(timeout_s, 12) / total_attempts)))
    request_timeout = max(8.0, min(20.0, float(per_attempt_timeout)))
    attempted_errors: list[str] = []
    seen_urls: set[str] = set()
    last_payload: dict[str, Any] = {}
    last_url = ""
    last_window = windows[-1][1]
    last_strategy = windows[-1][0]

    for strategy, (replay_from_ts, replay_to_ts), replay_driver in windows:
        for replay_limit in replay_limits:
            replay_url = build_replay_url(
                base_url=base_url,
                replay_from_ts=replay_from_ts,
                replay_to_ts=replay_to_ts,
                driver_id=replay_driver,
                limit=replay_limit,
            )
            if replay_url in seen_urls:
                continue
            seen_urls.add(replay_url)
            strategy_key = strategy if replay_limit == replay_limits[0] else f"{strategy}_limit_{replay_limit}"
            try:
                payload = wait_for_json(
                    replay_url,
                    lambda x: isinstance(x, dict),
                    timeout_s=per_attempt_timeout,
                    request_timeout_s=request_timeout,
                )
                last_payload = payload if isinstance(payload, dict) else {}
                last_url = replay_url
                last_window = (replay_from_ts, replay_to_ts)
                last_strategy = strategy_key
                if int(last_payload.get("count", 0) or 0) > 0:
                    return last_payload, replay_url, (replay_from_ts, replay_to_ts), strategy_key
                attempted_errors.append(f"{strategy_key}: empty_replay_count")
            except Exception as exc:  # noqa: BLE001
                attempted_errors.append(f"{strategy_key}: {exc}")

    if last_payload:
        return last_payload, last_url, last_window, last_strategy

    details = " | ".join(attempted_errors) if attempted_errors else "no replay attempts"
    raise RuntimeError(f"replay query failed after fallback windows: {details}")


def summarize_offer_quality(offers_payload: dict[str, Any]) -> dict[str, Any]:
    def safe_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    offers = [item for item in (offers_payload.get("offers") or []) if isinstance(item, dict)]
    if not offers:
        return {
            "offers_accept_count": 0,
            "offers_reject_count": 0,
            "offers_unknown_decision_count": 0,
            "offers_accept_rate_pct": 0.0,
            "offers_avg_accept_score": 0.0,
            "offers_avg_eur_per_hour": 0.0,
            "top_offer_accept_score": None,
            "top_offer_eur_per_hour": None,
        }

    accept_count = sum(1 for item in offers if str(item.get("decision", "")).lower() == "accept")
    reject_count = sum(1 for item in offers if str(item.get("decision", "")).lower() == "reject")
    unknown_count = len(offers) - accept_count - reject_count
    accept_scores = [safe_float(item.get("accept_score"), 0.0) for item in offers]
    eur_per_hour = [safe_float(item.get("eur_per_hour_net"), 0.0) for item in offers]
    top_offer = max(offers, key=lambda item: safe_float(item.get("accept_score"), float("-inf")))
    count = len(offers)

    return {
        "offers_accept_count": int(accept_count),
        "offers_reject_count": int(reject_count),
        "offers_unknown_decision_count": int(unknown_count),
        "offers_accept_rate_pct": round((accept_count / count) * 100.0, 2),
        "offers_avg_accept_score": round(sum(accept_scores) / count, 4),
        "offers_avg_eur_per_hour": round(sum(eur_per_hour) / count, 3),
        "top_offer_accept_score": round(safe_float(top_offer.get("accept_score"), 0.0), 4),
        "top_offer_eur_per_hour": round(safe_float(top_offer.get("eur_per_hour_net"), 0.0), 3),
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
    submitted = max(1, int(requests_n))

    def one_call() -> float:
        req = urllib.request.Request(
            endpoint,
            method="POST",
            data=payload_bytes,
            headers={"Content-Type": "application/json"},
        )
        t0 = time.perf_counter()
        with LOCAL_HTTP_OPENER.open(req, timeout=timeout) as response:
            _ = response.read()
        return (time.perf_counter() - t0) * 1000

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        futures = [pool.submit(one_call) for _ in range(submitted)]
        for fut in as_completed(futures):
            try:
                latencies_ms.append(fut.result())
            except Exception:  # noqa: BLE001
                errors += 1

    return {
        "requests": submitted,
        "success": len(latencies_ms),
        "errors": errors,
        "success_rate_pct": round((len(latencies_ms) / submitted) * 100.0, 2),
        "p50_ms": percentile(latencies_ms, 50),
        "p95_ms": percentile(latencies_ms, 95),
        "p99_ms": percentile(latencies_ms, 99),
        "mean_ms": round(statistics.mean(latencies_ms), 3) if latencies_ms else 0.0,
    }


def flatten_score_offer_kpi(score_perf: dict[str, Any]) -> dict[str, Any]:
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(default)

    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return int(default)

    return {
        "score_offer_requests": _safe_int(score_perf.get("requests"), 0),
        "score_offer_success": _safe_int(score_perf.get("success"), 0),
        "score_offer_error_count": _safe_int(score_perf.get("errors"), 0),
        "score_offer_p95_ms": round(_safe_float(score_perf.get("p95_ms"), 0.0), 3),
        "score_offer_p99_ms": round(_safe_float(score_perf.get("p99_ms"), 0.0), 3),
    }


def compose_up(build: bool, compose_env_file: str | None = None) -> None:
    cmd = ["docker", "compose"]
    resolved_env = resolve_compose_env_file(compose_env_file)
    if resolved_env:
        cmd.extend(["--env-file", resolved_env])
    cmd.extend(["up", "-d"])
    if build:
        cmd.append("--build")
    run_cmd(cmd, check=True)


def compose_down(compose_env_file: str | None = None) -> None:
    cmd = ["docker", "compose"]
    resolved_env = resolve_compose_env_file(compose_env_file)
    if resolved_env:
        cmd.extend(["--env-file", resolved_env])
    cmd.append("down")
    run_cmd(cmd, check=True)


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
    parser.add_argument(
        "--compose-env-file",
        default="",
        help=(
            "Optional docker compose env-file path. "
            "Defaults to COPILOT_SMOKE_COMPOSE_ENV_FILE then env/fleet_demo.env if present."
        ),
    )
    parser.add_argument("--timeout", type=int, default=180, help="max wait time for dynamic checks")
    parser.add_argument("--score-requests", type=int, default=240)
    parser.add_argument("--score-concurrency", type=int, default=30)
    parser.add_argument("--score-timeout", type=float, default=10.0)
    parser.add_argument(
        "--replay-limit",
        type=int,
        default=40,
        help="Max number of replay events requested by smoke check.",
    )
    parser.add_argument(
        "--min-drivers",
        type=int,
        default=None,
        help="Minimum active drivers required before running scenario checks (0 to disable).",
    )
    args = parser.parse_args()
    compose_env_file = resolve_compose_env_file(args.compose_env_file)
    args.min_drivers = resolve_default_min_drivers(compose_env_file) if args.min_drivers is None else int(args.min_drivers)
    args.min_drivers = max(0, int(args.min_drivers))

    started_at = datetime.now(timezone.utc)
    base_url = args.url.rstrip("/")

    try:
        ensure_docker_available()

        if args.up:
            compose_up(build=args.build, compose_env_file=compose_env_file)
        else:
            running = running_compose_services(compose_env_file=compose_env_file)
            missing = sorted(REQUIRED_DEMO_SERVICES - running) if running else []
            if missing:
                raise RuntimeError(
                    "required services are not running: "
                    + ", ".join(missing)
                    + " (run with --up or launch docker compose first)"
                )

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

        readiness_active: dict[str, Any] = {"active_couriers": 0}
        if args.min_drivers > 0:
            readiness_active = wait_for_active_drivers(base_url, args.min_drivers, args.timeout)

        driver_id = args.driver
        if driver_id is None:
            driver_id = discover_driver_id(base_url=base_url, timeout_s=args.timeout)
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
        virtual_ts_str = str((copilot_health.get("tlc_replay") or {}).get("virtual_time") or "")
        replay_payload, replay_url, replay_window, replay_strategy = fetch_replay_payload(
            base_url=base_url,
            driver_id=str(driver_id),
            offer_ts_str=offer_ts_str,
            virtual_ts_str=virtual_ts_str,
            timeout_s=args.timeout,
            limit=max(1, int(args.replay_limit)),
            limit_candidates=[
                max(1, int(args.replay_limit)),
                max(10, int(args.replay_limit) // 2),
            ],
        )
        replay_from_ts, replay_to_ts = replay_window

        hot_perf = wait_for_json(
            f"{base_url}/health/performance?samples=200",
            lambda x: isinstance(x, dict),
            timeout_s=args.timeout,
            request_timeout_s=30.0,
        )
        score_perf = benchmark_score_offer(
            base_url=base_url,
            requests_n=args.score_requests,
            concurrency=args.score_concurrency,
            timeout=args.score_timeout,
        )
        stats_payload: dict[str, Any] = {}
        try:
            stats_payload = wait_for_json(
                f"{base_url}/stats",
                lambda x: isinstance(x, dict),
                timeout_s=min(args.timeout, 40),
                request_timeout_s=20.0,
            )
        except Exception:
            stats_payload = {}
        active_couriers = active_couriers_count(base_url, timeout_s=12.0)
        peak_active_couriers = max(
            int(readiness_active.get("active_couriers", 0) or 0),
            int(stats_payload.get("active_couriers", 0) or 0),
            int(active_couriers),
        )
        stats_payload["active_couriers"] = peak_active_couriers
        dlq = dlq_stats(DLQ_DIR)
        replay_count = int(replay_payload.get("count", 0) or 0)
        replay_health_events = int(((copilot_health.get("tlc_replay") or {}).get("events", 0)) or 0)

        checks = {
            "api_health_ok": api_health.get("status") == "ok",
            "offers_available": int(offers_payload.get("count", 0)) > 0,
            "zones_available": int(zone_payload.get("count", 0)) > 0,
            "events_parquet_written": event_parquet_files > 0,
            "replay_available": (replay_count > 0) or (replay_health_events > 0),
            "active_drivers_ready": peak_active_couriers >= max(0, int(args.min_drivers)),
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
            "compose_env_file": compose_env_file,
            "checks": checks,
            "passed": passed,
            "kpi": {
                "hot_path": hot_perf.get("geosearch_benchmark", {}),
                "score_offer": score_perf,
                "offers_count": offers_payload.get("count", 0),
                "replay_count": replay_count,
                "replay_health_events": replay_health_events,
                "replay_window_strategy": replay_strategy,
                "replay_window_from": replay_from_ts.isoformat(),
                "replay_window_to": replay_to_ts.isoformat(),
                "replay_url": replay_url,
                "replay_mode": str(replay_payload.get("replay_mode", "unknown")),
                "replay_degraded_fallback": bool(replay_payload.get("degraded_fallback", False)),
                "events_parquet_files": event_parquet_files,
                "active_couriers": peak_active_couriers,
                "active_couriers_current": int(active_couriers),
                "min_drivers_target": max(0, int(args.min_drivers)),
                "model_loaded": bool(copilot_health.get("model_loaded", False)),
                "model_quality_gate": copilot_health.get("model_quality_gate", {}),
                "dlq": dlq,
                **flatten_score_offer_kpi(score_perf),
                **summarize_offer_quality(offers_payload),
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
                compose_down(compose_env_file=compose_env_file)
            except Exception as exc:  # noqa: BLE001
                print(f"Warning: docker compose down failed: {exc}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
