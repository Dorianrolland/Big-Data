"""Lot 4 performance and robustness evidence report (local, free, self-host)."""
from __future__ import annotations

import argparse
import json
import math
import statistics
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = ROOT / "data" / "reports"
DOC_PATH = ROOT / "docs" / "preuve-technique.md"
DLQ_DIR = ROOT / "data" / "dlq"
EVENTS_PARQUET_DIR = ROOT / "data" / "parquet_events"


def api_json(url: str, timeout: float = 10.0, method: str = "GET", payload: dict | None = None) -> dict:
    data = None
    headers: dict[str, str] = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url=url, method=method, data=data, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def percentile(values: list[float], pct: int) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = max(0, min(len(ordered) - 1, math.ceil(len(ordered) * pct / 100) - 1))
    return float(round(ordered[idx], 3))


def benchmark_score_offer(base_url: str, requests_n: int, concurrency: int, timeout: float) -> dict[str, Any]:
    endpoint = f"{base_url.rstrip('/')}/copilot/score-offer"
    body = {
        "estimated_fare_eur": 14.2,
        "estimated_distance_km": 3.4,
        "estimated_duration_min": 18,
        "demand_index": 1.4,
        "supply_index": 0.8,
        "weather_factor": 1.0,
        "traffic_factor": 1.1,
    }

    latencies: list[float] = []
    errors = 0

    def one_call() -> float:
        t0 = time.perf_counter()
        _ = api_json(endpoint, timeout=timeout, method="POST", payload=body)
        return (time.perf_counter() - t0) * 1000

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        futures = [pool.submit(one_call) for _ in range(max(1, requests_n))]
        for fut in as_completed(futures):
            try:
                latencies.append(fut.result())
            except Exception:
                errors += 1

    return {
        "requests": requests_n,
        "success": len(latencies),
        "errors": errors,
        "p50_ms": percentile(latencies, 50),
        "p95_ms": percentile(latencies, 95),
        "p99_ms": percentile(latencies, 99),
        "mean_ms": round(statistics.mean(latencies), 3) if latencies else 0.0,
    }


def dlq_stats() -> dict[str, Any]:
    if not DLQ_DIR.exists():
        return {"files": 0, "size_mb": 0.0}

    files = list(DLQ_DIR.rglob("*.jsonl"))
    size_mb = sum(f.stat().st_size for f in files if f.exists()) / (1024 * 1024)
    return {"files": len(files), "size_mb": round(size_mb, 4)}


def replay_count(base_url: str, minutes: int = 15) -> int:
    now = datetime.now(timezone.utc)
    since = now - timedelta(minutes=minutes)
    params = {
        "from": since.isoformat(),
        "to": now.isoformat(),
        "limit": "5000",
    }
    query = urllib.parse.urlencode(params)
    payload = api_json(f"{base_url.rstrip('/')}/copilot/replay?{query}")
    return int(payload.get("count", 0))


def parquet_event_files_count() -> int:
    if not EVENTS_PARQUET_DIR.exists():
        return 0
    return sum(1 for _ in EVENTS_PARQUET_DIR.rglob("*.parquet"))


def measure_ingestion_rate(base_url: str, seconds: int) -> dict[str, Any]:
    stats_start = api_json(f"{base_url.rstrip('/')}/stats")
    start_messages = int(stats_start.get("hot_path", {}).get("messages_traites", 0))
    start_replay = replay_count(base_url)
    start_event_files = parquet_event_files_count()
    start_ts = time.time()

    time.sleep(max(1, seconds))

    stats_end = api_json(f"{base_url.rstrip('/')}/stats")
    end_messages = int(stats_end.get("hot_path", {}).get("messages_traites", 0))
    end_replay = replay_count(base_url)
    end_event_files = parquet_event_files_count()
    elapsed = max(1e-6, time.time() - start_ts)

    delta_messages = end_messages - start_messages
    delta_replay = end_replay - start_replay

    return {
        "window_s": seconds,
        "messages_start": start_messages,
        "messages_end": end_messages,
        "messages_delta": delta_messages,
        "ingestion_msg_s": round(delta_messages / elapsed, 2),
        "replay_delta": delta_replay,
        "event_parquet_files_start": start_event_files,
        "event_parquet_files_end": end_event_files,
        "event_parquet_files_delta": end_event_files - start_event_files,
    }


def evaluate_checks(hot_perf: dict, score_perf: dict, ingest: dict, dlq: dict) -> dict[str, bool]:
    checks = {
        "hot_path_p99_lt_10ms": float(hot_perf.get("p99_ms", 9999)) < 10.0,
        "score_offer_p95_lt_150ms": float(score_perf.get("p95_ms", 9999)) < 150.0,
        "score_offer_error_free": int(score_perf.get("errors", 1)) == 0,
        "ingestion_rate_gt_20_msg_s": float(ingest.get("ingestion_msg_s", 0.0)) > 20.0,
        "dlq_is_empty": int(dlq.get("files", 1)) == 0,
    }
    if int(ingest.get("window_s", 0)) >= 65:
        checks["cold_event_parquet_growth_after_flush"] = int(ingest.get("event_parquet_files_delta", 0)) > 0
    return checks


def write_markdown(report: dict[str, Any]) -> None:
    DOC_PATH.parent.mkdir(parents=True, exist_ok=True)
    checks = report["checks"]

    def mark(ok: bool) -> str:
        return "PASS" if ok else "FAIL"

    replay_window = int(report["ingestion"]["window_s"])
    replay_note = (
        "checked (window >= 65s)"
        if replay_window >= 65
        else "not checked (window < 65s, below cold flush interval)"
    )

    replay_check_line = ""
    if "cold_event_parquet_growth_after_flush" in checks:
        replay_check_line = (
            f"- {mark(checks['cold_event_parquet_growth_after_flush'])} "
            "`cold_event_parquet_growth_after_flush`\n"
        )

    content = f"""# Preuve Technique (Lot 4)

Generated at: {report['generated_at_utc']}

## KPI Summary

- Hot path p99: {report['hot_path']['p99_ms']} ms (target < 10 ms)
- Score offer p95: {report['score_offer']['p95_ms']} ms (target < 150 ms)
- Ingestion throughput: {report['ingestion']['ingestion_msg_s']} msg/s
- Replay growth during window: {report['ingestion']['replay_delta']} events
- Replay growth check mode: {replay_note}
- Cold event parquet growth: {report['ingestion']['event_parquet_files_delta']} files
- DLQ files: {report['dlq']['files']}

## Acceptance Checks

- {mark(checks['hot_path_p99_lt_10ms'])} `hot_path_p99_lt_10ms`
- {mark(checks['score_offer_p95_lt_150ms'])} `score_offer_p95_lt_150ms`
- {mark(checks['score_offer_error_free'])} `score_offer_error_free`
- {mark(checks['ingestion_rate_gt_20_msg_s'])} `ingestion_rate_gt_20_msg_s`
- {mark(checks['dlq_is_empty'])} `dlq_is_empty`
{replay_check_line}

## Inputs

- Benchmark requests: {report['score_offer']['requests']}
- Benchmark concurrency: {report['score_offer'].get('concurrency')}
- Ingestion window: {report['ingestion']['window_s']} s
- API base URL: {report['base_url']}
"""
    DOC_PATH.write_text(content, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Lot 4 performance and robustness report")
    parser.add_argument("--url", default="http://localhost:8001")
    parser.add_argument("--ingest-window", type=int, default=20)
    parser.add_argument("--score-requests", type=int, default=300)
    parser.add_argument("--score-concurrency", type=int, default=30)
    parser.add_argument("--score-timeout", type=float, default=10.0)
    args = parser.parse_args()

    base_url = args.url.rstrip("/")

    # Hot path benchmark already exposed by API.
    hot = api_json(f"{base_url}/health/performance?samples=300").get("geosearch_benchmark", {})

    score = benchmark_score_offer(
        base_url=base_url,
        requests_n=args.score_requests,
        concurrency=args.score_concurrency,
        timeout=args.score_timeout,
    )
    score["concurrency"] = args.score_concurrency

    ingest = measure_ingestion_rate(base_url=base_url, seconds=args.ingest_window)
    dlq = dlq_stats()

    checks = evaluate_checks(hot_perf=hot, score_perf=score, ingest=ingest, dlq=dlq)
    passed = all(checks.values())

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "base_url": base_url,
        "hot_path": hot,
        "score_offer": score,
        "ingestion": ingest,
        "dlq": dlq,
        "checks": checks,
        "passed": passed,
    }

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = REPORT_DIR / f"perf_lot4_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    write_markdown(report)

    print("Lot 4 report:")
    print(f"  passed: {passed}")
    for key, value in checks.items():
        print(f"  {key}: {value}")
    print(f"  hot_path_p99_ms: {hot.get('p99_ms')}")
    print(f"  score_offer_p95_ms: {score.get('p95_ms')}")
    print(f"  ingestion_msg_s: {ingest.get('ingestion_msg_s')}")
    print(f"  output_json: {out_path}")
    print(f"  output_md: {DOC_PATH}")


if __name__ == "__main__":
    main()
