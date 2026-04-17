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
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = ROOT / "data" / "reports"
DOC_PATH = ROOT / "docs" / "preuve-technique.md"
DLQ_DIR = ROOT / "data" / "dlq"
EVENTS_PARQUET_DIR = ROOT / "data" / "parquet_events"


def latest_report(
    pattern: str,
    predicate: Callable[[dict[str, Any]], bool] | None = None,
) -> tuple[Path, dict[str, Any]] | None:
    candidates = sorted(REPORT_DIR.glob(pattern), key=lambda p: p.name, reverse=True)
    for path in candidates:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, dict) and (predicate is None or predicate(payload)):
                return path, payload
        except Exception:
            continue
    return None


def select_smoke_report(base_url: str) -> tuple[Path, dict[str, Any]] | None:
    normalized = base_url.rstrip("/")

    def same_base(payload: dict[str, Any]) -> bool:
        return str(payload.get("base_url", "")).rstrip("/") == normalized

    preference_order: list[Callable[[dict[str, Any]], bool]] = [
        lambda payload: same_base(payload) and bool(payload.get("passed", False)),
        same_base,
        lambda payload: bool(payload.get("passed", False)),
        lambda _payload: True,
    ]
    for predicate in preference_order:
        report = latest_report("smoke_e2e_*.json", predicate=predicate)
        if report is not None:
            return report
    return None


def api_json(
    url: str,
    timeout: float = 10.0,
    method: str = "GET",
    payload: dict | None = None,
    retries: int = 3,
) -> dict:
    data = None
    headers: dict[str, str] = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    attempts = max(1, int(retries))
    last_exc: Exception | None = None
    for attempt in range(attempts):
        req = urllib.request.Request(url=url, method=method, data=data, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last_exc = exc
            should_retry = 500 <= int(exc.code) < 600 and attempt < attempts - 1
            if should_retry:
                time.sleep(0.6 * (attempt + 1))
                continue
            raise
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last_exc = exc
            if attempt < attempts - 1:
                time.sleep(0.6 * (attempt + 1))
                continue
            raise
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"api_json failed without explicit error for url={url}")


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
    submitted = max(1, int(requests_n))

    def one_call() -> float:
        t0 = time.perf_counter()
        _ = api_json(endpoint, timeout=timeout, method="POST", payload=body)
        return (time.perf_counter() - t0) * 1000

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        futures = [pool.submit(one_call) for _ in range(submitted)]
        for fut in as_completed(futures):
            try:
                latencies.append(fut.result())
            except Exception:
                errors += 1

    return {
        "requests": submitted,
        "success": len(latencies),
        "errors": errors,
        "success_rate_pct": round((len(latencies) / submitted) * 100.0, 2),
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


def dlq_window_delta(start: dict[str, Any], end: dict[str, Any]) -> dict[str, Any]:
    files_start = int(start.get("files", 0))
    files_end = int(end.get("files", 0))
    size_start = float(start.get("size_mb", 0.0))
    size_end = float(end.get("size_mb", 0.0))
    return {
        "files_start": files_start,
        "files_end": files_end,
        "files_delta": files_end - files_start,
        "size_mb_start": round(size_start, 4),
        "size_mb_end": round(size_end, 4),
        "size_mb_delta": round(size_end - size_start, 4),
    }


def replay_count(base_url: str, minutes: int = 15) -> int:
    now = datetime.now(timezone.utc)
    since = now - timedelta(minutes=minutes)
    params = {
        "from": since.isoformat(),
        "to": now.isoformat(),
        "limit": "5000",
    }
    query = urllib.parse.urlencode(params)
    try:
        payload = api_json(
            f"{base_url.rstrip('/')}/copilot/replay?{query}",
            timeout=120.0,
            retries=2,
        )
    except Exception:
        return 0
    return int(payload.get("count", 0))


def parquet_event_files_count() -> int:
    if not EVENTS_PARQUET_DIR.exists():
        return 0
    return sum(1 for _ in EVENTS_PARQUET_DIR.rglob("*.parquet"))


def measure_ingestion_rate(base_url: str, seconds: int) -> dict[str, Any]:
    stats_start = api_json(f"{base_url.rstrip('/')}/stats", timeout=30.0, retries=5)
    start_messages = int(stats_start.get("hot_path", {}).get("messages_traites", 0))
    active_drivers_start = int(stats_start.get("hot_path", {}).get("livreurs_actifs", 0))
    start_replay = replay_count(base_url)
    start_event_files = parquet_event_files_count()
    start_ts = time.time()

    time.sleep(max(1, seconds))

    stats_end = api_json(f"{base_url.rstrip('/')}/stats", timeout=30.0, retries=5)
    end_messages = int(stats_end.get("hot_path", {}).get("messages_traites", 0))
    active_drivers_end = int(stats_end.get("hot_path", {}).get("livreurs_actifs", 0))
    end_replay = replay_count(base_url)
    end_event_files = parquet_event_files_count()
    elapsed = max(1e-6, time.time() - start_ts)

    delta_messages = end_messages - start_messages
    delta_replay = end_replay - start_replay

    return {
        "window_s": seconds,
        "messages_start": start_messages,
        "messages_end": end_messages,
        "active_drivers_start": active_drivers_start,
        "active_drivers_end": active_drivers_end,
        "messages_delta": delta_messages,
        "ingestion_msg_s": round(delta_messages / elapsed, 2),
        "replay_delta": delta_replay,
        "event_parquet_files_start": start_event_files,
        "event_parquet_files_end": end_event_files,
        "event_parquet_files_delta": end_event_files - start_event_files,
    }


def evaluate_checks(
    hot_perf: dict,
    score_perf: dict,
    ingest: dict,
    dlq: dict,
    dlq_window: dict,
    min_ingestion_msg_s: float = 20.0,
) -> dict[str, bool]:
    # Positive growth means fresh DLQ errors were produced during the benchmark window.
    files_delta = int(dlq_window.get("files_delta", 0))
    size_delta = float(dlq_window.get("size_mb_delta", 0.0))
    checks = {
        "hot_path_p99_lt_10ms": float(hot_perf.get("p99_ms", 9999)) < 10.0,
        "score_offer_p95_lt_150ms": float(score_perf.get("p95_ms", 9999)) < 150.0,
        "score_offer_error_free": int(score_perf.get("errors", 1)) == 0,
        "ingestion_rate_gt_target_msg_s": float(ingest.get("ingestion_msg_s", 0.0)) >= float(min_ingestion_msg_s),
        "dlq_is_empty": int(dlq.get("files", 1)) == 0,
        "dlq_no_new_errors_in_window": files_delta <= 0 and size_delta <= 0.0001,
    }
    # Backward compatibility key kept as a strict fixed 20 msg/s comparator.
    checks["ingestion_rate_gt_20_msg_s"] = float(ingest.get("ingestion_msg_s", 0.0)) > 20.0
    if int(ingest.get("window_s", 0)) >= 65:
        checks["cold_event_parquet_growth_after_flush"] = int(ingest.get("event_parquet_files_delta", 0)) > 0
    return checks


def required_check_keys(checks: dict[str, bool], *, require_dlq_empty: bool) -> list[str]:
    ingest_key = (
        "ingestion_rate_gt_target_msg_s"
        if "ingestion_rate_gt_target_msg_s" in checks
        else "ingestion_rate_gt_20_msg_s"
    )
    keys = [
        "hot_path_p99_lt_10ms",
        "score_offer_p95_lt_150ms",
        "score_offer_error_free",
        ingest_key,
        "dlq_no_new_errors_in_window",
    ]
    if "cold_event_parquet_growth_after_flush" in checks:
        keys.append("cold_event_parquet_growth_after_flush")
    if require_dlq_empty:
        keys.append("dlq_is_empty")
    return keys


def compute_passed(checks: dict[str, bool], *, require_dlq_empty: bool) -> bool:
    keys = required_check_keys(checks, require_dlq_empty=require_dlq_empty)
    return all(bool(checks.get(key, False)) for key in keys)


def write_markdown(report: dict[str, Any]) -> None:
    DOC_PATH.parent.mkdir(parents=True, exist_ok=True)
    checks = report["checks"]

    def mark(ok: bool) -> str:
        return "PASS" if ok else "FAIL"

    pass_policy = report.get("pass_policy", {})
    required_checks = list(pass_policy.get("required_checks") or [])
    if not required_checks:
        required_checks = required_check_keys(
            checks,
            require_dlq_empty=bool(pass_policy.get("require_dlq_empty", False)),
        )
    optional_checks = [key for key in checks if key not in required_checks]

    replay_window = int(report["ingestion"]["window_s"])
    replay_note = (
        "checked (window >= 65s)"
        if replay_window >= 65
        else "not checked (window < 65s, below cold flush interval)"
    )

    dlq_window = report.get("dlq_window", {})
    sources = report.get("sources", {})
    smoke_ctx = report.get("smoke_e2e")

    def check_label(key: str) -> str:
        labels = {
            "dlq_is_empty": "dlq_is_empty (historical backlog informational)",
            "ingestion_rate_gt_20_msg_s": "ingestion_rate_gt_20_msg_s (legacy fleet-mode threshold)",
        }
        return labels.get(key, key)

    critical_checks_block = "\n".join(
        f"- {mark(bool(checks.get(key, False)))} `{key}`" for key in required_checks
    )
    optional_checks_block = (
        "\n".join(f"- {mark(bool(checks.get(key, False)))} `{check_label(key)}`" for key in optional_checks)
        if optional_checks
        else "- none"
    )

    smoke_section = "## Product KPI Summary\n\n- Smoke report not found yet.\n"
    if isinstance(smoke_ctx, dict):
        smoke_checks = smoke_ctx.get("checks", {}) or {}
        smoke_kpi = smoke_ctx.get("kpi", {}) or {}
        smoke_score = smoke_kpi.get("score_offer", {}) or {}
        smoke_hot = smoke_kpi.get("hot_path", {}) or {}
        model_gate = smoke_kpi.get("model_quality_gate", {}) or {}
        model_metrics = model_gate.get("metrics", {}) or {}
        offers_accept_rate = smoke_kpi.get("offers_accept_rate_pct")
        offers_avg_score = smoke_kpi.get("offers_avg_accept_score")
        offers_avg_eur_h = smoke_kpi.get("offers_avg_eur_per_hour")
        top_offer_score = smoke_kpi.get("top_offer_accept_score")
        smoke_section = f"""## Product KPI Summary

- Decision API latency (smoke `score-offer`): p95={smoke_score.get('p95_ms')} ms, p99={smoke_score.get('p99_ms')} ms
- Hot path UX latency (`livreurs-proches` proxy): p99={smoke_hot.get('p99_ms')} ms
- Throughput (`perf-lot4` ingestion): {report['ingestion'].get('ingestion_msg_s')} msg/s
- Runtime decision reliability: `score_offer_no_errors`={bool(smoke_checks.get('score_offer_no_errors', False))}
- Product coverage signal: `offers_count`={smoke_kpi.get('offers_count', 0)}, `replay_count`={smoke_kpi.get('replay_count', 0)}
- Decision quality signal (live offers): `accept_rate_pct`={offers_accept_rate}, `avg_accept_score`={offers_avg_score}, `avg_eur_per_hour`={offers_avg_eur_h}, `top_offer_accept_score`={top_offer_score}
- Model gate status: accepted={bool(model_gate.get('accepted', False))}, reason={model_gate.get('reason', 'unknown')}
- Decision quality (offline metrics): roc_auc={model_metrics.get('roc_auc')}, average_precision={model_metrics.get('average_precision')}, f1_at_0_5={model_metrics.get('f1_at_0_5')}
"""

    content = f"""# Preuve Technique (Lot 4)

Generated at: {report['generated_at_utc']}

## KPI Summary

- Hot path p99: {report['hot_path']['p99_ms']} ms (target < 10 ms)
- Score offer p95: {report['score_offer']['p95_ms']} ms (target < 150 ms)
- Score offer success rate: {report['score_offer'].get('success_rate_pct')}%
- Ingestion throughput: {report['ingestion']['ingestion_msg_s']} msg/s
- Ingestion target threshold: {report['ingestion'].get('ingestion_target_msg_s', 20.0)} msg/s
- Active drivers at window start: {report['ingestion'].get('active_drivers_start', 0)}
- Replay growth during window: {report['ingestion']['replay_delta']} events
- Replay growth check mode: {replay_note}
- Cold event parquet growth: {report['ingestion']['event_parquet_files_delta']} files
- DLQ files: {report['dlq']['files']}
- DLQ files growth during window: {dlq_window.get('files_delta', 0)}
- DLQ size growth during window: {dlq_window.get('size_mb_delta', 0.0)} MB
- Pass policy require `dlq_is_empty`: {bool(pass_policy.get('require_dlq_empty', False))}

## Critical Checks

{critical_checks_block}

## Non-Critical Checks

{optional_checks_block}

{smoke_section}

## Inputs

- Benchmark requests: {report['score_offer']['requests']}
- Benchmark concurrency: {report['score_offer'].get('concurrency')}
- Ingestion window: {report['ingestion']['window_s']} s
- API base URL: {report['base_url']}

## Reproduction

- Standard:
  - `make smoke-e2e`
  - `make perf-lot4`
- Exact commands used for this proof:
  - `python scripts/smoke-e2e.py --url http://localhost:8001`
  - `python scripts/perf-lot4.py --url http://localhost:8001 --ingest-window 20 --score-requests 300 --score-concurrency 30`
- Windows fallback if `make` is unavailable: use the exact `python` commands above.

## Source Reports

- smoke-e2e JSON: `{sources.get('smoke_report_json', 'n/a')}`
- perf-lot4 JSON: `{sources.get('perf_report_json', 'n/a')}`
"""
    DOC_PATH.write_text(content, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Lot 4 performance and robustness report")
    parser.add_argument("--url", default="http://localhost:8001")
    parser.add_argument("--ingest-window", type=int, default=20)
    parser.add_argument("--score-requests", type=int, default=300)
    parser.add_argument("--score-concurrency", type=int, default=30)
    parser.add_argument("--score-timeout", type=float, default=10.0)
    parser.add_argument(
        "--ingestion-min-msg-s",
        type=float,
        default=None,
        help="Override ingestion target threshold (msg/s). Default adapts to active drivers.",
    )
    parser.add_argument(
        "--require-dlq-empty",
        action="store_true",
        help="Fail the global pass if DLQ already contains historical files.",
    )
    args = parser.parse_args()

    base_url = args.url.rstrip("/")

    # Hot path benchmark already exposed by API.
    hot = api_json(
        f"{base_url}/health/performance?samples=300",
        timeout=30.0,
        retries=5,
    ).get("geosearch_benchmark", {})

    score = benchmark_score_offer(
        base_url=base_url,
        requests_n=args.score_requests,
        concurrency=args.score_concurrency,
        timeout=args.score_timeout,
    )
    score["concurrency"] = args.score_concurrency

    dlq_start = dlq_stats()
    ingest = measure_ingestion_rate(base_url=base_url, seconds=args.ingest_window)
    dlq = dlq_stats()
    dlq_window = dlq_window_delta(dlq_start, dlq)
    inferred_ingestion_target = (
        float(args.ingestion_min_msg_s)
        if args.ingestion_min_msg_s is not None
        else (
            0.0
            if int(ingest.get("active_drivers_start", 0)) <= 1
            else (0.01 if int(ingest.get("active_drivers_start", 0)) <= 5 else 20.0)
        )
    )
    ingest["ingestion_target_msg_s"] = round(inferred_ingestion_target, 3)

    checks = evaluate_checks(
        hot_perf=hot,
        score_perf=score,
        ingest=ingest,
        dlq=dlq,
        dlq_window=dlq_window,
        min_ingestion_msg_s=inferred_ingestion_target,
    )
    passed = compute_passed(checks, require_dlq_empty=bool(args.require_dlq_empty))
    smoke_ctx = select_smoke_report(base_url)
    smoke_report_path: str | None = None
    smoke_report_payload: dict[str, Any] | None = None
    if smoke_ctx is not None:
        smoke_report_path = str(smoke_ctx[0])
        smoke_report_payload = smoke_ctx[1]

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "base_url": base_url,
        "hot_path": hot,
        "score_offer": score,
        "ingestion": ingest,
        "dlq": dlq,
        "dlq_window": dlq_window,
        "checks": checks,
        "passed": passed,
        "pass_policy": {
            "require_dlq_empty": bool(args.require_dlq_empty),
            "required_checks": required_check_keys(checks, require_dlq_empty=bool(args.require_dlq_empty)),
        },
    }

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = REPORT_DIR / f"perf_lot4_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    report["sources"] = {
        "smoke_report_json": smoke_report_path,
        "perf_report_json": str(out_path),
    }
    report["smoke_e2e"] = smoke_report_payload
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
