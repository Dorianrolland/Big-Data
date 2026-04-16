"""Benchmark copilot score endpoint latency with stdlib only."""
import argparse
import json
import math
import statistics
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed


def percentile(values: list[float], pct: int) -> float:
    if not values:
        return 0.0
    data = sorted(values)
    idx = max(0, min(len(data) - 1, math.ceil(len(data) * pct / 100) - 1))
    return round(data[idx], 3)


def _post_score_offer(endpoint: str, payload_bytes: bytes, timeout: float) -> float:
    req = urllib.request.Request(
        endpoint,
        data=payload_bytes,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    t0 = time.perf_counter()
    with urllib.request.urlopen(req, timeout=timeout) as response:
        _ = response.read()
    return (time.perf_counter() - t0) * 1000


def run(url: str, requests_n: int, concurrency: int, timeout: float) -> dict:
    endpoint = f"{url.rstrip('/')}/copilot/score-offer"
    payload = {
        "estimated_fare_eur": 14.2,
        "estimated_distance_km": 3.4,
        "estimated_duration_min": 18.0,
        "demand_index": 1.4,
        "supply_index": 0.8,
        "weather_factor": 1.0,
        "traffic_factor": 1.1,
    }
    payload_bytes = json.dumps(payload).encode("utf-8")

    latencies: list[float] = []
    errors = 0

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        futures = [
            pool.submit(_post_score_offer, endpoint, payload_bytes, timeout)
            for _ in range(max(1, requests_n))
        ]
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark /copilot/score-offer")
    parser.add_argument("--url", default="http://localhost:8001")
    parser.add_argument("--requests", type=int, default=300)
    parser.add_argument("--concurrency", type=int, default=40)
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args()

    result = run(args.url, args.requests, args.concurrency, args.timeout)
    for key, value in result.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
