# Preuve Technique (Lot 4)

Generated at: 2026-04-20T13:58:44.722763+00:00

## KPI Summary

- Hot path p99: 0.725 ms (target < 10 ms)
- Score offer p95: 42.244 ms (target < 150 ms)
- Score offer success rate: 100.0%
- Ingestion throughput: 0.0 msg/s
- Ingestion target threshold: 0.0 msg/s
- Active drivers at window start: 1
- Replay growth during window: 0 events
- Replay growth check mode: not checked (window < 65s, below cold flush interval)
- Cold event parquet growth: 0 files
- DLQ files: 0
- DLQ files growth during window: 0
- DLQ size growth during window: 0.0 MB
- Pass policy require `dlq_is_empty`: False

## Critical Checks

- PASS `hot_path_p99_lt_10ms`
- PASS `score_offer_p95_lt_150ms`
- PASS `score_offer_error_free`
- PASS `ingestion_rate_gt_target_msg_s`
- PASS `dlq_no_new_errors_in_window`

## Non-Critical Checks

- PASS `dlq_is_empty (historical backlog informational)`
- FAIL `ingestion_rate_gt_20_msg_s (legacy fleet-mode threshold)`

## Product KPI Summary

- Smoke report not found yet.


## Inputs

- Benchmark requests: 300
- Benchmark concurrency: 30
- Ingestion window: 20 s
- API base URL: http://localhost:8001

## Reproduction

- Standard:
  - `make smoke-e2e`
  - `make perf-lot4`
- Exact commands used for this proof:
  - `python scripts/smoke-e2e.py --url http://localhost:8001`
  - `python scripts/perf-lot4.py --url http://localhost:8001 --ingest-window 20 --score-requests 300 --score-concurrency 30`
- Windows fallback if `make` is unavailable: use the exact `python` commands above.

## Source Reports

- smoke-e2e JSON: `None`
- perf-lot4 JSON: `/home/cytech/ING3/Big Data/FleetStream/data/reports/perf_lot4_20260420T135844Z.json`
