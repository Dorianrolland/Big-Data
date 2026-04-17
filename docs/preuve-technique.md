# Preuve Technique (Lot 4)

Generated at: 2026-04-17T17:28:28.640417+00:00

## KPI Summary

- Hot path p99: 0.823 ms (target < 10 ms)
- Score offer p95: 42.758 ms (target < 150 ms)
- Ingestion throughput: 29.74 msg/s
- Replay growth during window: 0 events
- Replay growth check mode: not checked (window < 65s, below cold flush interval)
- Cold event parquet growth: 0 files
- DLQ files: 1
- DLQ files growth during window: 0
- DLQ size growth during window: 0.0 MB
- Pass policy require `dlq_is_empty`: False

## Acceptance Checks

- PASS `hot_path_p99_lt_10ms`
- PASS `score_offer_p95_lt_150ms`
- PASS `score_offer_error_free`
- PASS `ingestion_rate_gt_20_msg_s`
- PASS `dlq_no_new_errors_in_window`
- FAIL `dlq_is_empty`


## Inputs

- Benchmark requests: 300
- Benchmark concurrency: 30
- Ingestion window: 20 s
- API base URL: http://localhost:8001
