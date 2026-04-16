# Preuve Technique (Lot 4)

Generated at: 2026-04-13T17:01:00.219684+00:00

## KPI Summary

- Hot path p99: 0.877 ms (target < 10 ms)
- Score offer p95: 39.216 ms (target < 150 ms)
- Ingestion throughput: 21.26 msg/s
- Replay growth during window: 0 events
- Replay growth check mode: not checked (window < 65s, below cold flush interval)
- Cold event parquet growth: 0 files
- DLQ files: 1

## Acceptance Checks

- PASS `hot_path_p99_lt_10ms`
- PASS `score_offer_p95_lt_150ms`
- PASS `score_offer_error_free`
- PASS `ingestion_rate_gt_20_msg_s`
- FAIL `dlq_is_empty`


## Inputs

- Benchmark requests: 300
- Benchmark concurrency: 30
- Ingestion window: 20 s
- API base URL: http://localhost:8001
