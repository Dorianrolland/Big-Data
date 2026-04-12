# Preuve Technique (Lot 4)

Generated at: 2026-04-11T18:10:52.665686+00:00

## KPI Summary

- Hot path p99: 0.77 ms (target < 10 ms)
- Score offer p95: 28.599 ms (target < 150 ms)
- Ingestion throughput: 96.65 msg/s
- Replay growth during window: 0 events
- Replay growth check mode: checked (window >= 65s)
- Cold event parquet growth: 3 files
- DLQ files: 0

## Acceptance Checks

- PASS `hot_path_p99_lt_10ms`
- PASS `score_offer_p95_lt_150ms`
- PASS `score_offer_error_free`
- PASS `ingestion_rate_gt_20_msg_s`
- PASS `dlq_is_empty`
- PASS `cold_event_parquet_growth_after_flush`


## Inputs

- Benchmark requests: 80
- Benchmark concurrency: 8
- Ingestion window: 70 s
- API base URL: http://localhost:8001
