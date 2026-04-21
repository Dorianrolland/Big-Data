# Preuve Technique (Lot 4)

Generated at: 2026-04-21T14:17:36.302875+00:00

## KPI Summary

- Hot path p99: 0.225 ms (target < 10 ms)
- Score offer p95: 6798.437 ms (target < 150 ms)
- Score offer success rate: 100.0%
- Ingestion throughput: 0.0 msg/s
- Ingestion target threshold: 0.0 msg/s
- Active drivers at window start: 1
- Replay growth during window: 0 events
- Replay growth check mode: not checked (window < 65s, below cold flush interval)
- Cold event parquet growth: 1 files
- DLQ files: 0
- DLQ files growth during window: 0
- DLQ size growth during window: 0.0 MB
- Pass policy require `dlq_is_empty`: False

## Critical Checks

- PASS `hot_path_p99_lt_10ms`
- FAIL `score_offer_p95_lt_150ms`
- PASS `score_offer_error_free`
- PASS `ingestion_rate_gt_target_msg_s`
- PASS `dlq_no_new_errors_in_window`

## Non-Critical Checks

- PASS `dlq_is_empty (historical backlog informational)`
- FAIL `ingestion_rate_gt_20_msg_s (legacy fleet-mode threshold)`

## Product KPI Summary

- Decision API latency (smoke `score-offer`): p95=19.28 ms, p99=22.389 ms
- Hot path UX latency (`livreurs-proches` proxy): p99=0.706 ms
- Throughput (`perf-lot4` ingestion): 0.0 msg/s
- Runtime decision reliability: `score_offer_no_errors`=True
- Product coverage signal: `offers_count`=4, `replay_count`=2
- Decision quality signal (live offers): `accept_rate_pct`=75.0, `avg_accept_score`=0.745, `avg_eur_per_hour`=93.25, `top_offer_accept_score`=0.99
- Model gate status: accepted=True, reason=ok
- Decision quality (offline metrics): roc_auc=0.9536542994607511, average_precision=0.9224640070358155, f1_at_0_5=0.8482186432406051


## Inputs

- Benchmark requests: 80
- Benchmark concurrency: 8
- Ingestion window: 8 s
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

- smoke-e2e JSON: `C:\Users\icemo\Desktop\Big-Data\data\reports\smoke_e2e_20260417T213747Z.json`
- perf-lot4 JSON: `C:\Users\icemo\Desktop\Big-Data\data\reports\perf_lot4_20260421T141736Z.json`
