# Slides Outline

## Slide 1 - Problem
- Courier platform needs low-latency geospatial + scalable analytics.
- Goal: increase courier net revenue per hour with actionable recommendations.

## Slide 2 - Architecture
- Lambda architecture (Redpanda + Redis + Parquet/DuckDB).
- New copilot topics: offers, order events, context signals.
- Protobuf contract + schema registration.

## Slide 3 - Realtime Copilot
- Feature materialization in Redis.
- Score offer endpoint with ML + heuristic fallback.
- Zone recommendation endpoint for repositioning.

## Slide 4 - Product Demo
- PWA driver app.
- Offer score, decision, replay timeline.

## Slide 5 - KPIs
- Hot path p99 latency.
- Copilot score endpoint p95 latency.
- Throughput and DLQ error rate.

## Slide 6 - Data + ML
- Parquet events lake.
- Local training pipeline and model artifact versioning.

## Slide 7 - Reliability and Quality
- Schema contract tests.
- Unit tests + CI lint/tests.
- DLQ and replayability.

## Slide 8 - Limits and Next Steps
- Real Uber data via NYC TLC HVFHV (no auth, monthly parquet replay).
- Next: richer demand forecast and route-cost model.
