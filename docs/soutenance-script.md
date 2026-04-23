# Soutenance Demo Script (5-10 min)

## 1) Launch stack

```bash
make up
```

## 2) Verify core services

- API docs: `http://localhost:8001/docs`
- Live map: `http://localhost:8001/map`
- Driver Copilot app: `http://localhost:8001/copilot/driver-app`
- Copilot operator fallback: `http://localhost:8001/copilot`
- Grafana: `http://localhost:3000`

## 3) Demonstrate realtime pipeline

1. Open RedisInsight and show keys:
   - `fleet:geo`
   - `copilot:offer:*`
   - `copilot:context:zone:*`
2. Call:

```bash
curl "http://localhost:8001/stats"
```

3. Show cold-path growth:

```bash
pwsh ./scripts/duckdb-local.ps1 -Action events-count
```

## 4) Demonstrate copilot decisioning

1. In PWA, score manual offer.
2. Show driver offers and zone recommendations.
3. Call API directly:

```bash
curl -X POST "http://localhost:8001/copilot/score-offer" \
  -H "Content-Type: application/json" \
  -d '{"estimated_fare_eur":14,"estimated_distance_km":3.2,"estimated_duration_min":17,"demand_index":1.4,"supply_index":0.8,"weather_factor":1.0,"traffic_factor":1.1}'
```

## 5) Show replay and explainability

```bash
curl "http://localhost:8001/copilot/replay?from=2026-04-10T00:00:00Z&to=2026-04-11T23:59:59Z&driver_id=L001"
```

## 6) Show latency KPI

```bash
python scripts/benchmark-copilot.py --url http://localhost:8001 --requests 300 --concurrency 40
```

## 7) Optional model training demo

```bash
make train-copilot
curl "http://localhost:8001/copilot/health"
```
