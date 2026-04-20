# Local Runbook

## Prerequisites
- Docker + Docker Compose
- Python 3.11+ for local scripts/tests

## Start

```bash
make up
```

## Stop

```bash
make down
```

## Register schemas manually (optional)

```powershell
pwsh ./scripts/register-schemas.ps1
```

## Train local model

```bash
make train-copilot
```

Inspect trained metrics:

```bash
cat data/models/copilot_model.json
```

Validate model gate in API:

```bash
curl -s http://localhost:8001/copilot/health
```

## Validate

```bash
python -m ruff check tlc_replay context_poller hot_path cold_path api copilot_features driver_ingest ml tests schemas/register_schemas.py
python -m pytest -q
python scripts/smoke-e2e.py --url http://localhost:8001
python scripts/perf-lot4.py --url http://localhost:8001 --ingest-window 20
```

Windows shortcuts:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/smoke-e2e.ps1
powershell -ExecutionPolicy Bypass -File scripts/perf-lot4.ps1
powershell -ExecutionPolicy Bypass -File scripts/proof-lot4.ps1
```

## Key endpoints
- `GET /map`
- `GET /copilot`
- `POST /copilot/score-offer`
- `GET /copilot/driver/{id}/offers`
- `GET /copilot/driver/{id}/next-best-zone`
- `GET /copilot/replay`
- `GET http://localhost:8010/healthz`
- `POST http://localhost:8010/ingest/v1/position`
- `POST http://localhost:8010/ingest/v1/positions`

## Single-driver copilot mode (default profile: 10m train / 2m live)

The default local profile is now:
- `TLC_SCENARIO=single_driver`
- `TLC_TRAIN_MONTH_COUNT=10`
- `TLC_LIVE_MONTH_COUNT=2`
- `TLC_RESET_RUNTIME_ON_START=true`

Start in this mode:

```bash
make up
```

Or with explicit profile file:

```bash
docker compose --env-file env/single_driver_10m2m.env up -d --build
```

Open the focused map:

```text
http://localhost:8001/map?focus=drv_demo_001
```

Reset runtime state (cursors + Redis hot keys, model kept):

```bash
make single-driver-reset
```

Observe health and quality signals:

```bash
curl -s http://localhost:8001/copilot/health
```

Check these fields:
- `data_quality.supply_variance`
- `data_quality.traffic_nonzero_rate`
- `routing_quality.routing_success_rate`
- `routing_quality.hold_rate`

## Real device mode (no replay)

Stop TLC replay so only real device GPS is visible on the map:

```bash
make real-mode
```

Send one sandbox position to validate ingest pipeline:

```bash
make demo-ingest
```

Restart simulation later:

```bash
make sim-mode
```

## Long history replay (10 months / 1 year)

Use TLC historical data by month, then replay in chronological order:

```powershell
# 10 months starting Jan 2024
$env:TLC_MONTH="2024-01"; $env:TLC_MONTH_COUNT="10"; docker compose up -d --force-recreate tlc-replay

# 12 months (1 year) starting Jan 2024
$env:TLC_MONTH="2024-01"; $env:TLC_MONTH_COUNT="12"; docker compose up -d --force-recreate tlc-replay
```

Check that the service is iterating correctly:

```bash
docker logs -f fleetstream-tlc-replay
```

You should see logs like `starting month 2024-01 (1/10)`, then `(2/10)`, etc.

## source_platform — valeurs et conventions

Le champ `source_platform` identifie l'origine de chaque événement tout au long du pipeline.

| Valeur | Producteur | Description |
|--------|-----------|-------------|
| `driver_ingest` | `driver_ingest` | Position GPS envoyée par l'application mobile du livreur |
| `tlc_hvfhv_historical` | `tlc_replay`, `single_driver` | Données historiques TLC (NYC for-hire vehicle) rejouées |
| `context_poller_public` | `context_poller` | Signaux contextuels (météo, trafic, zones) |
| `unknown` | cold_path fallback | Proto reçu sans `source_platform` défini |

### Requête DuckDB — analyse par plateforme source

```python
import duckdb

con = duckdb.connect()
rows = con.execute("""
    SELECT
        source_platform,
        topic,
        COUNT(*)                                   AS nb_events,
        ROUND(AVG(estimated_fare_eur), 2)          AS avg_fare_eur,
        MIN(ts)                                    AS first_seen,
        MAX(ts)                                    AS last_seen
    FROM read_parquet(
        'data/parquet_events/topic=*/year=*/month=*/day=*/hour=*/*.parquet',
        hive_partitioning = true
    )
    WHERE source_platform IS NOT NULL
      AND source_platform != ''
    GROUP BY source_platform, topic
    ORDER BY nb_events DESC
""").fetchall()

for row in rows:
    print(row)
```

### Vérifier la traçabilité via l'API

```bash
curl -s "http://localhost:8001/copilot/replay?from=2026-04-01T00:00:00Z&to=2026-04-02T00:00:00Z&limit=5" \
  | python3 -c "import sys,json; [print(e['source_platform'], e['topic']) for e in json.load(sys.stdin)['events']]"
```
