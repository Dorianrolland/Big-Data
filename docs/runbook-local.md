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
