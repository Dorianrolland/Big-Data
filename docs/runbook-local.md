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
python -m ruff check producer hot_path cold_path uber_connector api copilot_features ml tests schemas/register_schemas.py
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
