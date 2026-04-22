# FleetStream

> Suivi temps rel d'une flotte de livreurs  Architecture Lambda sur Redpanda, Redis Stack et Apache Parquet.

**Cas d'usage** : Plateforme type Uber Eats grant 100+ livreurs simultanment  New York City.
**Problme adress** : Le SQL classique ne peut pas ingrer des milliers de coordonnes GPS par seconde tout en servant des requtes gospatiales  faible latence.

---

## Dmarrage en une commande

```bash
git clone git@github.com:Dorianrolland/Big-Data.git FleetStream && cd FleetStream
make up
```

**Services disponibles immdiatement :**

| Interface | URL | Description |
|---|---|---|
|  **Carte live** | http://localhost:8001/map | Dashboard HTML/JS Leaflet  zro clignotement |
|  **Analytics** | http://localhost:8501 | Streamlit  KPIs + trajectoires Cold Path |
|  **API Swagger** | http://localhost:8001/docs | Documentation interactive |
|  **Redpanda UI** | http://localhost:8080 | Visualisation topics Kafka en live |
|  **RedisInsight** | http://localhost:5540 | GUI Redis  cls GEO + hashes |
|  **Grafana** | http://localhost:3000 | Dashboard Prometheus (admin / fleetstream) |
|  **Prometheus** | http://localhost:9090 | Mtriques brutes |

---

## Architecture Lambda

```
                      
                                          REDPANDA                         
                (Kafka-compatible, sans Zookeeper)         
     PRODUCER     Topic: livreurs-gps (3 partitions, LZ4, 24h TTL)  
   100 livreurs     
     asyncio                           
              
                                                   
                        
                       HOT PATH            COLD PATH     
                      hot-consumer        cold-consumer  
                     group: hot          group: cold     
                     offset: latest      offset: earliest
                        
                                                   
                        
                      REDIS STACK         DATA LAKE      
                      GEOADD + TTL        Parquet/Snappy 
                        30 sec            Hive-partition 
                       < 10 ms            year/month/... 
                        
                                                   
                              
                                         
                               
                                  FASTAPI       
                                 Serving Layer  
                                                
                                /map             Dashboard Leaflet (mme origin)
                                /livreurs-proches GEOSEARCH Redis (<10ms)
                                /analytics/*     DuckDB sur Parquet
                                /metrics         Prometheus scrape
                               
```

### Pourquoi Lambda et pas Kappa ?

| Critre | Lambda (ce projet) | Kappa |
|---|---|---|
| Hot Path | Redis (TTL 30s, <10ms) | Stream processing continu |
| Cold Path | Parquet (batch, analytics) | Mme pipeline, fentres longues |
| Complexit | 2 consumers distincts | 1 pipeline unifi |
| ML-readiness | Parquet + DuckDB natif | Dpend du framework stream |

---

## Stack technique

| Composant | Technologie | Rle |
|---|---|---|
| Message Broker | **Redpanda v23.3** | Kafka-compatible, sans Zookeeper |
| Speed Layer | **Redis Stack 7.2** | GEOADD + GEOSEARCH, TTL 30s |
| Batch Layer | **Apache Parquet + PyArrow** | Snappy, hive-partitionn |
| Analytics | **DuckDB** | SQL sur Parquet, predicate pushdown |
| API | **FastAPI + uvicorn** | Serving layer + `/metrics` Prometheus |
| Carte Live | **Leaflet.js** | Marqueurs mis  jour en place (setLatLng) |
| Analytics UI | **Streamlit** | KPIs + trajectoires Cold Path |
| Monitoring | **Prometheus + Grafana** | Dashboard auto-provisionn |
| Simulation | **asyncio + aiokafka** | 100 livreurs, mouvement raliste |

---

## API  Endpoints

### Hot Path (Redis  <10ms)

```bash
# Livreurs dans un rayon autour de Times Square
curl "http://localhost:8001/livreurs-proches?lat=40.7580&lon=-73.9855&rayon=2"

# Position d'un livreur prcis
curl "http://localhost:8001/livreurs/L007"

# Mtriques temps rel
curl "http://localhost:8001/stats"

# Benchmark SLA Redis (proof pour jury)
curl "http://localhost:8001/health/performance?samples=200"
```

### Cold Path (DuckDB / Parquet)

```bash
# Historique + stats de trajectoire (distance Haversine, vitesse moy/max)
curl "http://localhost:8001/analytics/history/L042?heures=1"

# Heatmap de densit (zones surge pricing)
curl "http://localhost:8001/analytics/heatmap?heures=1&resolution=0.01"
```

**Exemple  rponse `/analytics/history/L042` :**
```json
{
  "livreur_id": "L042",
  "heures": 1,
  "resume": {
    "nb_points": 414,
    "distance_totale_km": 1.88,
    "vitesse_moy_kmh": 16.4,
    "vitesse_max_kmh": 30.0,
    "statut_dominant": "delivering"
  },
  "trajectory": [...]
}
```

---

## Dashboard Carte Live  Zro Clignotement

**http://localhost:8001/map**

La carte utilise Leaflet.js avec une technique anti-clignotement :
- On maintient un `Map<livreur_id, marker>` en mmoire JavaScript
- Toutes les 2 secondes : `fetch('/livreurs-proches')` silencieux
- Les marqueurs existants sont **mis  jour en place** (`marker.setLatLng()`)  la carte ne se recharge jamais
- Les nouveaux livreurs reoivent un marqueur, les disparus (TTL expir) sont supprims

```
 Orange   En livraison
 Vert     Disponible
 Gris     Inactif
```

---

## Performances mesures

| Mtrique | Valeur |
|---|---|
| Throughput TLC replay | ~250-800 courses concurrentes (sample rate configurable) |
| Latence GEOSEARCH p50 | ~0.8 ms |
| Latence GEOSEARCH p99 | ~2.3 ms  (SLA < 10ms) |
| Taille Parquet (1h) | ~8-12 MB (Snappy) |
| Requte DuckDB (1h) | <200 ms |

---

## Structure du projet

```
FleetStream/
 docker-compose.yml        # 11 services
 .env.example              # Variables configurables
 Makefile                  # make up / logs / demo / stress
 stress_test.py            # 1k5k livreurs, benchmark p50/p95/p99

 tlc_replay/               # NYC TLC HVFHV replay  positions + offers + events (100% vraies trips Uber)
 context_poller/           # Poll Citi Bike + Open-Meteo + NYC 311 + NYC Events  context-signals-v1
 hot_path/                 # Speed Layer  Redis GEOADD (TTL 30s)
 cold_path/                # Batch Layer  Parquet Snappy hive-partitionn

 api/                      # FastAPI (Hot + Cold + /map + /metrics)
    static/index.html     # Dashboard Leaflet servi  /map

 dashboard/                # Streamlit analytics (KPIs + Cold Path)

 monitoring/
    prometheus.yml        # Scrape API + Redpanda
    grafana/              # Dashboard JSON auto-provisionn

 data/parquet/             # Data Lake local (gitignored)
     year=YYYY/month=MM/day=DD/hour=HH/
         batch_*.parquet
```

---

## Stress Test

```bash
# 1 000 livreurs (1 000 msg/s) pendant 30s
make stress

# 5 000 livreurs (5 000 msg/s) pendant 60s
make stress-5k

# Benchmark API uniquement (500 requtes GEOSEARCH parallles)
make stress-api
```

Sortie typique :
```
KAFKA / REDPANDA:
  Messages envoys    : 30 000
  Dbit moyen         : 998 msg/s
  Taux de perte       : 0.0%

API  GEOSEARCH [ SLA OK (p99 < 10ms)]
  P50                 : 0.83 ms
  P95                 : 1.77 ms
  P99                 : 2.34 ms
```

---

## RedisInsight  Visualisation

1. Ouvrir http://localhost:5540
2. Cliquer **+ Add Redis Database**
3. Host: `redis`  Port: `6379`  **Add Database**
4. Explorer les cls :
   - `fleet:geo`  Sorted set gospatial (toutes les positions)
   - `fleet:livreur:L042`  Hash avec mtadonnes
   - `fleet:stats:*`  Compteurs monitoring

---

## Machine Learning  Cold Path

```python
import duckdb

conn = duckdb.connect()
df = conn.execute("""
    SELECT
        ROUND(lat / 0.01, 0) * 0.01  AS zone_lat,
        ROUND(lon / 0.01, 0) * 0.01  AS zone_lon,
        HOUR(ts)                      AS heure,
        DAYOFWEEK(ts)                 AS jour_semaine,
        COUNT(*)                      AS nb_livreurs,
        AVG(speed_kmh)                AS vitesse_moy,
        SUM(CASE WHEN status='delivering' THEN 1 ELSE 0 END) AS en_livraison
    FROM read_parquet('data/parquet/**/*.parquet', hive_partitioning = true)
    GROUP BY zone_lat, zone_lon, heure, jour_semaine
    ORDER BY nb_livreurs DESC
""").df()
```

---

## Variables d'environnement

| Variable | Dfaut | Description |
|---|---|---|
| `TLC_MONTH` | `2024-01` | Mois de depart du replay historique (format `YYYY-MM`) |
| `TLC_MONTHS` | `` | Liste CSV explicite des mois a rejouer (prioritaire), ex: `2024-01,2024-02,...,2024-10` |
| `TLC_MONTH_COUNT` | `0` | Nombre de mois consecutifs a rejouer a partir de `TLC_MONTH` (`10` pour 10 mois, `12` pour 1 an) |
| `TLC_SPEED_FACTOR` | `1` | 1 = temps rel NYC, 6 = 6 acclr |
| `TLC_TRIP_SAMPLE_RATE` | `0.15` | Fraction de trips Uber rejoues ( nb courses concurrentes) |
| `TLC_MAX_ACTIVE_TRIPS` | `800` | Plafond dur de courses simultanes |
| `TLC_COURIER_ID_MODE` | `trip` | Strategie d'identite chauffeur synthetique: `trip` (1 ID par course, evite les teleports inter-courses) ou `legacy_zone_day` (mode historique) |
| `TLC_ROUTE_MODE` | `osrm` | Mode trajectoire pour le replay TLC: `osrm` (route-aware) ou `linear` (fallback legacy) |
| `TLC_ROUTE_OSRM_URL` | `http://osrm:5000` | Endpoint OSRM utilis par le replay TLC pour router pickupdropoff |
| `TLC_ROUTE_OSRM_FALLBACK_URL` | `https://router.project-osrm.org` | Fallback OSRM public si le serveur local est indisponible (route-aware prioritaire) |
| `TLC_ROUTE_FETCH_TIMEOUT_S` | `0.7` | Budget max (s) du fetch route bloquant par course; au-del, fallback linear + prfetch async |
| `TLC_ROUTE_PREFETCH_ENABLED` | `true` | Active le prchargement asynchrone des routes sur les zone-pairs manquants |
| `TLC_ROUTE_PREFETCH_MAX_PENDING` | `256` | Nombre max de prefetch routes en attente |
| `TLC_ROUTE_PREFETCH_MAX_CONCURRENCY` | `8` | Concurrence max des requtes de prefetch route |
| `TLC_ROUTE_OSRM_TIMEOUT_S` | `4.0` | Timeout HTTP OSRM (secondes) |
| `TLC_ROUTE_CACHE_MAX` | `4096` | Taille max du cache de routes TLC (cl `pickup_zone + dropoff_zone`) |
| `DRIVER_INGEST_TOKEN` | `dev-insecure-token` | Token du gateway mobile `driver-ingest` |
| `CONTEXT_TICK_SECONDS` | `30` | Frquence de publication des signaux de contexte (263 zones) |
| `EVENTS_POLL_SECONDS` | `600` | Frquence de polling de la source NYC events (`tvpp-9vvx`) |
| `EVENT_RADIUS_KM` | `4.5` | Rayon zone/event utilis pour `event_pressure` |
| `EVENT_LOOKAHEAD_HOURS` | `6` | Fentre de prise en compte des vnements  venir |
| `EVENT_PRESSURE_CAP` | `0.75` | Cap de contribution events dans `demand_index` |
| `GPS_TTL_SECONDS` | `30` | TTL Redis |
| `BATCH_INTERVAL_SECONDS` | `60` | Frquence flush Parquet |
| `MAX_BATCH_RECORDS` | `50000` | Taille max buffer cold path |

---

## Commandes utiles

```bash
make up              # Lance tout le stack
make down            # Stoppe tout
make logs            # Logs en temps rel
make logs-api        # Logs API uniquement
make demo            # Requtes de dmonstration + benchmark
make stress          # Stress test 1000 livreurs
make stress-5k       # Stress test 5000 livreurs
make redis-cli       # Accs Redis CLI interactif
make clean           # Supprime containers + volumes + Parquet

#  Demo & Jury 
make demo-scoreboard # Backtest Copilot vs baselines (CSV + table)
make demo-scenarios  # 5 scnarios de dmo (pluie, trafic, fuel, event, baseline)
make build-mart      # Construit le Data Mart analytique (DuckDB + Parquet)
make query-kpis      # 10 KPIs en < 300ms depuis le Data Mart

# Mode flotte multi-chauffeurs (250 drivers)
make fleet-demo-up   # Lance le stack en mode fleet_demo.env
make fleet-demo-check # Vrifie la readiness (chauffeurs actifs, KPIs)
make fleet-demo-down  # Stoppe

# Script jury one-click (Windows PowerShell)
# .\scripts\demo-jury.ps1
# .\scripts\demo-jury.ps1 -Fleet -MinDrivers 50
```

---

## Auteur

Projet acadmique **CY Tech  ING3 Big Data**.
Architecture Lambda/Kappa applique au suivi de flotte en temps rel.

---

## Copilot Uber Eats MVP (v1)

This repository now includes a local-first copilot workflow for courier decision support.

### New event contract (Protobuf)

- Schema file: `schemas/copilot_events.proto`
- Generated code: `schemas/gen/copilot_events_pb2.py`
- Registry helper: `scripts/register-schemas.ps1` (uses `schemas/register_schemas.py`)

### New streaming topics

- `order-offers-v1`
- `order-events-v1`
- `context-signals-v1`

`livreurs-gps` is preserved for live courier positions.

### New services

- `copilot-features`: consumes offers/context and writes realtime feature vectors into Redis.
- `driver-ingest`: secure HTTP gateway for real mobile GPS ingestion (`CourierPositionV1` -> `livreurs-gps`).
- `driver-ingest` now carries a platform-agnostic contract via `source_platform`
  (Uber Eats, DoorDash, Deliveroo, custom adapters) while keeping the same protobuf backbone.
- Existing services (`tlc-replay`, `context-poller`, `hot-consumer`, `cold-consumer`, `api`) now support the copilot event flow.

### New API endpoints

- `POST /copilot/score-offer`
- `GET /copilot/driver/{id}/next-best-zone`
- `GET /copilot/driver/{id}/offers`
- `GET /copilot/replay`
- `GET /copilot/health`
- `GET /copilot` (mobile-first PWA)

### Scoring v2 tuning knobs

Scoring/ranking can be tuned at runtime via env vars (defaults shown in `.env.example`):

- score weights: `COPILOT_SCORE_W_NET_HOURLY`, `COPILOT_SCORE_W_NET_TRIP`, `COPILOT_SCORE_W_FUEL`, `COPILOT_SCORE_W_TIME`, `COPILOT_SCORE_W_CONTEXT`
- hybrid accept threshold: `COPILOT_ACCEPT_BASE_THRESHOLD`, `COPILOT_ACCEPT_BELOW_TARGET_PENALTY_MAX`, `COPILOT_ACCEPT_HIGH_FUEL_PENALTY`, `COPILOT_ACCEPT_ABOVE_TARGET_BONUS`
- ranking labels: `COPILOT_RANK_REJECT_EUR_H_DEFAULT`, `COPILOT_RANK_TOP_PICK_MIN_EUR_H`, `COPILOT_RANK_TOP_PICK_MIN_EDGE_EUR_H`, `COPILOT_RANK_BELOW_TARGET_SLACK_EUR_H`

`POST /copilot/score-offer`, `POST /copilot/rank-offers`, and `GET /copilot/driver/{id}/offers` now expose an optional `explanation_details` field while preserving existing `explanation` compatibility.
The scoring payload also exposes `decision_threshold` so UI/ops can understand why a borderline offer was accepted or rejected.

### Driver ingest API (real devices)

Gateway URL: `http://localhost:8010`

- `GET /healthz`
- `POST /ingest/v1/position`
- `POST /ingest/v1/positions`

Authentication:

- `Authorization: Bearer <token>` (or `X-Driver-Token`)
- local sandbox token default: `dev-insecure-token` (change in production)

### Driver PWA UX (lot 3)

- realtime health banner (ML mode + quality gate + metrics)
- driver session controls with quick windows (`Last 30m`, `Last 2h`, `Today`)
- KPI strip (`offers`, `accept rate`, `avg EUR/h`, `replay events`)
- manual scoring with presets (`Safe`, `Balanced`, `Surge`)
- one-tap scoring from cached offers
- offer filtering (`all` / `accept` / `reject`)
- ranked next-best zones with opportunity meter
- replay timeline with event-type summary

### Lot 4 evidence output

Running `make perf-lot4` generates:

- JSON report in `data/reports/perf_lot4_*.json`
- markdown summary in `docs/preuve-technique.md`

The report checks:

- hot path geosearch p99
- copilot score-offer p95
- ingestion throughput delta
- no new DLQ errors during the benchmark window

Notes:

- historical DLQ files do not fail the global `passed` by default
- to enforce strict empty-DLQ mode, run:
  `python scripts/perf-lot4.py --require-dlq-empty ...`

### New storage layout

- Position history stays in `data/parquet/`.
- Copilot business events are persisted in `data/parquet_events/`.

### Local ML workflow

Train model artifact locally:

```bash
make train-copilot
```

By default, training uses full history (`--train-months 0`).
To constrain to a fixed window (for faster experiments):

```bash
python ml/train_copilot_model.py \
  --data ./data/parquet_events \
  --out ./data/models/copilot_model.joblib \
  --train-start 2024-01 \
  --train-months 10
```

Run the reproducible training demo notebook (COP-014):

```bash
make train-copilot-report
```

Or directly:

```bash
jupyter nbconvert --execute --to notebook --inplace ml/notebooks/copilot_training_report.ipynb
```

Output:

- `data/models/copilot_model.joblib`
- `data/models/copilot_model.json`
- `data/reports/ml/copilot_training_report.json`
- `data/reports/ml/copilot_training_report.md`
- `data/reports/ml/copilot_roc_curve.png`
- `data/reports/ml/copilot_pr_curve.png`
- `data/reports/ml/copilot_calibration_curve.png`
- `data/reports/ml/copilot_feature_importance.png`

The metadata file now contains:

- quality metrics (`roc_auc`, `average_precision`, `brier_score`, `ece_10_bins`)
- label source distribution (`observed_realized`, `observed_rejected`, `accepted_proxy`, fallback)
- training parameters (`label_threshold_eur_h`, `context_window_minutes`)

At API startup, a quality gate validates the model before enabling ML scoring:

- `COPILOT_MODEL_MIN_ROWS`
- `COPILOT_MODEL_MIN_AUC`
- `COPILOT_MODEL_MIN_AVG_PRECISION`

If the gate fails, the API falls back to heuristic scoring automatically.

### Data source modes

#### 1) Simulation mode (default) - NYC TLC HVFHV replay

`tlc-replay` streams historical Uber NYC trips and emits:

- `OrderOfferV1` at `request_datetime`
- `OrderEventV1(accepted)` right after the offer
- `CourierPositionV1` at pickup, then along an OSRM route geometry when available
  (automatic fallback to legacy linear interpolation if OSRM is unavailable)
- `OrderEventV1(dropped_off)` at `dropoff_datetime`

Replay status metrics now expose route mode and routing counters (`trajectory_mode`,
`route_osrm_success`, `route_linear_fallback`, `route_cache_hits`, etc.).
For replay-generated events, `source_platform` is tagged with `|route=osrm|linear`
to expose the mode used per trip.

Replay long history (PowerShell examples):

```powershell
# 10 months from Jan 2024 (2024-01 .. 2024-10)
$env:TLC_MONTH="2024-01"; $env:TLC_MONTH_COUNT="10"; docker compose up -d --force-recreate tlc-replay

# 12 months from Jan 2024 (full year)
$env:TLC_MONTH="2024-01"; $env:TLC_MONTH_COUNT="12"; docker compose up -d --force-recreate tlc-replay
```

If you want exact custom months instead of a range:

```powershell
$env:TLC_MONTHS="2024-01,2024-02,2024-03,2024-04"; docker compose up -d --force-recreate tlc-replay
```

Quick visual before/after for COP-013 trajectory fix:

```powershell
# Legacy baseline (straight line)
$env:TLC_ROUTE_MODE="linear"; docker compose up -d --force-recreate tlc-replay

# Route-aware mode (OSRM)
$env:TLC_ROUTE_MODE="osrm"; $env:TLC_ROUTE_OSRM_URL="http://osrm:5000"; docker compose up -d --force-recreate tlc-replay
```

For true local OSRM routing (recommended for demos with many drivers), you must prepare
the OSRM files once under `data/osrm/` and run the routing profile:

```powershell
New-Item -ItemType Directory -Force data\osrm | Out-Null
Invoke-WebRequest `
  -Uri "https://download.geofabrik.de/north-america/us/new-york-latest.osm.pbf" `
  -OutFile "data/osrm/new-york-latest.osm.pbf"

docker run --rm -t -v "${PWD}\data\osrm:/data" ghcr.io/project-osrm/osrm-backend:v5.27.1 `
  osrm-extract -p /opt/car.lua /data/new-york-latest.osm.pbf
docker run --rm -t -v "${PWD}\data\osrm:/data" ghcr.io/project-osrm/osrm-backend:v5.27.1 `
  osrm-partition /data/new-york-latest.osrm
docker run --rm -t -v "${PWD}\data\osrm:/data" ghcr.io/project-osrm/osrm-backend:v5.27.1 `
  osrm-customize /data/new-york-latest.osrm

docker compose --profile routing up -d osrm
```

If local OSRM is down or files are missing, replay now falls back to a non-blocking
mode (`linear` with orthogonal path + smooth front interpolation) so the demo keeps
running without teleports/crashes.

Open `http://localhost:8001/map` and compare trajectory realism on the same pickup/dropoff zones.

Lat/lon come from the 263 NYC taxi zone centroids (pre-computed once with DuckDB
spatial from the official TLC shapefile and shipped as `tlc_replay/nyc_zone_centroids.json`).
To regenerate the file:

```bash
python scripts/gen_nyc_zone_centroids.py
```

#### 2) Real mode - mobile GPS ingestion

`driver-ingest` receives real courier GPS from your mobile app / SDK and publishes
`CourierPositionV1` directly into `livreurs-gps` (then consumed by `hot-consumer`).

Switch commands:

```bash
make real-mode   # stop tlc-replay, keep real GPS only
make sim-mode    # restart tlc-replay
```

Quick ingest check:

```bash
make demo-ingest
```

### Context streaming  real public APIs

The `context-poller` service streams `ContextSignalV1` events for each of the 263
NYC taxi zones every `CONTEXT_TICK_SECONDS`, using only public API data:

- **Citi Bike GBFS** (`gbfs.lyft.com/gbfs/2.3/bkn/`)  `demand_index`
- **Open-Meteo** (`api.open-meteo.com`)  `weather_factor`
- **NYC 311 Socrata** (`data.cityofnewyork.us/resource/erm2-nwe9.json`)  `traffic_factor`
- **NYC DOT Weekly Traffic Advisory** (`nyc.gov/.../motorist/weektraf.shtml`)  `closure_pressure` (planned closures)
- **NYC DOT Traffic Speeds Feed** (`linkdata.nyctmc.org/.../LinkSpeedQuery.txt`)  `speed_pressure` (real-time slowdown)
  - unit conversion behavior is configurable via `TRAFFIC_SPEED_INPUT_UNIT` (`mph` by default)
- **NYC Permitted Event Information** (`data.cityofnewyork.us/resource/tvpp-9vvx.json`)  `event_pressure` inject dans `demand_index` (cap)
- **Redis GEO** (`fleet:geo`, populated by `hot-consumer` from TLC positions)  `supply_index`

There is no synthetic context anywhere in the platform.
`GET /copilot/health` expose aussi `events_context` (source, statut, fentre, volumtrie) et les KPI de diffusion `event_pressure_*`.

### Developer checks

```bash
python -m ruff check tlc_replay context_poller hot_path cold_path api copilot_features ml tests schemas/register_schemas.py
python -m pytest -q
```

### New helper commands

```bash
make demo-copilot
make bench-copilot
make smoke-e2e
make perf-lot4
make proof-lot4
```

### DuckDB local script updates

`./scripts/duckdb-local.ps1` now supports:

- `events-count`
- `events-sample`

for `copilot_events` view.

