# FleetStream

> Suivi temps réel d'une flotte de livreurs — Architecture Lambda sur Redpanda, Redis Stack et Apache Parquet.

**Cas d'usage** : Plateforme type Uber Eats gérant 100+ livreurs simultanément à New York City.
**Problème adressé** : Le SQL classique ne peut pas ingérer des milliers de coordonnées GPS par seconde tout en servant des requêtes géospatiales à faible latence.

---

## Démarrage en une commande

```bash
git clone git@github.com:Dorianrolland/Big-Data.git FleetStream && cd FleetStream
make up
```

**Services disponibles immédiatement :**

| Interface | URL | Description |
|---|---|---|
| 🗺️ **Carte live** | http://localhost:8001/map | Dashboard HTML/JS Leaflet — zéro clignotement |
| 📊 **Analytics** | http://localhost:8501 | Streamlit — KPIs + trajectoires Cold Path |
| 📖 **API Swagger** | http://localhost:8001/docs | Documentation interactive |
| 📡 **Redpanda UI** | http://localhost:8080 | Visualisation topics Kafka en live |
| 🔍 **RedisInsight** | http://localhost:5540 | GUI Redis — clés GEO + hashes |
| 📈 **Grafana** | http://localhost:3000 | Dashboard Prometheus (admin / fleetstream) |
| ⚙️ **Prometheus** | http://localhost:9090 | Métriques brutes |

---

## Architecture Lambda

```
                      ┌─────────────────────────────────────────────────────┐
                      │                    REDPANDA                         │
  ┌──────────────┐    │          (Kafka-compatible, sans Zookeeper)         │
  │   PRODUCER   │───▶│  Topic: livreurs-gps (3 partitions, LZ4, 24h TTL)  │
  │ 100 livreurs │    └──────────────────┬──────────────────────────────────┘
  │   asyncio    │                       │
  └──────────────┘            ┌──────────┴──────────┐
                              │                     │
                    ┌─────────▼──────┐    ┌─────────▼──────┐
                    │   HOT PATH     │    │   COLD PATH     │
                    │  hot-consumer  │    │  cold-consumer  │
                    │ group: hot     │    │ group: cold     │
                    │ offset: latest │    │ offset: earliest│
                    └─────────┬──────┘    └─────────┬──────┘
                              │                     │
                    ┌─────────▼──────┐    ┌─────────▼──────┐
                    │  REDIS STACK   │    │  DATA LAKE      │
                    │  GEOADD + TTL  │    │  Parquet/Snappy │
                    │    30 sec      │    │  Hive-partition │
                    │   < 10 ms      │    │  year/month/... │
                    └─────────┬──────┘    └─────────┬──────┘
                              │                     │
                              └──────────┬──────────┘
                                         │
                               ┌─────────▼──────┐
                               │   FASTAPI       │
                               │  Serving Layer  │
                               │                 │
                               │ /map            ◀── Dashboard Leaflet (même origin)
                               │ /livreurs-proches◀── GEOSEARCH Redis (<10ms)
                               │ /analytics/*    ◀── DuckDB sur Parquet
                               │ /metrics        ◀── Prometheus scrape
                               └─────────────────┘
```

### Pourquoi Lambda et pas Kappa ?

| Critère | Lambda (ce projet) | Kappa |
|---|---|---|
| Hot Path | Redis (TTL 30s, <10ms) | Stream processing continu |
| Cold Path | Parquet (batch, analytics) | Même pipeline, fenêtres longues |
| Complexité | 2 consumers distincts | 1 pipeline unifié |
| ML-readiness | Parquet + DuckDB natif | Dépend du framework stream |

---

## Stack technique

| Composant | Technologie | Rôle |
|---|---|---|
| Message Broker | **Redpanda v23.3** | Kafka-compatible, sans Zookeeper |
| Speed Layer | **Redis Stack 7.2** | GEOADD + GEOSEARCH, TTL 30s |
| Batch Layer | **Apache Parquet + PyArrow** | Snappy, hive-partitionné |
| Analytics | **DuckDB** | SQL sur Parquet, predicate pushdown |
| API | **FastAPI + uvicorn** | Serving layer + `/metrics` Prometheus |
| Carte Live | **Leaflet.js** | Marqueurs mis à jour en place (setLatLng) |
| Analytics UI | **Streamlit** | KPIs + trajectoires Cold Path |
| Monitoring | **Prometheus + Grafana** | Dashboard auto-provisionné |
| Simulation | **asyncio + aiokafka** | 100 livreurs, mouvement réaliste |

---

## API — Endpoints

### Hot Path (Redis — <10ms)

```bash
# Livreurs dans un rayon autour de Times Square
curl "http://localhost:8001/livreurs-proches?lat=40.7580&lon=-73.9855&rayon=2"

# Position d'un livreur précis
curl "http://localhost:8001/livreurs/L007"

# Métriques temps réel
curl "http://localhost:8001/stats"

# Benchmark SLA Redis (proof pour jury)
curl "http://localhost:8001/health/performance?samples=200"
```

### Cold Path (DuckDB / Parquet)

```bash
# Historique + stats de trajectoire (distance Haversine, vitesse moy/max)
curl "http://localhost:8001/analytics/history/L042?heures=1"

# Heatmap de densité (zones surge pricing)
curl "http://localhost:8001/analytics/heatmap?heures=1&resolution=0.01"
```

**Exemple — réponse `/analytics/history/L042` :**
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

## Dashboard Carte Live — Zéro Clignotement

**http://localhost:8001/map**

La carte utilise Leaflet.js avec une technique anti-clignotement :
- On maintient un `Map<livreur_id, marker>` en mémoire JavaScript
- Toutes les 2 secondes : `fetch('/livreurs-proches')` silencieux
- Les marqueurs existants sont **mis à jour en place** (`marker.setLatLng()`) — la carte ne se recharge jamais
- Les nouveaux livreurs reçoivent un marqueur, les disparus (TTL expiré) sont supprimés

```
🟠 Orange  — En livraison
🟢 Vert    — Disponible
⚫ Gris    — Inactif
```

---

## Performances mesurées

| Métrique | Valeur |
|---|---|
| Throughput producer | 100 msg/s (configurable) |
| Latence GEOSEARCH p50 | ~0.8 ms |
| Latence GEOSEARCH p99 | ~2.3 ms ✅ (SLA < 10ms) |
| Taille Parquet (1h) | ~8-12 MB (Snappy) |
| Requête DuckDB (1h) | <200 ms |

---

## Structure du projet

```
FleetStream/
├── docker-compose.yml        # 11 services
├── .env.example              # Variables configurables
├── Makefile                  # make up / logs / demo / stress
├── stress_test.py            # 1k–5k livreurs, benchmark p50/p95/p99
│
├── producer/                 # 100 livreurs asyncio à NYC
├── hot_path/                 # Speed Layer → Redis GEOADD (TTL 30s)
├── cold_path/                # Batch Layer → Parquet Snappy hive-partitionné
│
├── api/                      # FastAPI (Hot + Cold + /map + /metrics)
│   └── static/index.html     # Dashboard Leaflet servi à /map
│
├── dashboard/                # Streamlit analytics (KPIs + Cold Path)
│
├── monitoring/
│   ├── prometheus.yml        # Scrape API + Redpanda
│   └── grafana/              # Dashboard JSON auto-provisionné
│
└── data/parquet/             # Data Lake local (gitignored)
    └── year=YYYY/month=MM/day=DD/hour=HH/
        └── batch_*.parquet
```

---

## Stress Test

```bash
# 1 000 livreurs (1 000 msg/s) pendant 30s
make stress

# 5 000 livreurs (5 000 msg/s) pendant 60s
make stress-5k

# Benchmark API uniquement (500 requêtes GEOSEARCH parallèles)
make stress-api
```

Sortie typique :
```
KAFKA / REDPANDA:
  Messages envoyés    : 30 000
  Débit moyen         : 998 msg/s
  Taux de perte       : 0.0%

API — GEOSEARCH [✅ SLA OK (p99 < 10ms)]
  P50                 : 0.83 ms
  P95                 : 1.77 ms
  P99                 : 2.34 ms
```

---

## RedisInsight — Visualisation

1. Ouvrir http://localhost:5540
2. Cliquer **+ Add Redis Database**
3. Host: `redis` · Port: `6379` → **Add Database**
4. Explorer les clés :
   - `fleet:geo` — Sorted set géospatial (toutes les positions)
   - `fleet:livreur:L042` — Hash avec métadonnées
   - `fleet:stats:*` — Compteurs monitoring

---

## Machine Learning — Cold Path

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

| Variable | Défaut | Description |
|---|---|---|
| `NUM_LIVREURS` | `100` | Livreurs simulés |
| `EMIT_INTERVAL_MS` | `1000` | Intervalle d'émission |
| `GPS_TTL_SECONDS` | `30` | TTL Redis |
| `BATCH_INTERVAL_SECONDS` | `60` | Fréquence flush Parquet |
| `MAX_BATCH_RECORDS` | `50000` | Taille max buffer cold path |

---

## Commandes utiles

```bash
make up              # Lance tout le stack
make down            # Stoppe tout
make logs            # Logs en temps réel
make logs-api        # Logs API uniquement
make demo            # Requêtes de démonstration + benchmark
make stress          # Stress test 1000 livreurs
make stress-5k       # Stress test 5000 livreurs
make redis-cli       # Accès Redis CLI interactif
make clean           # Supprime containers + volumes + Parquet
```

---

## Auteur

Projet académique **CY Tech — ING3 Big Data**.
Architecture Lambda/Kappa appliquée au suivi de flotte en temps réel.

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
- `uber-driver-connector`: optional connector to Uber Driver API (`/partners/me`, `/partners/trips`) that publishes normalized events to `order-events-v1`.
- Existing services (`producer`, `hot-consumer`, `cold-consumer`, `api`) now support the copilot event flow.

### New API endpoints

- `POST /copilot/score-offer`
- `GET /copilot/driver/{id}/next-best-zone`
- `GET /copilot/driver/{id}/offers`
- `GET /copilot/replay`
- `GET /copilot/health`
- `GET /copilot` (mobile-first PWA)

`GET /copilot/health` now includes `uber_connector` status from Redis.

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
- DLQ file count

### New storage layout

- Position history stays in `data/parquet/`.
- Copilot business events are persisted in `data/parquet_events/`.

### Local ML workflow

Train model artifact locally:

```bash
make train-copilot
```

Output:

- `data/models/copilot_model.joblib`
- `data/models/copilot_model.json`

The metadata file now contains:

- quality metrics (`roc_auc`, `average_precision`, `brier_score`, `ece_10_bins`)
- label source distribution (`observed_realized`, `observed_rejected`, `accepted_proxy`, fallback)
- training parameters (`label_threshold_eur_h`, `context_window_minutes`)

At API startup, a quality gate validates the model before enabling ML scoring:

- `COPILOT_MODEL_MIN_ROWS`
- `COPILOT_MODEL_MIN_AUC`
- `COPILOT_MODEL_MIN_AVG_PRECISION`

If the gate fails, the API falls back to heuristic scoring automatically.

### Uber Driver API integration (optional)

The connector is **on by default** and runs in degraded mode without token.

Enable with env vars:

- `UBER_CONNECTOR_ENABLED=true`
- `UBER_ENV=sandbox` (or `production`)
- `UBER_ACCESS_TOKEN=<oauth bearer>`

Output:

- normalized `order.event.v1` records into `order-events-v1`
- connector status visible in `/copilot/health -> uber_connector`

### Developer checks

```bash
python -m ruff check producer hot_path cold_path uber_connector api copilot_features ml tests schemas/register_schemas.py
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
