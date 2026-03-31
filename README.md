# FleetStream

> Suivi temps réel d'une flotte de livreurs — Architecture Lambda sur Redpanda, Redis et Parquet.

**Cas d'usage** : Plateforme type Uber Eats gérant 100+ livreurs simultanément.
**Problème adressé** : SQL classique ne peut pas ingérer des milliers de coordonnées GPS par seconde tout en servant des requêtes géospatiales à faible latence.

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
                               │ /livreurs-proches◀── GEOSEARCH Redis
                               │ /analytics/*    ◀── DuckDB sur Parquet
                               └─────────────────┘
```

### Pourquoi Lambda et pas Kappa ?

| Critère | Lambda (ce projet) | Kappa |
|---|---|---|
| Hot Path | Redis (TTL 30s, <10ms) | Stream processing continu |
| Cold Path | Parquet (batch, analytics) | Même pipeline, fenêtres longues |
| Complexité | 2 consumers distincts | 1 pipeline unifié |
| ML-readiness | Parquet + DuckDB natif | Dépend du framework stream |

**Choix Lambda** : Le hot path et le cold path ont des exigences radicalement différentes (latence sub-10ms vs throughput analytique). Deux consumers spécialisés sont plus efficaces qu'un pipeline générique.

---

## Stack technique

| Composant | Technologie | Rôle |
|---|---|---|
| Message Broker | **Redpanda v23.3** | File de messages Kafka-compatible, sans Zookeeper |
| Speed Layer | **Redis Stack 7.2** | GEOADD + GEOSEARCH, TTL 30s, pipeline atomique |
| Batch Layer | **Apache Parquet + PyArrow** | Stockage colonnaire Snappy, hive-partitionné |
| Analytics | **DuckDB** | SQL sur Parquet en mémoire, predicate pushdown |
| API | **FastAPI + uvicorn** | Serving layer asynchrone, Swagger auto-généré |
| Simulation | **asyncio + aiokafka** | 100 livreurs, mouvement réaliste, 100 msg/s |

---

## Démarrage en une commande

```bash
git clone <repo-url> FleetStream && cd FleetStream
cp .env.example .env   # ou utiliser les valeurs par défaut
make up
```

Ou directement :

```bash
docker compose up --build -d
```

**Services disponibles :**

| Service | URL |
|---|---|
| API REST + Swagger | http://localhost:8001/docs |
| Redpanda Console (topics) | http://localhost:8080 |
| Redis (CLI) | `make redis-cli` |

---

## API — Exemples

### Hot Path (Redis — réponse <10ms)

**Trouver les livreurs dans un rayon de 2km autour de Notre-Dame :**
```bash
curl "http://localhost:8001/livreurs-proches?lat=48.8530&lon=2.3499&rayon=2"
```
```json
{
  "count": 8,
  "rayon_km": 2.0,
  "livreurs": [
    {
      "livreur_id": "L042",
      "lat": 48.8551,
      "lon": 2.3471,
      "speed_kmh": 22.3,
      "status": "delivering",
      "distance_km": 0.312
    }
  ]
}
```

**Filtrer par statut (livreurs disponibles uniquement) :**
```bash
curl "http://localhost:8001/livreurs-proches?lat=48.8566&lon=2.3522&rayon=5&statut=available"
```

**Position d'un livreur précis :**
```bash
curl "http://localhost:8001/livreurs/L007"
```

**Métriques temps réel :**
```bash
curl "http://localhost:8001/stats"
```
```json
{
  "hot_path": {
    "livreurs_actifs": 100,
    "messages_traites": 42180,
    "statuts": { "delivering": 65, "available": 25, "idle": 10 }
  },
  "cold_path": {
    "fichiers_parquet": 12,
    "taille_totale_mb": 3.4
  }
}
```

### Cold Path (DuckDB sur Parquet)

**Trajectoire des 2 dernières heures :**
```bash
curl "http://localhost:8001/analytics/trajectoire/L042?heures=2"
```

**Heatmap de densité (détection zones surge pricing) :**
```bash
curl "http://localhost:8001/analytics/heatmap?heures=1&resolution=0.01"
```

---

## Structure du projet

```
FleetStream/
├── docker-compose.yml        # Infrastructure complète (6 services)
├── .env                      # Variables d'environnement
├── Makefile                  # Commandes de développement
│
├── producer/                 # Simulation GPS 100 livreurs (asyncio)
│   ├── main.py               # Mouvement réaliste + hotspots restaurants
│   ├── Dockerfile
│   └── requirements.txt
│
├── hot_path/                 # Speed Layer → Redis GEOADD
│   ├── main.py               # Pipeline atomique, TTL 30s, <10ms
│   ├── Dockerfile
│   └── requirements.txt
│
├── cold_path/                # Batch Layer → Parquet Snappy
│   ├── main.py               # Hive-partitionné, flush 60s ou 50k records
│   ├── Dockerfile
│   └── requirements.txt
│
├── api/                      # Serving Layer FastAPI
│   ├── main.py               # Hot (Redis) + Cold (DuckDB) endpoints
│   ├── Dockerfile            # Multi-stage build
│   └── requirements.txt
│
└── data/                     # Data Lake local (gitignored)
    └── parquet/
        └── year=2024/month=01/day=15/hour=14/
            └── batch_*.parquet
```

---

## Flux de données détaillé

### 1. Production (100 msg/s)

Chaque livreur émet un message JSON toutes les secondes :

```json
{
  "livreur_id": "L042",
  "lat": 48.8566,
  "lon": 2.3522,
  "speed_kmh": 23.5,
  "heading_deg": 127.3,
  "status": "delivering",
  "accuracy_m": 5.2,
  "timestamp": "2024-01-15T14:23:45.123+00:00"
}
```

Le mouvement est simulé avec un **filtre passe-bas** (inertie physique) et un **rappel automatique** vers Paris si le livreur dépasse le périphérique.

### 2. Hot Path — Redis Stack

- **GEOADD** : met à jour la position dans le sorted set géospatial `fleet:geo`
- **HSET** : stocke les métadonnées dans `fleet:livreur:<id>`
- **EXPIRE** : TTL 30s — si un livreur s'arrête, il disparaît automatiquement
- **Pipeline** : toutes les commandes d'un batch envoyées en 1 round-trip réseau

### 3. Cold Path — Parquet

- Consumer group séparé (`cold-consumer`) avec `auto_offset_reset=earliest`
- Buffer en mémoire, flush déclenché par timer (60s) ou taille (50k records)
- Écriture PyArrow directe (sans pandas) — 3x plus rapide
- Partitionnement Hive : DuckDB fait du **predicate pushdown** automatique

### 4. Serving Layer — FastAPI

- **GEOSEARCH** Redis : trouve les N livreurs les plus proches en <10ms
- **DuckDB** : requête SQL analytique sur les fichiers Parquet locaux
- **Pipeline Redis** : récupère tous les hashes en 1 seul round-trip

---

## Performances observées

| Métrique | Valeur |
|---|---|
| Throughput producer | ~100 msg/s (configurable) |
| Latence hot path (GEOSEARCH) | <5ms (p99) |
| Latence hot path (HSET) | <2ms (p99) |
| Taille Parquet (1h, 100 livreurs) | ~8-12 MB (Snappy) |
| Temps requête DuckDB (1h de données) | <200ms |

---

## Variables d'environnement

| Variable | Défaut | Description |
|---|---|---|
| `NUM_LIVREURS` | `100` | Nombre de livreurs simulés |
| `EMIT_INTERVAL_MS` | `1000` | Intervalle d'émission (ms) |
| `GPS_TTL_SECONDS` | `30` | TTL des données Redis |
| `BATCH_INTERVAL_SECONDS` | `60` | Fréquence de flush Parquet |
| `MAX_BATCH_RECORDS` | `50000` | Taille max du buffer cold path |

---

## Cold Path et Machine Learning

Le data lake Parquet est conçu pour alimenter directement des pipelines ML :

```python
import duckdb

# Feature engineering pour la prédiction de surge pricing
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
    FROM read_parquet('data/parquet/**/*.parquet', hive_partitioning := true)
    GROUP BY zone_lat, zone_lon, heure, jour_semaine
    ORDER BY nb_livreurs DESC
""").df()
```

**Features générées** : densité par zone, vitesse moyenne, ratio de livraison actif, patterns horaires/hebdomadaires → inputs directs pour un modèle de **surge pricing** ou de **prédiction de disponibilité**.

---

## Commandes utiles

```bash
make up              # Lance tout le stack
make down            # Stoppe tout
make logs            # Logs en temps réel (tous services)
make logs-producer   # Logs du producer uniquement
make logs-api        # Logs de l'API
make status          # État des conteneurs
make demo            # Requêtes de démonstration
make redis-cli       # Accès shell Redis interactif
make clean           # Supprime containers + volumes + données Parquet
```

---

## Auteur

Projet académique CY Tech — Architecture Big Data (Lambda/Kappa).
