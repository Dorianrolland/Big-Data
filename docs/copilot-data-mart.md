# Copilot Data Mart — Guide DuckDB

## Architecture

```
data/parquet_events/          ← Cold Path (stream Redpanda → Parquet)
  topic=order-offers-v1/
  topic=order-events-v1/
  topic=context-signals-v1/
      └── year=.../month=.../day=.../hour=.../*.parquet

data/marts/copilot/           ← Data Mart (tables optimisées)
  fact_offers.parquet
  fact_order_events.parquet
  fact_context_signals.parquet
  fact_missions.parquet
```

## Construire le mart

```bash
python3 scripts/build-copilot-mart.py
# ou: make demo-scoreboard (inclut backtest)
```

## 10 KPIs en une commande

```bash
python3 scripts/query-copilot-kpis.py
python3 scripts/query-copilot-kpis.py --fmt json > data/reports/kpis.json
```

## Requêtes DuckDB de démonstration

### 1. Volume total et taux de surge

```sql
SELECT
    COUNT(*) AS total_signals,
    ROUND(100.0 * SUM(CASE WHEN surge_candidate THEN 1 ELSE 0 END) / COUNT(*), 2) AS surge_rate_pct,
    ROUND(AVG(pressure_ratio), 4) AS avg_pressure_ratio
FROM 'data/marts/copilot/fact_context_signals.parquet';
```

### 2. Profil horaire de la demande

```sql
SELECT
    hour,
    ROUND(AVG(demand_index), 4) AS avg_demand,
    ROUND(AVG(supply_index), 4) AS avg_supply,
    ROUND(AVG(pressure_ratio), 4) AS avg_pressure,
    COUNT(*) AS n
FROM 'data/marts/copilot/fact_context_signals.parquet'
GROUP BY hour
ORDER BY avg_demand DESC;
```

### 3. Impact météo sur la demande

```sql
SELECT
    ROUND(weather_factor, 1) AS weather_bucket,
    ROUND(AVG(demand_index), 4) AS avg_demand,
    ROUND(AVG(pressure_ratio), 4) AS avg_pressure,
    COUNT(*) AS n
FROM 'data/marts/copilot/fact_context_signals.parquet'
GROUP BY weather_bucket
ORDER BY weather_bucket;
```

### 4. Répartition par source_platform

```sql
SELECT source_platform, COUNT(*) AS cnt
FROM 'data/marts/copilot/fact_context_signals.parquet'
GROUP BY source_platform
ORDER BY cnt DESC;
```

### 5. Top zones par pression (opportunités Copilot)

```sql
SELECT
    zone_id,
    ROUND(AVG(pressure_ratio), 4) AS avg_pressure,
    ROUND(AVG(demand_index), 4) AS avg_demand,
    COUNT(*) AS n
FROM 'data/marts/copilot/fact_context_signals.parquet'
GROUP BY zone_id
ORDER BY avg_pressure DESC
LIMIT 10;
```

### 6. Rentabilité des offres Copilot

```sql
SELECT
    zone_id,
    ROUND(AVG(net_eur_h), 2) AS avg_net_eur_h,
    ROUND(AVG(net_eur), 2) AS avg_net_eur,
    ROUND(AVG(fuel_cost_eur), 2) AS avg_fuel,
    COUNT(*) AS offers
FROM 'data/marts/copilot/fact_offers.parquet'
GROUP BY zone_id
ORDER BY avg_net_eur_h DESC;
```

### 7. Jointure cross-tables (signal contexte → offre)

```sql
SELECT
    cs.zone_id,
    ROUND(AVG(cs.pressure_ratio), 4) AS zone_pressure,
    ROUND(AVG(o.net_eur_h), 2) AS avg_offer_net_eur_h
FROM 'data/marts/copilot/fact_context_signals.parquet' cs
LEFT JOIN 'data/marts/copilot/fact_offers.parquet' o USING (zone_id)
GROUP BY cs.zone_id
ORDER BY zone_pressure DESC
LIMIT 10;
```

### 8. Fenêtre glissante 1h (time series)

```sql
SELECT
    DATE_TRUNC('hour', ts) AS hour_bucket,
    COUNT(*) AS signals_per_hour,
    ROUND(AVG(demand_index), 4) AS avg_demand,
    ROUND(AVG(pressure_ratio), 4) AS avg_pressure
FROM 'data/marts/copilot/fact_context_signals.parquet'
GROUP BY hour_bucket
ORDER BY hour_bucket;
```

## En session Python interactive

```python
import duckdb
con = duckdb.connect()

# Accès direct depuis Python
df = con.execute("""
    SELECT zone_id, ROUND(AVG(pressure_ratio), 4) AS avg_pressure, COUNT(*) AS n
    FROM 'data/marts/copilot/fact_context_signals.parquet'
    GROUP BY zone_id ORDER BY avg_pressure DESC LIMIT 10
""").fetchdf()
print(df)
```

## Critères de validation Big Data (jury)

| Critère              | Valeur observée          | Commande de vérification            |
|----------------------|--------------------------|-------------------------------------|
| Volume cold path     | 85k+ signaux contextuels | `python3 scripts/query-copilot-kpis.py` |
| Temps requête        | < 300 ms (10 KPIs)       | idem                                |
| Sources distinctes   | 3 topics Parquet         | `ls data/parquet_events/`           |
| Partitionnement Hive | year/month/day/hour      | `ls data/parquet_events/topic=*/`   |
| Mart construit       | 4 tables fact_*          | `ls data/marts/copilot/`            |
