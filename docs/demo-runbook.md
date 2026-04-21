# Demo Runbook — 5 minutes Jury

## Lancement (une commande)

```powershell
# Windows
.\scripts\demo_run.ps1

# Linux / Mac
make up && sleep 30 && python3 ml/backtest_copilot.py
```

---

## Flow de narration (5 min)

### Min 0–1 : Contexte "le problème"

> "FleetStream est un copilote décisionnel temps réel pour livreurs VTC NYC.
> Chaque offre de mission dure 8 secondes — le chauffeur accepte ou refuse.
> Notre Copilot score chaque offre en <50 ms et recommande la meilleure action."

Montrer : `http://localhost:8001/copilot` → PWA avec chip d'offre en direct.

---

### Min 1–2 : Pipeline Lambda en direct

> "Les données GPS arrivent via Redpanda, traitées sur deux chemins :"

- **Hot path** → Redis (latence p99 < 10 ms) → scoring temps réel
- **Cold path** → Parquet partitionné → analytics DuckDB

Montrer :
- Redpanda UI `http://localhost:8080` → topic `livreurs-gps` qui reçoit des messages
- `curl http://localhost:8001/stats` → débit en direct

---

### Min 2–3 : Copilot en action

> "Voici le score d'une offre en live :"

```bash
curl -X POST http://localhost:8001/copilot/score-offer \
  -H "Content-Type: application/json" \
  -d '{"courier_id":"drv_demo_001","offer_id":"o1","estimated_fare_eur":14,"estimated_distance_km":5,"estimated_duration_min":20,"zone_id":"Z01","demand_index":0.8,"supply_index":0.3}'
```

> "Score 0.81 → ACCEPT. Le modèle ML LogReg entraîné sur TLC data explique :
> net €/h élevé + forte demande + faible supply = opportunité."

Montrer : `/copilot/health` → qualité contexte, alertes supply/routing.

---

### Min 3–4 : Preuve de valeur — Backtest

> "Combien vaut le Copilot vs une stratégie naïve ?"

Ouvrir `data/reports/backtest_summary.csv` ou lancer :

```bash
python3 ml/backtest_copilot.py
```

| Stratégie    | Accept% | Net €/h | Km   |
|--------------|---------|---------|------|
| copilot      | 92%     | 57 €/h  | ...  |
| greedy       | 89%     | 59 €/h  | ...  |
| random       | 59%     | 52 €/h  | ...  |
| always_accept| 100%    | 53 €/h  | ...  |

> "Le Copilot accepte sélectivement les offres à fort rendement, éliminant
> les courses non rentables tout en maintenant un taux d'acceptation élevé."

---

### Min 4–5 : Scénarios & Big Data

> "Pour valider la robustesse : 3 scénarios business."

```bash
python3 ml/scenario_generator.py --list
python3 ml/scenario_generator.py --scenario traffic_jam
```

> "En embouteillages : taux d'acceptation chute de 92% → 85%,
> le Copilot protège le chauffeur des courses non rentables.
> En pluie forte : demand +35%, Copilot capte les pics de surge."

Montrer : `data/scenarios/scenarios_comparison.json`

---

## Commandes de secours

```bash
# Redémarrer un service
make restart-api

# Vérifier les logs
make logs-api

# Reset complet
make down && make up

# Vérifier les tests
python3 -m pytest tests/ -q --tb=short
```

---

## KPIs attendus (critères de réussite)

| Métrique           | Cible    | Check                          |
|--------------------|----------|--------------------------------|
| Hot path p99       | < 10 ms  | `/health/performance`          |
| Score offer p95    | < 150 ms | `/health/performance`          |
| DLQ cold path      | 0 fichier| `ls data/dlq/`                 |
| Tests unitaires    | 100% pass| `pytest tests/ -q`             |
| Backtest CSV       | généré   | `data/reports/backtest_summary.csv` |
