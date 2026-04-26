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

> "FleetStream est un copilote décisionnel temps réel pour livreurs de nourriture à New York.
> Chaque offre de mission dure 8 secondes — le livreur accepte ou refuse.
> Notre Copilot score chaque offre en <50 ms et recommande la meilleure action."

Montrer : `http://localhost:8001/copilot/driver-app` → app livreur Copilot dédiée.
Secours si besoin : `http://localhost:8001/copilot` → PWA opérateur complète.

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
> net $/h élevé + forte demande + faible supply = opportunité."

Montrer : `/copilot/health` → qualité contexte, alertes supply/routing.

---

### Min 3–4 : Preuve de valeur — Backtest

> "Combien vaut le Copilot vs une stratégie naïve ?"

Ouvrir `data/reports/backtest_summary.csv` ou lancer :

```bash
python3 ml/backtest_copilot.py
```

| Stratégie    | Accept% | Net $/h | Km   |
|--------------|---------|---------|------|
| copilot      | 92%     | 57 $/h  | ...  |
| greedy       | 89%     | 59 $/h  | ...  |
| random       | 59%     | 52 $/h  | ...  |
| always_accept| 100%    | 53 $/h  | ...  |

> "Le Copilot accepte sélectivement les offres à fort rendement, éliminant
> les livraisons non rentables tout en maintenant un taux d'acceptation élevé."

---

### Min 4–5 : Scénarios & Big Data

> "Pour valider la robustesse : 3 scénarios business."

```bash
python3 ml/scenario_generator.py --list
python3 ml/scenario_generator.py --scenario traffic_jam
```

> "En embouteillages : taux d'acceptation chute de 92% → 85%,
> le Copilot protège le livreur des livraisons non rentables.
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

---

## Question Jury Probable : "Pourquoi vos métriques ML sont si hautes ?"

### Réponse courte prête à dire

> "Oui, les métriques sont très élevées, et on l'assume comme un point à challenger.
> On a justement gardé ce signal visible dans `/copilot/health` avec un flag qualité
> `temporal_auc_extremely_high` pour ne pas masquer le risque. Notre lecture honnête,
> c'est que le problème est probablement très séparable sur cette fenêtre TLC, mais
> qu'il faut encore vérifier plus profondément l'absence de fuite de variables ou un
> train/test trop favorable avant d'en faire une promesse produit forte."

### Les chiffres exacts à connaître

- Modèle : `copilot_v2`
- Entraîné le `2026-04-20T22:34:47Z`
- Fenêtre d'entraînement : `2024-01-01` -> `2024-11-01`
- Split déclaré : `temporal`
- `roc_auc = 0.999490138364625`
- `average_precision = 0.9985776645784981`
- Flag qualité : `temporal_auc_extremely_high`

### Si le jury insiste

- "On ne présente pas ce score comme une vérité business définitive, mais comme un résultat expérimental très fort qui doit être audité."
- "Le garde-fou important, c'est qu'on expose explicitement le `model_quality_gate` dans l'API au lieu de cacher ce type de signal."
- "La vraie valeur de la soutenance est surtout l'architecture Big Data temps réel, la robustesse de la simulation, le hot path Redis, le cold path Parquet/DuckDB, et l'intégration du scoring dans la décision opérationnelle."

### Ce qu'il ne faut pas dire

- Ne pas dire : "le modèle est parfait"
- Ne pas dire : "cela prouve qu'il généralisera en production"
- Préférer : "résultat prometteur, mais encore à auditer sur la qualité du protocole d'évaluation"
