.PHONY: up down logs build clean restart status train-copilot train-copilot-10m demo-copilot demo-rank demo-next-zone bench-copilot smoke-e2e perf-lot4 proof-lot4 real-mode sim-mode demo-ingest prepare-routing-osrm single-driver-reset single-driver-up single-driver-down single-driver-logs focus-map

## Lance l'intégralité du stack (build + démarrage)
up:
	docker compose up --build -d
	@echo ""
	@echo "✓ FleetStream démarré !"
	@echo "  API docs       → http://localhost:8001/docs"
	@echo "  Dashboard live → http://localhost:8501"
	@echo "  Redpanda UI    → http://localhost:8080"
	@echo "  RedisInsight   → http://localhost:5540"
	@echo "  Grafana        → http://localhost:3000  (admin / fleetstream)"
	@echo "  Prometheus     → http://localhost:9090"
	@echo ""

## Stoppe tous les conteneurs
down:
	docker compose down

## Stoppe et supprime volumes + images buildées
clean:
	docker compose down -v --rmi local
	rm -rf data/parquet

## Affiche les logs de tous les services (follow)
logs:
	docker compose logs -f

## Logs d'un service spécifique : make logs-api, make logs-tlc-replay, etc.
logs-%:
	docker compose logs -f $*

## Rebuild + restart un service : make restart-tlc-replay
restart-%:
	docker compose up --build -d $*

## Statut des conteneurs
status:
	docker compose ps

## Accès Redis CLI
redis-cli:
	docker exec -it fleetstream-redis redis-cli

## Test de l'API (requiert curl + python3)
demo:
	@echo "→ Livreurs proches de Times Square (rayon 2km):"
	curl -s "http://localhost:8001/livreurs-proches?lat=40.7580&lon=-73.9855&rayon=2" | python3 -m json.tool
	@echo ""
	@echo "→ Stats temps réel:"
	curl -s "http://localhost:8001/stats" | python3 -m json.tool
	@echo ""
	@echo "→ Benchmark Redis (SLA proof):"
	curl -s "http://localhost:8001/health/performance?samples=200" | python3 -m json.tool

## Stress test (hors Docker — requiert: pip install aiokafka aiohttp)
stress:
	python3 stress_test.py --livreurs 1000 --duration 30

stress-5k:
	python3 stress_test.py --livreurs 5000 --duration 60

stress-api:
	python3 stress_test.py --skip-kafka --api-requests 1000

## Entraînement du modèle copilot (local)
train-copilot:
	python3 ml/train_copilot_model.py --data ./data/parquet_events --out ./data/models/copilot_model.joblib

## Entraînement du modèle copilot sur une fenêtre 10 mois fixe (split 10/2).
## Skippe automatiquement si le pickle existant correspond déjà à la fenêtre.
train-copilot-10m:
	python3 ml/train_copilot_model.py \
	    --data ./data/parquet_events \
	    --out ./data/models/copilot_model.joblib \
	    --train-start $${TLC_MONTH:-2024-01} \
	    --train-months 10

## Prépare les données OSRM pour New York (~1x, nécessite ~15GB disque).
prepare-routing-osrm:
	bash scripts/prepare_osrm.sh

## Démarre le scénario single-driver avec routage OSRM.
## Wipe runtime state (cursors + Redis) au démarrage ; garde le modèle .joblib.
single-driver-up:
	TLC_SCENARIO=single_driver \
	TLC_RESET_RUNTIME_ON_START=true \
	ROUTING_PROVIDERS=$${ROUTING_PROVIDERS:-osrm} \
	TLC_TRAIN_MONTH_COUNT=$${TLC_TRAIN_MONTH_COUNT:-10} \
	TLC_LIVE_MONTH_COUNT=$${TLC_LIVE_MONTH_COUNT:-2} \
	docker compose --profile routing up --build -d osrm tlc-replay
	@echo ""
	@echo "✓ Single-driver scenario started (driver_id=$${TLC_SINGLE_DRIVER_ID:-drv_demo_001})"
	@echo "  Focus map → http://localhost:8001/map?focus=$${TLC_SINGLE_DRIVER_ID:-drv_demo_001}"

## Reset explicite du runtime (cursors replay + Redis fleet keys), modèle intact.
## Portable: pas de HEREDOC (Windows git-bash et make standards ok).
single-driver-reset:
	docker exec fleetstream-redis redis-cli DEL \
	    copilot:replay:tlc:status \
	    copilot:replay:tlc:cursor \
	    copilot:replay:tlc:single:status \
	    copilot:replay:tlc:single:cursor \
	    fleet:geo
	docker compose restart tlc-replay
	@echo "✓ Runtime state reset (cursors + fleet:geo) — model pickle untouched"

## Stop + logs helpers pour le scénario single-driver.
single-driver-down:
	docker compose stop tlc-replay osrm

single-driver-logs:
	docker compose logs -f tlc-replay

focus-map:
	@echo "→ Open http://localhost:8001/map?focus=$${TLC_SINGLE_DRIVER_ID:-drv_demo_001}"

## Demo rapide des endpoints copilot
demo-copilot:
	@echo "→ Score d'offre (fallback si modèle absent):"
	curl -s -X POST "http://localhost:8001/copilot/score-offer" -H "Content-Type: application/json" -d "{\"estimated_fare_eur\":12.4,\"estimated_distance_km\":3.2,\"estimated_duration_min\":18,\"demand_index\":1.3,\"supply_index\":0.8,\"weather_factor\":1.0,\"traffic_factor\":1.1}" | python3 -m json.tool
	@echo ""
	@echo "→ Zones recommandées:"
	curl -s "http://localhost:8001/copilot/driver/L001/next-best-zone" | python3 -m json.tool

## Demo rank-offers: classe 3 offres simultanées par €/h net.
## C'est le cas d'usage typique pour un livreur qui voit plusieurs courses
## en même temps et doit choisir la plus rentable pour son temps.
demo-rank:
	@echo "→ Classement de 3 offres simultanées par €/h net:"
	curl -s -X POST "http://localhost:8001/copilot/rank-offers" \
	    -H "Content-Type: application/json" \
	    -d '{"offers":[{"offer_id":"A","estimated_fare_eur":8.0,"estimated_distance_km":2.0,"estimated_duration_min":12,"demand_index":1.2,"supply_index":0.9,"weather_factor":1.0,"traffic_factor":1.0},{"offer_id":"B","estimated_fare_eur":18.0,"estimated_distance_km":6.0,"estimated_duration_min":25,"demand_index":1.4,"supply_index":0.8,"weather_factor":1.0,"traffic_factor":1.0},{"offer_id":"C","estimated_fare_eur":14.0,"estimated_distance_km":3.5,"estimated_duration_min":18,"demand_index":1.3,"supply_index":1.0,"weather_factor":1.0,"traffic_factor":1.1}],"rank_by":"eur_per_hour_net","reject_below_eur_h":10}' \
	    | python3 -m json.tool

## Demo next-best-zone en mode homeward : le livreur est à Times Square et
## veut rentrer à Brooklyn. distance_weight=0.5 pénalise les zones loin, et le
## hint home_lat/home_lon privilégie les zones sur le chemin du retour.
demo-next-zone:
	@echo "→ Zones recommandées (mode homeward, distance_weight=0.5):"
	curl -s "http://localhost:8001/copilot/driver/L001/next-best-zone?lat=40.7580&lon=-73.9855&home_lat=40.6782&home_lon=-73.9442&distance_weight=0.5&max_distance_km=15&top_k=5" | python3 -m json.tool

bench-copilot:
	python3 scripts/benchmark-copilot.py --url http://localhost:8001 --requests 300 --concurrency 40

smoke-e2e:
	python scripts/smoke-e2e.py --url http://localhost:8001

perf-lot4:
	python scripts/perf-lot4.py --url http://localhost:8001 --ingest-window 20 --score-requests 300 --score-concurrency 30

proof-lot4:
	python scripts/smoke-e2e.py --url http://localhost:8001
	python scripts/perf-lot4.py --url http://localhost:8001 --ingest-window 20 --score-requests 300 --score-concurrency 30

## Mode réel : stoppe le replay TLC pour ne garder que les positions devices réelles
real-mode:
	docker compose stop tlc-replay
	@echo "✓ Replay TLC stoppé. Envoie GPS via http://localhost:8010/ingest/v1/position"

## Mode simulation : relance le replay TLC
sim-mode:
	docker compose up -d tlc-replay
	@echo "✓ Replay TLC relancé."

## Test rapide du gateway d'ingestion GPS (sandbox token local)
demo-ingest:
	curl -s -X POST "http://localhost:8010/ingest/v1/position" -H "Authorization: Bearer dev-insecure-token" -H "Content-Type: application/json" -d "{\"courier_id\":\"L001\",\"lat\":40.7580,\"lon\":-73.9855,\"speed_kmh\":12.0,\"heading_deg\":180.0,\"status\":\"delivering\",\"accuracy_m\":8.0,\"battery_pct\":92.0}" | python3 -m json.tool
	@echo ""
	@echo "→ Vérif API live map source (Redis hot path):"
	curl -s "http://localhost:8001/livreurs/L001" | python3 -m json.tool
