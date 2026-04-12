.PHONY: up down logs build clean restart status train-copilot demo-copilot bench-copilot smoke-e2e perf-lot4 proof-lot4

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

## Logs d'un service spécifique : make logs-api, make logs-producer, etc.
logs-%:
	docker compose logs -f $*

## Rebuild + restart un service : make restart-producer
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

## Demo rapide des endpoints copilot
demo-copilot:
	@echo "→ Score d'offre (fallback si modèle absent):"
	curl -s -X POST "http://localhost:8001/copilot/score-offer" -H "Content-Type: application/json" -d "{\"estimated_fare_eur\":12.4,\"estimated_distance_km\":3.2,\"estimated_duration_min\":18,\"demand_index\":1.3,\"supply_index\":0.8,\"weather_factor\":1.0,\"traffic_factor\":1.1}" | python3 -m json.tool
	@echo ""
	@echo "→ Zones recommandées:"
	curl -s "http://localhost:8001/copilot/driver/L001/next-best-zone" | python3 -m json.tool

bench-copilot:
	python3 scripts/benchmark-copilot.py --url http://localhost:8001 --requests 300 --concurrency 40

smoke-e2e:
	python scripts/smoke-e2e.py --url http://localhost:8001

perf-lot4:
	python scripts/perf-lot4.py --url http://localhost:8001 --ingest-window 20 --score-requests 300 --score-concurrency 30

proof-lot4:
	python scripts/smoke-e2e.py --url http://localhost:8001
	python scripts/perf-lot4.py --url http://localhost:8001 --ingest-window 20 --score-requests 300 --score-concurrency 30
