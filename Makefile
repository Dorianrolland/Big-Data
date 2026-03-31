.PHONY: up down logs build clean restart status

## Lance l'intégralité du stack (build + démarrage)
up:
	docker compose up --build -d
	@echo ""
	@echo "✓ FleetStream démarré !"
	@echo "  API docs    → http://localhost:8001/docs"
	@echo "  Redpanda UI → http://localhost:8080"
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

## Test de l'API (requiert curl + jq)
demo:
	@echo "→ Livreurs proches de Notre-Dame (rayon 2km):"
	curl -s "http://localhost:8001/livreurs-proches?lat=48.8530&lon=2.3499&rayon=2" | python3 -m json.tool
	@echo ""
	@echo "→ Stats temps réel:"
	curl -s "http://localhost:8001/stats" | python3 -m json.tool
