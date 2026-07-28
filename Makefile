.PHONY: up down logs verify clean

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

# Phase 0 smoke test: confirm all four services are reachable.
verify:
	@echo "== Redpanda cluster health =="
	@docker compose exec -T redpanda rpk cluster health
	@echo "\n== Redpanda Connect health (expect: pong) =="
	@curl -sf http://localhost:4195/ping && echo " -> pong"
	@echo "\n== Qdrant health =="
	@curl -sf http://localhost:6333/healthz && echo ""
	@echo "\n== Qdrant collections (expect empty list) =="
	@curl -sf http://localhost:6333/collections | python3 -m json.tool
	@echo "\nAll checks passed."

clean:
	docker compose down -v
