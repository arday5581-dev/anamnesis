.PHONY: up down logs verify init-qdrant clean

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

# Phase 1: one-time (idempotent) creation of the `fhir_docs` Qdrant collection.
init-qdrant:
	@./scripts/init-qdrant.sh

# Phase 0+1+2+3 smoke test: confirm all services are reachable, the
# `fhir_docs` collection exists, the ingest pipeline is producing docs, and
# the index pipeline is embedding+upserting them into Qdrant.
verify:
	@echo "== Redpanda cluster health =="
	@docker compose exec -T redpanda rpk cluster health
	@echo "\n== Redpanda Connect (ingest) health (expect: pong) =="
	@curl -sf http://localhost:4195/ping && echo " -> pong"
	@echo "\n== Redpanda Connect (index) health (expect: pong) =="
	@curl -sf http://localhost:4196/ping && echo " -> pong"
	@echo "\n== Qdrant health =="
	@curl -sf http://localhost:6333/healthz && echo ""
	@echo "\n== Embeddings (TEI) health =="
	@curl -sf http://localhost:8090/health && echo "ok"
	@echo "\n== Qdrant collections (expect fhir_docs) =="
	@curl -sf http://localhost:6333/collections | python3 -m json.tool
	@echo "\n== fhir.docs topic (expect normalized FHIR resource docs) =="
	@docker compose exec -T redpanda rpk topic consume fhir.docs -n 3
	@echo "\n== fhir_docs point count (expect > 0, grows as ingest ticks) =="
	@curl -sf http://localhost:6333/collections/fhir_docs | python3 -c "import json,sys; print(json.load(sys.stdin)['result']['points_count'])"
	@echo "\nAll checks passed."

clean:
	docker compose down -v
