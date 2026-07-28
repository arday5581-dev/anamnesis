# anamnesis

A FHIR → RAG → chat pipeline. Redpanda Connect ingests and embeds FHIR
resources into Qdrant; a Rust/Axum app answers clinical-reasoning questions
grounded in the retrieved patient context.

> **Data note:** use synthetic/test data only (e.g. the public HAPI FHIR
> sandbox). No real PHI on this stack.

> The name `anamnesis` (the patient-history taking step in medicine) is a
> placeholder — rename the compose `name:` and directory if you prefer.

---

## Phase 0 — Environment

Brings up the four core services and confirms they're reachable.

| Service          | Host port(s)        | Purpose                          |
|------------------|---------------------|----------------------------------|
| Redpanda         | 19092 (Kafka)       | buffer topic between ingest & DB |
| Redpanda Console | 8080                | web UI for topics/messages       |
| Redpanda Connect | 4195                | ingest pipeline (health only)    |
| Qdrant           | 6333 REST / 6334 gRPC | vector store                   |

### Run

```bash
docker compose up -d      # or: make up
```

First start pulls images and can take a minute or two. Watch progress with
`docker compose ps` — wait for `redpanda` to become `healthy`.

### Verify

```bash
make verify
```

Or manually:

```bash
# Redpanda broker
docker compose exec redpanda rpk cluster health         # Healthy: true

# Redpanda Connect
curl -s http://localhost:4195/ping                      # pong

# Qdrant
curl -s http://localhost:6333/healthz                   # healthz check passed
curl -s http://localhost:6333/collections | jq          # {"result":{"collections":[]},...}
```

Web UIs:
- Redpanda Console → http://localhost:8080
- Qdrant dashboard → http://localhost:6333/dashboard

### Tear down

```bash
docker compose down       # keep volumes
make clean                # docker compose down -v (wipe data)
```

