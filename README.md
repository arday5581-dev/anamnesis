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

---

## Roadmap

Decisions locked in for the remaining phases:
- **Interface**: Axum REST/WebSocket API only, no bundled frontend.
- **LLM**: OpenAI Chat Completions API for the reasoning step.
- **Embeddings**: a local embedding server (no external API key), added as
  a new docker-compose service.
- **Ingest trigger**: scheduled/polling via Redpanda Connect's `generate`
  input hitting the FHIR API on an interval (continuous, not one-shot).

### Phase 1 — Local embedding service + Qdrant collection bootstrap

Add a new service to `docker-compose.yml`: Hugging Face **Text Embeddings
Inference (TEI)** (`ghcr.io/huggingface/text-embeddings-inference`), CPU
image, model `BAAI/bge-small-en-v1.5` (384-dim, fast on CPU, good enough for
a demo). Exposes `POST /embed` over HTTP on a new host port (e.g. `8090`).

Add a one-time bootstrap step (`make init-qdrant` or a `connect/`-adjacent
script) that `curl`s `PUT http://localhost:6333/collections/fhir_docs` with
`vectors: { size: 384, distance: "Cosine" }`, matching TEI's output
dimension. Idempotent — safe to re-run.

Update `Makefile`'s `verify` target to also check the embeddings service
health and confirm the `fhir_docs` collection exists.

**Status: implemented.** Run `make up` then `make init-qdrant`, and
`make verify` checks everything including the new service.

Note: pinned to `cpu-1.7` — `cpu-1.5` fails to download model artifacts
("relative URL without a base"), an upstream bug against current HF Hub
redirects. TEI's CPU image also only ships an amd64 build; on Apple
Silicon, `docker-compose.yml` sets `platform: linux/amd64` for the
`embeddings` service so it runs under Rosetta emulation — drop that line
on an amd64 host.

### Phase 2 — FHIR ingest pipeline (Redpanda Connect)

Replace `connect/healthcheck.yaml`'s placeholder with the real pipeline
(new file, e.g. `connect/ingest.yaml`), still run by the existing `connect`
service in `docker-compose.yml` (swap the `command` path):

- **Input**: `generate` on an interval (e.g. every 60s) to trigger a tick.
- **Processor**: `http` request to the HAPI sandbox
  (`https://hapi.fhir.org/baseR4/Patient?_count=20&...`) plus follow-up
  requests for related resources per patient (`Condition`, `Observation`,
  `MedicationRequest`) — scoped to a fixed small cohort (e.g. by name or a
  known patient ID list) to keep the demo bounded and avoid hammering the
  public sandbox.
- **Processor**: unbundle the FHIR `Bundle.entry[]` array into one message
  per resource (`unarchive`/`mapping` with `root = this.entry.map_each(...)`).
- **Processor**: `mapping` to normalize each resource into a flat document:
  `{ resource_type, resource_id, patient_id, patient_name, text, raw }`,
  where `text` is a short human-readable summary built from the resource
  fields (e.g. "Condition: Type 2 diabetes mellitus, onset 2019, patient
  Jane Smith") — this is what gets embedded.
- **Output**: produce to a new Redpanda topic (e.g. `fhir.docs`) — this is
  the "buffer topic" `docker-compose.yml` already anticipates.

**Status: implemented** (`connect/ingest.yaml`, wired into the `connect`
service's `command`). Deviations from the plan above, found while building
against the live sandbox:

- **One request, not several.** A single `Patient?_id=...&_revinclude=...`
  search returns the patients plus every linked Condition/Observation/
  MedicationRequest in one Bundle — simpler and gentler on the public
  sandbox than a follow-up call per patient per resource type.
- **Cohort is a hardcoded patient ID list** (Carlos Ramirez, Maria
  Williams, James Anderson — IDs in `ingest.yaml`), not a name search.
  Most patients on the public sandbox are empty test stubs; these three
  were found (by sampling `MedicationRequest?status=active&_include=...`)
  to actually have linked Conditions/Observations/MedicationRequests. If
  the sandbox resets and these IDs stop resolving, `make verify`'s
  `fhir.docs` check will start coming up empty — pick fresh IDs the same
  way and swap them into the `_id=` list.
- **Patient-name resolution happens before the split.** Bloblang's
  `unarchive` turns the one Bundle-array message into N independent
  per-resource messages, so the `mapping` processor first builds an
  in-message `patient_id → patient_name` lookup from the Bundle's Patient
  entries and resolves it onto every doc *before* unarchiving — otherwise
  each split-off Condition/Observation/MedicationRequest message would have
  no way to see its sibling Patient resource's name.

### Phase 3 — Embed & index pipeline (Redpanda Connect)

Second pipeline (new file, e.g. `connect/index.yaml`), run as another
`connect` instance/service (or a second stream in the same instance):

- **Input**: `kafka_franz` (or `redpanda`) consuming the `fhir.docs` topic.
- **Processor**: `branch` — call the TEI embedding service's `/embed` HTTP
  endpoint with the document's `text` field, attach the resulting vector.
- **Output**: `qdrant` output component writing to the `fhir_docs`
  collection — vector from the embed step, payload = the normalized
  document fields (`resource_type`, `patient_id`, `patient_name`, `text`,
  `raw`).

  *Verify during implementation* that the `redpandadata/connect:latest`
  image ships a `qdrant` output component with the fields needed
  (vector + arbitrary JSON payload). If it's missing or too limited,
  fall back to a thin Rust consumer (`rdkafka` + `qdrant-client` crates)
  reading `fhir.docs` and upserting directly — same topic/collection
  contract either way, so this is a drop-in swap.

**Status: implemented** (`connect/index.yaml`, run as a second `connect`
service — `index` — in `docker-compose.yml`, its own consumer group so it
tracks progress independently of the ingest side). The `qdrant` output
component does ship with everything needed, but two bugs in this version
(`connect:latest`, benthos 4.102.0) needed workarounds, found by testing
each field in isolation against the live services:

- **`id` mapping runs with no message in scope.** Any reference to `this`
  in the output's `id` field fails with `context was undefined, unable to
  reference` — confirmed the same `this` reference works fine in
  `vector_mapping`/`payload_mapping`, so it's specific to `id`. Workaround:
  a `mapping` processor stashes `meta resource_id = this.resource_id`
  right before the output, and `id` reads it back via `meta("resource_id")`
  instead of `this.resource_id` directly.
- **Nested numeric fields break payload serialization.** `raw`'s nested
  numbers (e.g. `valueQuantity.value`) come through as Go's `json.Number`
  from the original HTTP response parse, and the qdrant client's payload
  struct conversion rejects that type outright (`unable to coerce payload
  output type: invalid type: json.Number`). Round-tripping the payload
  through `.string().parse_json()` re-decodes it as plain float64/int,
  which is accepted.

Verified against the live stack: `make verify` shows a growing
`fhir_docs` point count, and a manual similarity search (embed a question
via TEI, `POST /collections/fhir_docs/points/search`) correctly surfaces
the right patient's Condition/Observation/MedicationRequest docs ranked by
relevance.

### Phase 4 — Chat API (Rust/Axum)

Flesh out `Cargo.toml` and `src/`:

- Dependencies: `axum`, `tokio`, `reqwest`, `serde`/`serde_json`,
  `qdrant-client`, `tracing`/`tracing-subscriber`, `anyhow`, `dotenvy`.
- `src/main.rs`: Axum app with:
  - `GET /healthz`
  - `POST /chat` — body `{ "message": "<question>" }`, response
    `{ "answer": "...", "sources": [{resource_type, resource_id, patient_name}] }`
  - Optional `GET /chat/ws` — WebSocket variant that streams the OpenAI
    response token-by-token, for a nicer chat feel.
- `src/retrieval.rs`: embed the incoming question via the same TEI
  `/embed` endpoint used in ingest (keeps query/document vectors in the
  same space), then `qdrant-client` similarity search (top-K, e.g. 8) on
  `fhir_docs`.
- `src/llm.rs`: build an OpenAI chat completion request — system prompt
  instructing the model to reason only from the provided patient context
  and to say so explicitly when the context is insufficient; user message
  = retrieved doc summaries + the original question. Call via `reqwest`.
- Config via env vars (`.env` + `dotenvy`): `OPENAI_API_KEY`, `QDRANT_URL`,
  `EMBEDDINGS_URL`, `OPENAI_MODEL` (default e.g. `gpt-4o-mini`).

**Status: implemented.** `cargo build`/`cargo clippy` clean. Copy
`.env.example` to `.env` and fill in `OPENAI_API_KEY` (all other vars
already default to the host-exposed ports from `docker-compose.yml`), then
`cargo run`. Notes:

- `/chat/ws`'s wire protocol (not specified by the plan, so decided during
  implementation): client sends one text message per question; server
  replies with a stream of `{"type":"delta","content":"..."}` messages
  followed by one `{"type":"done","sources":[...]}`, or `{"type":"error",
  "message":"..."}` if retrieval or the OpenAI call fails.
- Verified live against the running Phase 1-3 stack (`docker compose up`):
  `GET /healthz` returns `ok`, and both `/chat` and `/chat/ws` correctly
  embed the question, retrieve the right patient's docs from `fhir_docs`,
  and fail gracefully with a clear `OPENAI_API_KEY is not set` error at the
  LLM-call step — this environment has no OpenAI key, so the actual
  completion call is implemented per spec but not live-tested; wire up a
  real key to exercise it end-to-end.
- `qdrant-client` 1.18.0 talking to the qdrant 1.12.4 server logs a
  version-compatibility warning at startup (same skew noted for Redpanda
  Connect's qdrant output in Phase 3) — cosmetic, search/upsert both work.

### Phase 5 — End-to-end verification

- `make up` brings up all services (redpanda, console, connect ×2 (or one
  with two streams), qdrant, embeddings).
- Confirm ingest: `docker compose logs -f connect` shows FHIR fetches and
  Redpanda topic writes; `rpk topic consume fhir.docs -n 5` shows normalized
  docs.
- Confirm indexing: `curl http://localhost:6333/collections/fhir_docs`
  shows a growing point count.
- Confirm chat: `cargo run`, then
  `curl -X POST localhost:3000/chat -d '{"message":"What conditions does Jane Smith have?"}'`
  returns an answer citing retrieved FHIR resources.

### Critical files for the roadmap

- `docker-compose.yml` — add `embeddings` service, wire ingest/index Connect
  configs.
- `connect/ingest.yaml` (new) — FHIR poll → normalize → `fhir.docs` topic.
- `connect/index.yaml` (new) — topic → embed → Qdrant upsert.
- `Cargo.toml` — add axum/reqwest/qdrant-client/etc. dependencies.
- `src/main.rs`, `src/retrieval.rs`, `src/llm.rs` (new) — chat API.
- `Makefile`, `README.md` — bootstrap/verify targets and docs updates.

