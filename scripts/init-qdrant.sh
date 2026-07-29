#!/usr/bin/env bash
# One-time (idempotent) bootstrap: create the `fhir_docs` Qdrant collection
# sized for TEI's BAAI/bge-small-en-v1.5 output (384-dim, cosine distance).
set -euo pipefail

QDRANT_URL="${QDRANT_URL:-http://localhost:6333}"
COLLECTION="fhir_docs"

if curl -sf "${QDRANT_URL}/collections/${COLLECTION}" >/dev/null 2>&1; then
  echo "Collection '${COLLECTION}' already exists, skipping."
else
  echo "Creating collection '${COLLECTION}'..."
  curl -sf -X PUT "${QDRANT_URL}/collections/${COLLECTION}" \
    -H 'Content-Type: application/json' \
    -d '{"vectors": {"size": 384, "distance": "Cosine"}}'
  echo
fi
