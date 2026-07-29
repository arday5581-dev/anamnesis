use anyhow::{Context, Result};
use qdrant_client::Payload;
use qdrant_client::qdrant::QueryPointsBuilder;
use serde::{Deserialize, Serialize};

use crate::AppState;

const COLLECTION: &str = "fhir_docs";
const TOP_K: u64 = 8;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RetrievedDoc {
    pub resource_type: String,
    pub resource_id: String,
    pub patient_id: String,
    pub patient_name: String,
    pub text: String,
}

/// Embeds `query` via TEI (the same model used at ingest time, so query and
/// document vectors live in the same space) and returns the top-K nearest
/// `fhir_docs` points.
pub async fn retrieve(state: &AppState, query: &str) -> Result<Vec<RetrievedDoc>> {
    let vector = embed(state, query).await?;

    let response = state
        .qdrant
        .query(
            QueryPointsBuilder::new(COLLECTION)
                .query(vector)
                .limit(TOP_K)
                .with_payload(true),
        )
        .await
        .context("qdrant query failed")?;

    response
        .result
        .into_iter()
        .map(|point| {
            Payload::from(point.payload)
                .deserialize::<RetrievedDoc>()
                .context("failed to deserialize qdrant point payload")
        })
        .collect()
}

async fn embed(state: &AppState, text: &str) -> Result<Vec<f32>> {
    #[derive(Serialize)]
    struct EmbedRequest<'a> {
        inputs: &'a str,
    }

    let url = format!("{}/embed", state.embeddings_url);
    let mut vectors: Vec<Vec<f32>> = state
        .http
        .post(url)
        .json(&EmbedRequest { inputs: text })
        .send()
        .await
        .context("embeddings request failed")?
        .error_for_status()
        .context("embeddings request returned an error status")?
        .json()
        .await
        .context("failed to parse embeddings response")?;

    vectors
        .pop()
        .context("embeddings response contained no vectors")
}
