mod llm;
mod retrieval;

use std::sync::Arc;

use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use qdrant_client::Qdrant;
use serde::{Deserialize, Serialize};
use serde_json::json;

use retrieval::RetrievedDoc;

pub struct AppState {
    http: reqwest::Client,
    qdrant: Qdrant,
    embeddings_url: String,
    openai_api_key: String,
    openai_model: String,
}

#[derive(Deserialize)]
struct ChatRequest {
    message: String,
}

#[derive(Serialize)]
struct Source {
    resource_type: String,
    resource_id: String,
    patient_name: String,
}

impl From<&RetrievedDoc> for Source {
    fn from(doc: &RetrievedDoc) -> Self {
        Self {
            resource_type: doc.resource_type.clone(),
            resource_id: doc.resource_id.clone(),
            patient_name: doc.patient_name.clone(),
        }
    }
}

#[derive(Serialize)]
struct ChatResponse {
    answer: String,
    sources: Vec<Source>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(
            |_| tracing_subscriber::EnvFilter::new("anamnesis=info,tower_http=info"),
        ))
        .init();

    let qdrant_url =
        std::env::var("QDRANT_URL").unwrap_or_else(|_| "http://localhost:6334".to_string());
    let embeddings_url =
        std::env::var("EMBEDDINGS_URL").unwrap_or_else(|_| "http://localhost:8090".to_string());
    let openai_api_key = std::env::var("OPENAI_API_KEY").unwrap_or_default();
    let openai_model =
        std::env::var("OPENAI_MODEL").unwrap_or_else(|_| "gpt-4o-mini".to_string());
    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".to_string());

    if openai_api_key.is_empty() {
        tracing::warn!(
            "OPENAI_API_KEY is not set — /chat and /chat/ws will return an error until it is"
        );
    }

    let qdrant = Qdrant::from_url(&qdrant_url).build()?;
    let state = Arc::new(AppState {
        http: reqwest::Client::new(),
        qdrant,
        embeddings_url,
        openai_api_key,
        openai_model,
    });

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/chat", post(chat))
        .route("/chat/ws", get(chat_ws))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    tracing::info!("listening on {}", listener.local_addr()?);
    axum::serve(listener, app).await?;

    Ok(())
}

async fn healthz() -> &'static str {
    "ok"
}

async fn chat(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ChatRequest>,
) -> Result<Json<ChatResponse>, (StatusCode, String)> {
    let docs = retrieval::retrieve(&state, &req.message)
        .await
        .map_err(internal_error)?;

    let answer = llm::answer(&state, &req.message, &docs)
        .await
        .map_err(internal_error)?;

    let sources = docs.iter().map(Source::from).collect();

    Ok(Json(ChatResponse { answer, sources }))
}

fn internal_error(err: anyhow::Error) -> (StatusCode, String) {
    tracing::error!("{err:#}");
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}

async fn chat_ws(State(state): State<Arc<AppState>>, ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws(socket, state))
}

/// Wire protocol: client sends one text message per question; server
/// replies with a stream of `{"type":"delta","content":"..."}` messages
/// followed by one `{"type":"done","sources":[...]}` (or `{"type":"error",...}`
/// if retrieval or the OpenAI call fails).
async fn handle_ws(mut socket: WebSocket, state: Arc<AppState>) {
    while let Some(Ok(msg)) = socket.recv().await {
        let Message::Text(text) = msg else {
            continue;
        };

        let docs = match retrieval::retrieve(&state, &text).await {
            Ok(docs) => docs,
            Err(err) => {
                send_error(&mut socket, err).await;
                continue;
            }
        };

        let mut rx = match llm::stream_answer(&state, &text, &docs).await {
            Ok(rx) => rx,
            Err(err) => {
                send_error(&mut socket, err).await;
                continue;
            }
        };

        while let Some(delta) = rx.recv().await {
            if send_json(&mut socket, &json!({"type": "delta", "content": delta}))
                .await
                .is_err()
            {
                return;
            }
        }

        let sources: Vec<Source> = docs.iter().map(Source::from).collect();
        if send_json(&mut socket, &json!({"type": "done", "sources": sources}))
            .await
            .is_err()
        {
            return;
        }
    }
}

async fn send_json(socket: &mut WebSocket, value: &serde_json::Value) -> Result<(), axum::Error> {
    socket.send(Message::Text(value.to_string().into())).await
}

async fn send_error(socket: &mut WebSocket, err: anyhow::Error) {
    tracing::error!("{err:#}");
    let _ = send_json(&mut *socket, &json!({"type": "error", "message": err.to_string()})).await;
}
