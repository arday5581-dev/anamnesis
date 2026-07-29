use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::mpsc;

use crate::AppState;
use crate::retrieval::RetrievedDoc;

const OPENAI_URL: &str = "https://api.openai.com/v1/chat/completions";

const SYSTEM_PROMPT: &str = "You are a clinical-reasoning assistant. Answer only using the \
patient context provided below — do not draw on outside medical knowledge to fill in specifics \
about this patient. If the context doesn't contain enough information to answer, say so \
explicitly instead of guessing.";

fn build_messages(question: &str, docs: &[RetrievedDoc]) -> serde_json::Value {
    let context = if docs.is_empty() {
        "(no matching patient records found)".to_string()
    } else {
        docs.iter()
            .map(|d| format!("- {}", d.text))
            .collect::<Vec<_>>()
            .join("\n")
    };

    json!([
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": format!("Patient context:\n{context}\n\nQuestion: {question}")},
    ])
}

/// Single-shot chat completion for `POST /chat`.
pub async fn answer(state: &AppState, question: &str, docs: &[RetrievedDoc]) -> Result<String> {
    if state.openai_api_key.is_empty() {
        bail!("OPENAI_API_KEY is not set");
    }

    #[derive(Deserialize)]
    struct Response {
        choices: Vec<Choice>,
    }
    #[derive(Deserialize)]
    struct Choice {
        message: ResponseMessage,
    }
    #[derive(Deserialize)]
    struct ResponseMessage {
        content: String,
    }

    let body = json!({
        "model": state.openai_model,
        "messages": build_messages(question, docs),
    });

    let response: Response = state
        .http
        .post(OPENAI_URL)
        .bearer_auth(&state.openai_api_key)
        .json(&body)
        .send()
        .await
        .context("OpenAI request failed")?
        .error_for_status()
        .context("OpenAI returned an error status")?
        .json()
        .await
        .context("failed to parse OpenAI response")?;

    response
        .choices
        .into_iter()
        .next()
        .map(|c| c.message.content)
        .context("OpenAI response contained no choices")
}

/// Streaming chat completion for `GET /chat/ws`: kicks off the OpenAI
/// request, then relays token deltas onto the returned channel from a
/// background task as they arrive over server-sent events.
pub async fn stream_answer(
    state: &AppState,
    question: &str,
    docs: &[RetrievedDoc],
) -> Result<mpsc::UnboundedReceiver<String>> {
    if state.openai_api_key.is_empty() {
        bail!("OPENAI_API_KEY is not set");
    }

    let body = json!({
        "model": state.openai_model,
        "messages": build_messages(question, docs),
        "stream": true,
    });

    let response = state
        .http
        .post(OPENAI_URL)
        .bearer_auth(&state.openai_api_key)
        .json(&body)
        .send()
        .await
        .context("OpenAI request failed")?
        .error_for_status()
        .context("OpenAI returned an error status")?;

    let (tx, rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        let mut stream = response.bytes_stream();
        let mut buf = String::new();

        while let Some(chunk) = stream.next().await {
            let Ok(chunk) = chunk else { return };
            buf.push_str(&String::from_utf8_lossy(&chunk));

            while let Some(pos) = buf.find("\n\n") {
                let event: String = buf.drain(..pos + 2).collect();
                let Some(data) = event.trim_end().strip_prefix("data: ") else {
                    continue;
                };
                if data == "[DONE]" {
                    return;
                }

                let Ok(parsed) = serde_json::from_str::<serde_json::Value>(data) else {
                    continue;
                };
                if let Some(delta) = parsed["choices"][0]["delta"]["content"].as_str()
                    && tx.send(delta.to_string()).is_err()
                {
                    return;
                }
            }
        }
    });

    Ok(rx)
}
