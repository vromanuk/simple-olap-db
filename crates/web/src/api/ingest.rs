use std::sync::Arc;

use arrow::json::ReaderBuilder;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};

use olap_engine::ingest::IngestSender;

use super::errors::to_http_error;

#[derive(Debug, Deserialize)]
pub struct IngestRequest {
    pub events: Vec<serde_json::Value>,
}

#[derive(Serialize)]
pub struct IngestResponse {
    pub rows_accepted: usize,
    pub pending_total: usize,
}

#[tracing::instrument(skip(sender, request), fields(event_count))]
pub async fn ingest_handler(
    State(sender): State<Arc<IngestSender>>,
    Json(request): Json<IngestRequest>,
) -> Result<Json<IngestResponse>, (StatusCode, String)> {
    let event_count = request.events.len();
    tracing::Span::current().record("event_count", event_count);

    if request.events.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "no events provided".to_string()));
    }

    let json_lines: String = request
        .events
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid JSON: {e}")))?
        .join("\n");
    let json_bytes = json_lines.into_bytes();

    let reader = ReaderBuilder::new(sender.schema())
        .build(std::io::Cursor::new(json_bytes))
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("failed to parse events: {e}"),
            )
        })?;

    for batch_result in reader {
        let batch = batch_result.map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("failed to decode events: {e}"),
            )
        })?;
        sender.send(batch).await.map_err(to_http_error)?;
    }

    Ok(Json(IngestResponse {
        rows_accepted: event_count,
        pending_total: sender.pending_row_count(),
    }))
}
