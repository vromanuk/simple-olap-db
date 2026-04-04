use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};

use olap_engine::query_engine::{QueryEngine, TableFormat};

use super::errors::to_http_error;

#[derive(Debug, Deserialize)]
pub struct RegisterRequest {
    pub name: String,
    pub path: String,
    pub format: TableFormat,
}

#[derive(Serialize)]
pub struct RegisterResponse {
    pub table: String,
    pub status: String,
}

#[tracing::instrument(skip(engine))]
pub async fn register_table(
    State(engine): State<Arc<QueryEngine>>,
    Json(request): Json<RegisterRequest>,
) -> Result<Json<RegisterResponse>, (StatusCode, String)> {
    engine
        .register_table(&request.name, &request.path, &request.format)
        .await
        .map_err(to_http_error)?;

    Ok(Json(RegisterResponse {
        table: request.name,
        status: "registered".to_string(),
    }))
}
