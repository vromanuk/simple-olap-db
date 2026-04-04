use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};

use olap_engine::query_engine::QueryEngine;
use olap_engine::sql_statement::SqlStatement;

use super::errors::to_http_error;

#[derive(Debug, Deserialize)]
pub struct ExplainRequest {
    pub sql: String,
}

#[derive(Serialize)]
pub struct ExplainResponse {
    pub plan: String,
}

#[tracing::instrument(skip(engine))]
pub async fn explain_handler(
    State(engine): State<Arc<QueryEngine>>,
    Json(request): Json<ExplainRequest>,
) -> Result<Json<ExplainResponse>, (StatusCode, String)> {
    let statement = SqlStatement::try_new(request.sql).map_err(to_http_error)?;
    let plan = engine.explain(&statement).await.map_err(to_http_error)?;

    Ok(Json(ExplainResponse { plan }))
}
