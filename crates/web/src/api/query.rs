use std::sync::Arc;

use arrow::json::LineDelimitedWriter;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};

use olap_engine::query_engine::{EngineError, QueryEngine, QueryResult};
use olap_engine::sql_statement::SqlStatement;

use super::errors::to_http_error;

#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub sql: String,
}

#[derive(Serialize)]
pub struct QueryResponse {
    pub columns: Vec<String>,
    pub rows: Vec<serde_json::Value>,
}

#[tracing::instrument(skip(engine))]
pub async fn query_handler(
    State(engine): State<Arc<QueryEngine>>,
    Json(request): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, (StatusCode, String)> {
    let statement = SqlStatement::try_new(request.sql).map_err(to_http_error)?;

    let result = engine.execute(&statement).await.map_err(to_http_error)?;

    let response = build_response(result).map_err(to_http_error)?;

    Ok(Json(response))
}

fn build_response(result: QueryResult) -> Result<QueryResponse, EngineError> {
    let columns = result
        .schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();

    let rows = batches_to_json_rows(&result)?;

    Ok(QueryResponse { columns, rows })
}

fn batches_to_json_rows(result: &QueryResult) -> Result<Vec<serde_json::Value>, EngineError> {
    let mut buf = Vec::new();
    {
        let mut writer = LineDelimitedWriter::new(&mut buf);
        for batch in &result.batches {
            writer
                .write(batch)
                .map_err(|e| EngineError::Table(format!("failed to serialize results: {e}")))?;
        }
        writer
            .finish()
            .map_err(|e| EngineError::Table(format!("failed to finalize results: {e}")))?;
    }

    String::from_utf8(buf)
        .map_err(|e| EngineError::Table(format!("invalid UTF-8 in results: {e}")))?
        .lines()
        .filter(|line| !line.is_empty())
        .map(serde_json::from_str)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| EngineError::Table(format!("failed to parse JSON: {e}")))
}
