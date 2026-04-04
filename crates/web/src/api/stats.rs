use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Serialize;

use olap_engine::query_engine::QueryEngine;

use super::errors::to_http_error;

#[derive(Serialize)]
pub struct StatsResponse {
    pub table: String,
    pub num_columns: usize,
    pub num_rows: Option<usize>,
}

#[tracing::instrument(skip(engine))]
pub async fn table_stats(
    State(engine): State<Arc<QueryEngine>>,
    Path(table_name): Path<String>,
) -> Result<Json<StatsResponse>, (StatusCode, String)> {
    let stats = engine
        .table_stats(&table_name)
        .await
        .map_err(to_http_error)?;

    Ok(Json(StatsResponse {
        table: stats.table_name,
        num_columns: stats.num_columns,
        num_rows: stats.num_rows,
    }))
}
