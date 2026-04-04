use axum::extract::Path;
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};

use olap_engine::compactor;

use super::errors::to_http_error;

#[derive(Serialize)]
pub struct OptimizeResponse {
    pub table: String,
    pub files_before: usize,
    pub files_after: usize,
    pub version: i64,
}

#[derive(Serialize)]
pub struct VacuumResponse {
    pub table: String,
    pub files_deleted: usize,
    pub files_remaining: usize,
}

#[derive(Debug, Deserialize)]
pub struct VacuumRequest {
    pub retention_hours: Option<u64>,
}

#[tracing::instrument]
pub async fn optimize_handler(
    Path(table_path): Path<String>,
) -> Result<Json<OptimizeResponse>, (StatusCode, String)> {
    let result = compactor::optimize_table(&table_path)
        .await
        .map_err(to_http_error)?;

    Ok(Json(OptimizeResponse {
        table: table_path,
        files_before: result.files_before,
        files_after: result.files_after,
        version: result.version,
    }))
}

#[tracing::instrument]
pub async fn vacuum_handler(
    Path(table_path): Path<String>,
    Json(request): Json<VacuumRequest>,
) -> Result<Json<VacuumResponse>, (StatusCode, String)> {
    let result = compactor::vacuum_table(&table_path, request.retention_hours)
        .await
        .map_err(to_http_error)?;

    Ok(Json(VacuumResponse {
        table: table_path,
        files_deleted: result.files_deleted,
        files_remaining: result.files_remaining,
    }))
}
