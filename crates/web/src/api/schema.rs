use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Serialize;

use olap_engine::query_engine::QueryEngine;

use super::errors::to_http_error;

#[derive(Serialize)]
pub struct SchemaResponse {
    pub table: String,
    pub columns: Vec<ColumnResponse>,
}

#[derive(Serialize)]
pub struct ColumnResponse {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[tracing::instrument(skip(engine))]
pub async fn table_schema(
    State(engine): State<Arc<QueryEngine>>,
    Path(table_name): Path<String>,
) -> Result<Json<SchemaResponse>, (StatusCode, String)> {
    let schema = engine
        .table_schema(&table_name)
        .await
        .map_err(to_http_error)?;

    let columns = schema
        .columns
        .into_iter()
        .map(|c| ColumnResponse {
            name: c.name,
            data_type: c.data_type,
            nullable: c.nullable,
        })
        .collect();

    Ok(Json(SchemaResponse {
        table: schema.table_name,
        columns,
    }))
}
