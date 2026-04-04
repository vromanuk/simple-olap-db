use std::sync::Arc;

use axum::extract::State;
use axum::Json;
use serde::Serialize;

use olap_engine::query_engine::QueryEngine;

#[derive(Serialize)]
pub struct TablesResponse {
    pub tables: Vec<String>,
}

#[tracing::instrument(skip(engine))]
pub async fn list_tables(State(engine): State<Arc<QueryEngine>>) -> Json<TablesResponse> {
    let tables = engine.list_tables();
    Json(TablesResponse { tables })
}
