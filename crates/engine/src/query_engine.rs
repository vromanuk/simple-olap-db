use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use crate::datafusion_engine::DataFusionEngine;
use crate::duckdb_engine::DuckDBEngine;
use crate::sql_statement::SqlStatement;

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("invalid SQL: {0}")]
    InvalidSql(String),

    #[error("execution failed: {0}")]
    Execution(#[from] datafusion::error::DataFusionError),

    #[error("delta table error: {0}")]
    DeltaTable(#[from] deltalake::DeltaTableError),

    #[error("table error: {0}")]
    Table(String),
}

pub type EngineResult<T> = Result<T, EngineError>;

#[derive(Debug)]
pub struct QueryResult {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

impl QueryResult {
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }
}

#[derive(Debug)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Debug)]
pub struct TableStats {
    pub table_name: String,
    pub num_columns: usize,
    pub num_rows: Option<usize>,
}

#[derive(Debug)]
pub struct TableSchema {
    pub table_name: String,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TableFormat {
    Delta,
    Parquet,
}

/// Query engine enum — DataFusion methods are async-native, DuckDB methods are
/// synchronous and run via `spawn_blocking` to avoid blocking the tokio runtime.
pub enum QueryEngine {
    DataFusion(DataFusionEngine),
    DuckDB(DuckDBEngine),
}

impl QueryEngine {
    pub async fn execute(self: &Arc<Self>, query: &SqlStatement) -> EngineResult<QueryResult> {
        match self.as_ref() {
            QueryEngine::DataFusion(engine) => engine.execute(query.sql()).await,
            QueryEngine::DuckDB(_) => {
                let engine = Arc::clone(self);
                let sql = query.sql().to_string();
                tokio::task::spawn_blocking(move || match engine.as_ref() {
                    QueryEngine::DuckDB(e) => e.execute(&sql),
                    _ => unreachable!(),
                })
                .await
                .map_err(|e| EngineError::Table(format!("blocking task failed: {e}")))?
            }
        }
    }

    pub async fn explain(self: &Arc<Self>, query: &SqlStatement) -> EngineResult<String> {
        match self.as_ref() {
            QueryEngine::DataFusion(engine) => engine.explain(query.sql()).await,
            QueryEngine::DuckDB(_) => Ok("EXPLAIN not supported for DuckDB engine".to_string()),
        }
    }

    pub fn list_tables(&self) -> Vec<String> {
        match self {
            QueryEngine::DataFusion(engine) => engine.list_tables(),
            QueryEngine::DuckDB(engine) => engine.list_tables(),
        }
    }

    pub async fn table_schema(self: &Arc<Self>, name: &str) -> EngineResult<TableSchema> {
        match self.as_ref() {
            QueryEngine::DataFusion(engine) => engine.table_schema(name).await,
            QueryEngine::DuckDB(_) => {
                let engine = Arc::clone(self);
                let name = name.to_string();
                tokio::task::spawn_blocking(move || match engine.as_ref() {
                    QueryEngine::DuckDB(e) => e.table_schema(&name),
                    _ => unreachable!(),
                })
                .await
                .map_err(|e| EngineError::Table(format!("blocking task failed: {e}")))?
            }
        }
    }

    pub async fn table_stats(self: &Arc<Self>, name: &str) -> EngineResult<TableStats> {
        match self.as_ref() {
            QueryEngine::DataFusion(engine) => engine.table_stats(name).await,
            QueryEngine::DuckDB(_) => {
                let engine = Arc::clone(self);
                let name = name.to_string();
                tokio::task::spawn_blocking(move || match engine.as_ref() {
                    QueryEngine::DuckDB(e) => e.table_stats(&name),
                    _ => unreachable!(),
                })
                .await
                .map_err(|e| EngineError::Table(format!("blocking task failed: {e}")))?
            }
        }
    }

    pub async fn register_table(
        self: &Arc<Self>,
        name: &str,
        path: &str,
        format: &TableFormat,
    ) -> EngineResult<()> {
        match self.as_ref() {
            QueryEngine::DataFusion(engine) => match format {
                TableFormat::Delta => engine.register_delta_table(name, path).await,
                TableFormat::Parquet => engine.register_parquet_file(name, path).await,
            },
            QueryEngine::DuckDB(_) => match format {
                TableFormat::Parquet => {
                    let engine = Arc::clone(self);
                    let name = name.to_string();
                    let path = path.to_string();
                    tokio::task::spawn_blocking(move || match engine.as_ref() {
                        QueryEngine::DuckDB(e) => e.register_parquet_file(&name, &path),
                        _ => unreachable!(),
                    })
                    .await
                    .map_err(|e| EngineError::Table(format!("blocking task failed: {e}")))?
                }
                TableFormat::Delta => Err(EngineError::Table(
                    "DuckDB engine does not support Delta format".to_string(),
                )),
            },
        }
    }
}
