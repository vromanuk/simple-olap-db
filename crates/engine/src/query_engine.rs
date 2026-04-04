use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use crate::datafusion_engine::DataFusionEngine;
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

pub enum QueryEngine {
    DataFusion(DataFusionEngine),
}

impl QueryEngine {
    pub async fn execute(&self, query: &SqlStatement) -> EngineResult<QueryResult> {
        match self {
            QueryEngine::DataFusion(engine) => engine.execute(query.sql()).await,
        }
    }

    pub async fn explain(&self, query: &SqlStatement) -> EngineResult<String> {
        match self {
            QueryEngine::DataFusion(engine) => engine.explain(query.sql()).await,
        }
    }

    pub fn list_tables(&self) -> Vec<String> {
        match self {
            QueryEngine::DataFusion(engine) => engine.list_tables(),
        }
    }

    pub async fn table_schema(&self, name: &str) -> EngineResult<TableSchema> {
        match self {
            QueryEngine::DataFusion(engine) => engine.table_schema(name).await,
        }
    }

    pub async fn table_stats(&self, name: &str) -> EngineResult<TableStats> {
        match self {
            QueryEngine::DataFusion(engine) => engine.table_stats(name).await,
        }
    }

    pub async fn register_table(
        &self,
        name: &str,
        path: &str,
        format: &TableFormat,
    ) -> EngineResult<()> {
        match self {
            QueryEngine::DataFusion(engine) => match format {
                TableFormat::Delta => engine.register_delta_table(name, path).await,
                TableFormat::Parquet => engine.register_parquet_file(name, path).await,
            },
        }
    }
}
