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

pub struct QueryResult {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

impl QueryResult {
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }
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

    pub fn list_tables(&self) -> Vec<String> {
        match self {
            QueryEngine::DataFusion(engine) => engine.list_tables(),
        }
    }
}
