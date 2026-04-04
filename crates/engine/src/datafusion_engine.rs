use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion::prelude::*;

use crate::query_engine::{EngineResult, QueryResult};

pub struct DataFusionEngine {
    ctx: SessionContext,
}

impl Default for DataFusionEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl DataFusionEngine {
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
        }
    }

    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }

    pub async fn register_delta_table(&self, name: &str, path: &str) -> EngineResult<()> {
        let table = deltalake::open_table(path).await?;
        self.ctx.register_table(name, Arc::new(table))?;

        Ok(())
    }

    pub async fn register_parquet_file(&self, name: &str, path: &str) -> EngineResult<()> {
        self.ctx
            .register_parquet(name, path, ParquetReadOptions::default())
            .await?;

        Ok(())
    }

    pub fn register_batch(&self, name: &str, batch: RecordBatch) -> EngineResult<()> {
        self.ctx.register_batch(name, batch)?;

        Ok(())
    }

    #[tracing::instrument(skip(self), fields(rows))]
    pub async fn execute(&self, sql: &str) -> EngineResult<QueryResult> {
        let df = self.ctx.sql(sql).await?;
        let schema = df.schema().inner().clone();
        let batches = df.collect().await?;
        let result = QueryResult { schema, batches };
        tracing::Span::current().record("rows", result.num_rows());
        Ok(result)
    }

    #[tracing::instrument(skip(self))]
    pub fn list_tables(&self) -> Vec<String> {
        let catalog = self.ctx.catalog("datafusion").unwrap();
        let schema = catalog.schema("public").unwrap();

        schema.table_names()
    }
}
