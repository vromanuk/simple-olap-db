use std::any::Any;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;

#[derive(Debug)]
pub struct CustomTable {
    schema: SchemaRef,
    batches: Vec<Vec<RecordBatch>>,
}

impl CustomTable {
    pub fn new(batches: Vec<RecordBatch>) -> Result<Self> {
        let schema = batches
            .first()
            .ok_or_else(|| datafusion::error::DataFusionError::Plan("no batches provided".into()))?
            .schema();

        Ok(Self {
            schema,
            batches: vec![batches],
        })
    }
}

#[async_trait::async_trait]
impl TableProvider for CustomTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let source =
            MemorySourceConfig::try_new(&self.batches, self.schema(), projection.cloned())?;
        Ok(Arc::new(DataSourceExec::new(Arc::new(source))))
    }
}
