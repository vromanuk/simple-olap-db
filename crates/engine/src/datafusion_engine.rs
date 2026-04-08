use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use datafusion::prelude::*;

use arrow::array::AsArray;
use arrow::datatypes::Int64Type;

use crate::query_engine::{
    ColumnInfo, EngineError, EngineResult, QueryResult, TableSchema, TableStats,
};

pub struct DataFusionEngine {
    ctx: SessionContext,
    delta_tables: Mutex<HashMap<String, String>>,
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
            delta_tables: Mutex::new(HashMap::new()),
        }
    }

    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }

    pub async fn register_delta_table(&self, name: &str, path: &str) -> EngineResult<()> {
        let table = deltalake::open_table(path).await?;
        self.ctx.register_table(name, Arc::new(table))?;

        self.delta_tables
            .lock()
            .unwrap()
            .insert(name.to_string(), path.to_string());

        Ok(())
    }

    /// Re-registers all Delta tables to pick up new files written since last query.
    async fn refresh_delta_tables(&self) -> EngineResult<()> {
        let tables: Vec<(String, String)> = self
            .delta_tables
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        for (name, path) in tables {
            let table = deltalake::open_table(&path).await?;
            self.ctx
                .deregister_table(&name)
                .map_err(|e| EngineError::Table(e.to_string()))?;
            self.ctx.register_table(&name, Arc::new(table))?;
        }

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
        self.refresh_delta_tables().await?;
        let df = self.ctx.sql(sql).await?;
        let schema = df.schema().inner().clone();
        let batches = df.collect().await?;
        let result = QueryResult { schema, batches };
        tracing::Span::current().record("rows", result.num_rows());
        Ok(result)
    }

    pub async fn explain(&self, sql: &str) -> EngineResult<String> {
        let df = self.ctx.sql(sql).await?;
        let logical = format!("{}", df.logical_plan().display_indent());

        let physical = df.create_physical_plan().await?;
        let physical_str = format!(
            "{}",
            datafusion::physical_plan::displayable(physical.as_ref()).indent(true)
        );

        Ok(format!(
            "Logical Plan:\n{logical}\n\nPhysical Plan:\n{physical_str}"
        ))
    }

    pub async fn table_schema(&self, name: &str) -> EngineResult<TableSchema> {
        let provider = self
            .ctx
            .table_provider(name)
            .await
            .map_err(|_| EngineError::Table(format!("table '{name}' not found")))?;

        let schema = provider.schema();
        let columns = schema
            .fields()
            .iter()
            .map(|f| ColumnInfo {
                name: f.name().clone(),
                data_type: format!("{}", f.data_type()),
                nullable: f.is_nullable(),
            })
            .collect();

        Ok(TableSchema {
            table_name: name.to_string(),
            columns,
        })
    }

    pub async fn table_stats(&self, name: &str) -> EngineResult<TableStats> {
        let provider = self
            .ctx
            .table_provider(name)
            .await
            .map_err(|_| EngineError::Table(format!("table '{name}' not found")))?;

        let num_columns = provider.schema().fields().len();

        let sql = format!("SELECT COUNT(*) as cnt FROM \"{name}\"");
        let df = self.ctx.sql(&sql).await?;
        let batches = df.collect().await?;

        let num_rows = batches.first().map(|b| {
            let col = b.column(0).as_primitive::<Int64Type>();
            col.value(0) as usize
        });

        Ok(TableStats {
            table_name: name.to_string(),
            num_columns,
            num_rows,
        })
    }

    #[tracing::instrument(skip(self))]
    pub fn list_tables(&self) -> Vec<String> {
        let catalog = self.ctx.catalog("datafusion").unwrap();
        let schema = catalog.schema("public").unwrap();

        schema.table_names()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use olap_core::sample_data::create_sample_batch;

    fn setup() -> DataFusionEngine {
        let engine = DataFusionEngine::new();
        engine
            .register_batch("events", create_sample_batch())
            .unwrap();
        engine
    }

    #[tokio::test]
    async fn execute_select_all() {
        let engine = setup();
        let result = engine.execute("SELECT * FROM events").await.unwrap();
        assert_eq!(result.num_rows(), 8);
        assert_eq!(result.schema.fields().len(), 7);
    }

    #[tokio::test]
    async fn execute_with_filter() {
        let engine = setup();
        let result = engine
            .execute("SELECT * FROM events WHERE is_mobile = true")
            .await
            .unwrap();
        assert_eq!(result.num_rows(), 5);
    }

    #[tokio::test]
    async fn execute_aggregation() {
        let engine = setup();
        let result = engine
            .execute("SELECT country, COUNT(*) as cnt FROM events GROUP BY country")
            .await
            .unwrap();
        assert_eq!(result.num_rows(), 3);
    }

    #[tokio::test]
    async fn execute_invalid_table() {
        let engine = setup();
        let err = engine
            .execute("SELECT * FROM nonexistent")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("nonexistent"));
    }

    #[tokio::test]
    async fn list_tables_returns_registered() {
        let engine = setup();
        let tables = engine.list_tables();
        assert_eq!(tables, vec!["events"]);
    }

    #[tokio::test]
    async fn table_schema_returns_columns() {
        let engine = setup();
        let schema = engine.table_schema("events").await.unwrap();
        assert_eq!(schema.table_name, "events");
        assert_eq!(schema.columns.len(), 7);
        assert_eq!(schema.columns[0].name, "event_id");
    }

    #[tokio::test]
    async fn table_schema_not_found() {
        let engine = setup();
        let err = engine.table_schema("nonexistent").await.unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[tokio::test]
    async fn table_stats_returns_counts() {
        let engine = setup();
        let stats = engine.table_stats("events").await.unwrap();
        assert_eq!(stats.num_columns, 7);
        assert_eq!(stats.num_rows, Some(8));
    }

    #[tokio::test]
    async fn explain_returns_plan() {
        let engine = setup();
        let plan = engine.explain("SELECT * FROM events").await.unwrap();
        assert!(plan.contains("Logical Plan"));
        assert!(plan.contains("Physical Plan"));
    }
}
