use std::sync::{Arc, Mutex};

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, RecordBatch, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use duckdb::types::Value;
use duckdb::Connection;

use crate::query_engine::{
    ColumnInfo, EngineError, EngineResult, QueryResult, TableSchema, TableStats,
};

pub struct DuckDBEngine {
    conn: Mutex<Connection>,
}

impl DuckDBEngine {
    pub fn new() -> EngineResult<Self> {
        let conn = Connection::open_in_memory()
            .map_err(|e| EngineError::Table(format!("failed to open DuckDB: {e}")))?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn register_sample_data(&self) -> EngineResult<()> {
        self.run_ddl(
            "CREATE TABLE events AS
             SELECT * FROM (VALUES
                (1, '/home', 1001, 'US', 250, true),
                (2, '/products', 1002, 'DE', NULL, false),
                (3, '/home', NULL, 'US', 120, true),
                (4, '/checkout', 1001, 'JP', 890, false),
                (5, '/products', 1003, 'US', 340, true),
                (6, '/home', NULL, 'DE', 150, true),
                (7, '/about', 1002, 'US', NULL, false),
                (8, '/products', 1004, 'JP', 200, true)
             ) AS t(event_id, page_url, user_id, country, duration_ms, is_mobile)",
        )
    }

    fn run_ddl(&self, sql: &str) -> EngineResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(sql)
            .map_err(|e| EngineError::Table(format!("DDL failed: {e}")))?;
        Ok(())
    }

    pub fn register_parquet_file(&self, name: &str, path: &str) -> EngineResult<()> {
        self.run_ddl(&format!(
            "CREATE VIEW \"{name}\" AS SELECT * FROM read_parquet('{path}')"
        ))
    }

    #[tracing::instrument(skip(self), fields(rows))]
    pub fn execute(&self, sql: &str) -> EngineResult<QueryResult> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| EngineError::Table(format!("failed to acquire DuckDB lock: {e}")))?;

        let mut stmt = conn
            .prepare(sql)
            .map_err(|e| EngineError::Table(format!("DuckDB prepare failed: {e}")))?;

        // Collect all rows first — query_map executes the statement internally
        let all_rows: Vec<Vec<Value>> = stmt
            .query_map([], |row| {
                let count = row.as_ref().column_count();
                let mut values = Vec::with_capacity(count);
                for i in 0..count {
                    values.push(row.get::<_, Value>(i)?);
                }
                Ok(values)
            })
            .map_err(|e| EngineError::Table(format!("DuckDB query failed: {e}")))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| EngineError::Table(format!("DuckDB row error: {e}")))?;

        // Column names available after execution
        let column_count = stmt.column_count();
        let column_names: Vec<String> = (0..column_count)
            .map(|i| {
                stmt.column_name(i)
                    .map(|s| s.to_string())
                    .unwrap_or_else(|_| format!("col_{i}"))
            })
            .collect();

        if all_rows.is_empty() {
            let fields: Vec<Field> = column_names
                .iter()
                .map(|n| Field::new(n, DataType::Utf8, true))
                .collect();
            let schema = Arc::new(Schema::new(fields));
            return Ok(QueryResult {
                schema,
                batches: vec![],
            });
        }

        let (schema, columns) = build_arrow_columns(&column_names, &all_rows)?;
        let batch = RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| EngineError::Table(format!("failed to build RecordBatch: {e}")))?;

        let result = QueryResult {
            schema,
            batches: vec![batch],
        };
        tracing::Span::current().record("rows", result.num_rows());
        Ok(result)
    }

    #[tracing::instrument(skip(self))]
    pub fn list_tables(&self) -> Vec<String> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")
            .unwrap();

        stmt.query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect()
    }

    pub fn table_schema(&self, name: &str) -> EngineResult<TableSchema> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = ?")
            .map_err(|e| EngineError::Table(e.to_string()))?;

        let columns: Vec<ColumnInfo> = stmt
            .query_map([name], |row| {
                Ok(ColumnInfo {
                    name: row.get(0)?,
                    data_type: row.get(1)?,
                    nullable: row.get::<_, String>(2)? == "YES",
                })
            })
            .map_err(|e| EngineError::Table(e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();

        if columns.is_empty() {
            return Err(EngineError::Table(format!("table '{name}' not found")));
        }

        Ok(TableSchema {
            table_name: name.to_string(),
            columns,
        })
    }

    pub fn table_stats(&self, name: &str) -> EngineResult<TableStats> {
        let schema = self.table_schema(name)?;
        let conn = self.conn.lock().unwrap();
        let sql = format!("SELECT COUNT(*) FROM \"{name}\"");
        let num_rows: i64 = conn
            .query_row(&sql, [], |row| row.get(0))
            .map_err(|e| EngineError::Table(e.to_string()))?;

        Ok(TableStats {
            table_name: name.to_string(),
            num_columns: schema.columns.len(),
            num_rows: Some(num_rows as usize),
        })
    }
}

trait ColumnBuilder {
    fn append(&mut self, value: &Value);
    fn finish_column(self: Box<Self>) -> (DataType, ArrayRef);
}

struct Int64Column(Int64Builder);
impl ColumnBuilder for Int64Column {
    fn append(&mut self, value: &Value) {
        match value {
            Value::Null => self.0.append_null(),
            Value::Int(v) => self.0.append_value(*v as i64),
            Value::BigInt(v) => self.0.append_value(*v),
            Value::HugeInt(v) => self.0.append_value(*v as i64),
            _ => self.0.append_null(),
        }
    }
    fn finish_column(mut self: Box<Self>) -> (DataType, ArrayRef) {
        (DataType::Int64, Arc::new(self.0.finish()))
    }
}

struct Float64Column(Float64Builder);
impl ColumnBuilder for Float64Column {
    fn append(&mut self, value: &Value) {
        match value {
            Value::Null => self.0.append_null(),
            Value::Float(v) => self.0.append_value(*v as f64),
            Value::Double(v) => self.0.append_value(*v),
            _ => self.0.append_null(),
        }
    }
    fn finish_column(mut self: Box<Self>) -> (DataType, ArrayRef) {
        (DataType::Float64, Arc::new(self.0.finish()))
    }
}

struct BooleanColumn(BooleanBuilder);
impl ColumnBuilder for BooleanColumn {
    fn append(&mut self, value: &Value) {
        match value {
            Value::Null => self.0.append_null(),
            Value::Boolean(v) => self.0.append_value(*v),
            _ => self.0.append_null(),
        }
    }
    fn finish_column(mut self: Box<Self>) -> (DataType, ArrayRef) {
        (DataType::Boolean, Arc::new(self.0.finish()))
    }
}

struct StringColumn(StringBuilder);
impl ColumnBuilder for StringColumn {
    fn append(&mut self, value: &Value) {
        match value {
            Value::Null => self.0.append_null(),
            Value::Text(s) => self.0.append_value(s),
            v => self.0.append_value(format!("{v:?}")),
        }
    }
    fn finish_column(mut self: Box<Self>) -> (DataType, ArrayRef) {
        (DataType::Utf8, Arc::new(self.0.finish()))
    }
}

fn make_builder(sample: Option<&Value>, capacity: usize) -> Box<dyn ColumnBuilder> {
    match sample {
        Some(Value::Int(..)) | Some(Value::BigInt(..)) | Some(Value::HugeInt(..)) => {
            Box::new(Int64Column(Int64Builder::with_capacity(capacity)))
        }
        Some(Value::Float(..)) | Some(Value::Double(..)) => {
            Box::new(Float64Column(Float64Builder::with_capacity(capacity)))
        }
        Some(Value::Boolean(..)) => {
            Box::new(BooleanColumn(BooleanBuilder::with_capacity(capacity)))
        }
        _ => Box::new(StringColumn(StringBuilder::new())),
    }
}

fn build_arrow_columns(
    names: &[String],
    rows: &[Vec<Value>],
) -> EngineResult<(SchemaRef, Vec<ArrayRef>)> {
    let mut fields = Vec::with_capacity(names.len());
    let mut columns = Vec::with_capacity(names.len());

    for col_idx in 0..names.len() {
        let first_non_null = rows.iter().find_map(|row| match &row[col_idx] {
            Value::Null => None,
            v => Some(v),
        });

        let mut builder = make_builder(first_non_null, rows.len());
        for row in rows {
            builder.append(&row[col_idx]);
        }

        let (data_type, array) = builder.finish_column();
        fields.push(Field::new(&names[col_idx], data_type, true));
        columns.push(array);
    }

    Ok((Arc::new(Schema::new(fields)), columns))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> DuckDBEngine {
        let engine = DuckDBEngine::new().unwrap();
        engine.register_sample_data().unwrap();
        engine
    }

    #[test]
    fn execute_select_all() {
        let engine = setup();
        let result = engine.execute("SELECT * FROM events").unwrap();
        assert_eq!(result.num_rows(), 8);
    }

    #[test]
    fn execute_with_filter() {
        let engine = setup();
        let result = engine
            .execute("SELECT * FROM events WHERE is_mobile = true")
            .unwrap();
        assert_eq!(result.num_rows(), 5);
    }

    #[test]
    fn execute_aggregation() {
        let engine = setup();
        let result = engine
            .execute("SELECT country, COUNT(*) as cnt FROM events GROUP BY country")
            .unwrap();
        assert_eq!(result.num_rows(), 3);
    }

    #[test]
    fn execute_invalid_table() {
        let engine = setup();
        let err = engine.execute("SELECT * FROM nonexistent").unwrap_err();
        assert!(err.to_string().to_lowercase().contains("nonexistent"));
    }

    #[test]
    fn list_tables_returns_registered() {
        let engine = setup();
        let tables = engine.list_tables();
        assert!(tables.contains(&"events".to_string()));
    }

    #[test]
    fn table_schema_returns_columns() {
        let engine = setup();
        let schema = engine.table_schema("events").unwrap();
        assert_eq!(schema.table_name, "events");
        assert_eq!(schema.columns.len(), 6);
    }

    #[test]
    fn table_schema_not_found() {
        let engine = setup();
        let err = engine.table_schema("nonexistent").unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn table_stats_returns_counts() {
        let engine = setup();
        let stats = engine.table_stats("events").unwrap();
        assert_eq!(stats.num_rows, Some(8));
        assert_eq!(stats.num_columns, 6);
    }
}
