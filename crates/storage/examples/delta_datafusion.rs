use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::*;
use deltalake::operations::DeltaOps;
use deltalake::protocol::SaveMode;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let table_path = "delta_datafusion_test";

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("country", DataType::Utf8, false),
        Field::new("temperature", DataType::Int32, false),
    ]));

    let batch0 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec![
                "Berlin", "Tokyo", "New York", "Munich",
            ])),
            Arc::new(StringArray::from(vec!["DE", "JP", "US", "DE"])),
            Arc::new(Int32Array::from(vec![15, 22, 18, 12])),
        ],
    )?;

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![5, 6, 7])),
            Arc::new(StringArray::from(vec!["London", "Paris", "Osaka"])),
            Arc::new(StringArray::from(vec!["GB", "FR", "JP"])),
            Arc::new(Int32Array::from(vec![10, 20, 25])),
        ],
    )?;

    let table = DeltaOps::try_from_uri(table_path)
        .await?
        .write(vec![batch0])
        .with_save_mode(SaveMode::Overwrite)
        .await?;

    let table = DeltaOps(table)
        .write(vec![batch1])
        .with_save_mode(SaveMode::Append)
        .await?;

    println!(
        "=== Delta table: {} files, version {} ===\n",
        table.get_files_iter()?.count(),
        table.version()
    );

    let ctx = SessionContext::new();
    ctx.register_table("cities", Arc::new(table))?;

    println!("=== 1. SELECT * ===");
    ctx.sql("SELECT * FROM cities ORDER BY id")
        .await?
        .show()
        .await?;

    println!("\n=== 2. Filter + Aggregation ===");
    ctx.sql("SELECT country, COUNT(*) as cnt, AVG(temperature) as avg_temp FROM cities GROUP BY country ORDER BY avg_temp DESC")
        .await?
        .show()
        .await?;

    println!("\n=== 3. Physical plan (observe pushdown) ===");
    let df = ctx
        .sql("SELECT city, temperature FROM cities WHERE country = 'JP'")
        .await?;
    let physical = df.clone().create_physical_plan().await?;
    println!(
        "{}",
        datafusion::physical_plan::displayable(physical.as_ref()).indent(true)
    );
    println!("--- Results ---");
    df.show().await?;

    println!("\n=== 4. Time travel: query version 0 only ===");
    let table_v0 = deltalake::open_table_with_version(table_path, 0).await?;
    let ctx_v0 = SessionContext::new();
    ctx_v0.register_table("cities_v0", Arc::new(table_v0))?;
    ctx_v0
        .sql("SELECT * FROM cities_v0 ORDER BY id")
        .await?
        .show()
        .await?;

    std::fs::remove_dir_all(table_path)?;

    Ok(())
}
