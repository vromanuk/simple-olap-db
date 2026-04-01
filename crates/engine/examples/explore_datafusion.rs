use std::fs::File;
use std::sync::Arc;

use datafusion::functions_aggregate::expr_fn::avg;
use datafusion::prelude::*;
use olap_core::sample_data::create_sample_batch;
use parquet::arrow::ArrowWriter;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let batch = create_sample_batch();
    ctx.register_batch("events", batch.clone())?;

    println!("=== 1. Simple SELECT ===");
    ctx.sql("SELECT * FROM events").await?.show().await?;

    println!("\n=== 2. Filter + Projection ===");
    ctx.sql("SELECT page_url, duration_ms FROM events WHERE is_mobile = true")
        .await?
        .show()
        .await?;

    println!("\n=== 3. Aggregation with ORDER BY ===");
    ctx.sql("SELECT country, COUNT(*) as cnt, AVG(duration_ms) as avg_dur FROM events GROUP BY country ORDER BY cnt DESC")
        .await?
        .show()
        .await?;

    println!("\n=== 4. Optimizer: Logical vs Physical ===");
    let df = ctx
        .sql("SELECT country, AVG(duration_ms) as avg_dur FROM events WHERE is_mobile = true GROUP BY country")
        .await?;

    println!("--- Logical Plan ---");
    println!("{}\n", df.logical_plan().display_indent());

    println!("--- Physical Plan ---");
    let physical = df.create_physical_plan().await?;
    println!(
        "{}",
        datafusion::physical_plan::displayable(physical.as_ref()).indent(true)
    );

    println!("\n=== 5. DataFrame API ===");
    let df = ctx
        .table("events")
        .await?
        .filter(col("is_mobile").eq(lit(true)))?
        .aggregate(
            vec![col("country")],
            vec![avg(col("duration_ms")).alias("avg_dur")],
        )?
        .sort(vec![col("avg_dur").sort(true, true)])?;

    df.show().await?;

    println!("\n=== 6. Parquet source: physical plan comparison ===");
    let parquet_path = "events_for_datafusion.parquet";
    let file = File::create(parquet_path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    ctx.register_parquet("events_pq", parquet_path, ParquetReadOptions::default())
        .await?;

    let df = ctx
        .sql("SELECT country, AVG(duration_ms) as avg_dur FROM events_pq WHERE is_mobile = true GROUP BY country")
        .await?;

    println!("--- Physical Plan (Parquet source) ---");
    let physical = df.clone().create_physical_plan().await?;
    println!(
        "{}",
        datafusion::physical_plan::displayable(physical.as_ref()).indent(true)
    );

    println!("\n--- Results ---");
    df.show().await?;

    std::fs::remove_file(parquet_path).unwrap();

    println!("\n=== 7. Custom TableProvider ===");
    let custom = olap_engine::custom_table::CustomTable::new(vec![create_sample_batch()])?;
    ctx.register_table("custom_events", Arc::new(custom))?;

    ctx.sql(
        "SELECT country, COUNT(*) as cnt FROM custom_events GROUP BY country ORDER BY cnt DESC",
    )
    .await?
    .show()
    .await?;

    Ok(())
}
