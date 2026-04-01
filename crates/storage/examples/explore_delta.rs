use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use deltalake::operations::DeltaOps;
use deltalake::protocol::SaveMode;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let table_path = "delta_test_table";

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("temperature", DataType::Int32, false),
    ]));

    // --- Write version 0 ---
    println!("=== Writing version 0 ===");
    let batch0 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Berlin", "Tokyo", "New York"])),
            Arc::new(Int32Array::from(vec![15, 22, 18])),
        ],
    )?;

    let table = DeltaOps::try_from_uri(table_path)
        .await?
        .write(vec![batch0])
        .with_save_mode(SaveMode::Overwrite)
        .await?;

    println!("  version: {}", table.version());
    println!("  files: {:?}", table.get_files_iter()?.collect::<Vec<_>>());

    // --- Write version 1 (append more data) ---
    println!("\n=== Writing version 1 (append) ===");
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![4, 5])),
            Arc::new(StringArray::from(vec!["London", "Paris"])),
            Arc::new(Int32Array::from(vec![12, 20])),
        ],
    )?;

    let table = DeltaOps(table)
        .write(vec![batch1])
        .with_save_mode(SaveMode::Append)
        .await?;

    println!("  version: {}", table.version());
    println!("  files: {:?}", table.get_files_iter()?.collect::<Vec<_>>());

    // --- Inspect the transaction log ---
    println!("\n=== Transaction Log (_delta_log/) ===");
    for entry in std::fs::read_dir(format!("{table_path}/_delta_log"))? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().map_or(false, |e| e == "json") {
            println!("\n--- {} ---", path.file_name().unwrap().to_string_lossy());
            let content = std::fs::read_to_string(&path)?;
            for line in content.lines() {
                let v: serde_json::Value = serde_json::from_str(line)?;
                println!("  {}", serde_json::to_string_pretty(&v)?);
            }
        }
    }

    // --- Time travel: read version 0 ---
    println!("\n=== Time Travel: read version 0 ===");
    let table_v0 = deltalake::open_table_with_version(table_path, 0).await?;
    println!("  version: {}", table_v0.version());
    println!(
        "  files: {:?}",
        table_v0.get_files_iter()?.collect::<Vec<_>>()
    );

    println!("\n=== Time Travel: read version 1 (current) ===");
    let table_v1 = deltalake::open_table_with_version(table_path, 1).await?;
    println!("  version: {}", table_v1.version());
    println!(
        "  files: {:?}",
        table_v1.get_files_iter()?.collect::<Vec<_>>()
    );

    // Cleanup
    std::fs::remove_dir_all(table_path)?;

    Ok(())
}
