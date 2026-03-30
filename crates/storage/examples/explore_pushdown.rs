use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{AsArray, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::compute::{filter, kernels::cmp::gt_eq};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::util::pretty::print_batches;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::file::statistics::Statistics;

fn create_large_batch(row_group_id: i64) -> RecordBatch {
    let n = 1000;
    let base_id = row_group_id * n as i64;

    let schema = Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::Int64, false),
        Field::new("page_url", DataType::Utf8, false),
        Field::new("country", DataType::Utf8, false),
        Field::new("duration_ms", DataType::Int32, false),
        Field::new("is_mobile", DataType::Boolean, false),
    ]));

    let pages = ["/home", "/products", "/checkout", "/about"];
    let countries = ["US", "DE", "JP", "GB"];

    let event_ids: Vec<i64> = (0..n).map(|i| base_id + i as i64).collect();
    let page_urls: Vec<&str> = (0..n).map(|i| pages[i % pages.len()]).collect();
    let country_vals: Vec<&str> = (0..n).map(|i| countries[i % countries.len()]).collect();
    let durations: Vec<i32> = (0..n)
        .map(|i| (row_group_id as i32 * 100) + (i as i32 % 500))
        .collect();
    let mobile: Vec<bool> = (0..n).map(|i| i % 2 == 0).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(event_ids)),
            Arc::new(StringArray::from(page_urls)),
            Arc::new(StringArray::from(country_vals)),
            Arc::new(Int32Array::from(durations)),
            Arc::new(BooleanArray::from(mobile)),
        ],
    )
    .unwrap()
}

fn main() {
    let path = Path::new("pushdown_test.parquet");

    let batch0 = create_large_batch(0);
    let batch1 = create_large_batch(1);
    let batch2 = create_large_batch(5);

    let props = WriterProperties::builder()
        .set_max_row_group_size(1000)
        .build();

    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch0.schema(), Some(props)).unwrap();
    writer.write(&batch0).unwrap();
    writer.write(&batch1).unwrap();
    writer.write(&batch2).unwrap();
    writer.close().unwrap();

    println!("=== Wrote 3 row groups (1000 rows each) ===");
    println!("  RG0: duration_ms range [0, 499]");
    println!("  RG1: duration_ms range [100, 599]");
    println!("  RG2: duration_ms range [500, 999]\n");

    println!("=== 1. Read ALL columns, ALL rows ===");
    let file = File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let all_batches: Vec<_> = reader.map(|b| b.unwrap()).collect();
    let total_rows: usize = all_batches.iter().map(|b| b.num_rows()).sum();
    let total_cols = all_batches[0].num_columns();
    println!("  rows={total_rows}, columns={total_cols}\n");

    println!("=== 2. Projection pushdown: only country + duration_ms ===");
    let file = File::open(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let file_metadata = builder.metadata().clone();
    let parquet_schema = file_metadata.file_metadata().schema_descr_ptr();
    let mask = parquet::arrow::ProjectionMask::columns(&parquet_schema, ["country", "duration_ms"]);
    let reader = builder.with_projection(mask).build().unwrap();
    let projected: Vec<_> = reader.map(|b| b.unwrap()).collect();
    println!(
        "  rows={}, columns={} (was {})",
        projected.iter().map(|b| b.num_rows()).sum::<usize>(),
        projected[0].num_columns(),
        total_cols,
    );
    println!(
        "  columns: {:?}\n",
        projected[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>()
    );
    print_batches(&[projected[0].clone()]).unwrap();

    println!("\n=== 3. Predicate pushdown: duration_ms >= 500 ===");
    println!("  checking row group statistics:");
    for i in 0..file_metadata.num_row_groups() {
        let rg = file_metadata.row_group(i);
        let dur_col = rg.column(3);
        if let Some(stats) = dur_col.statistics() {
            if let Statistics::Int32(s) = stats {
                let min = s.min_opt().unwrap();
                let max = s.max_opt().unwrap();
                let skip = *max < 500;
                println!(
                    "    RG{i}: duration min={min}, max={max} → {}",
                    if skip { "SKIP (max < 500)" } else { "READ" }
                );
            }
        }
    }

    let file = File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let mut filtered_rows = 0;
    for batch in reader {
        let batch = batch.unwrap();
        let durations = batch.column(3).as_primitive::<Int32Type>();
        let mask = gt_eq(durations, &Int32Array::new_scalar(500)).unwrap();
        let filtered = filter(durations, &mask).unwrap();
        filtered_rows += filtered.len();
    }
    println!("  rows matching duration_ms >= 500: {filtered_rows} (of {total_rows})");

    std::fs::remove_file(path).unwrap();
}
