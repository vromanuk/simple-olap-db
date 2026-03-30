use std::fs::File;
use std::path::Path;

use arrow::util::pretty::print_batches;
use olap_core::sample_data::create_sample_batch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::data_type::AsBytes;
use parquet::file::reader::FileReader;
use parquet::file::reader::SerializedFileReader;
use parquet::file::statistics::Statistics;

fn main() {
    let batch = create_sample_batch();
    let path = Path::new("sample_data.parquet");

    // --- WRITE ---
    println!("=== Writing to Parquet ===");
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    let file_metadata = writer.close().unwrap();
    println!(
        "wrote {} rows, {} row groups\n",
        file_metadata.num_rows,
        file_metadata.row_groups.len()
    );

    // --- READ BACK ---
    println!("=== Reading from Parquet ===");
    let file = File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();

    let batches: Vec<_> = reader.map(|b| b.unwrap()).collect();
    print_batches(&batches).unwrap();

    // --- INSPECT METADATA ---
    println!("\n=== File Metadata ===");
    let file = File::open(path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();

    println!("row groups: {}", metadata.num_row_groups());
    println!("total rows: {}\n", metadata.file_metadata().num_rows());

    for rg_idx in 0..metadata.num_row_groups() {
        let rg = metadata.row_group(rg_idx);
        println!("--- Row Group {rg_idx} ---");
        println!("  rows: {}", rg.num_rows());
        println!("  columns: {}", rg.num_columns());
        println!("  compressed size: {} bytes", rg.compressed_size());

        for col_idx in 0..rg.num_columns() {
            let col = rg.column(col_idx);
            let col_path = col.column_path().to_string();
            let _encoding = col.encodings();

            print!("  [{col_idx}] {col_path:<15}");
            print!(
                "compressed={:<6} uncompressed={:<6}",
                col.compressed_size(),
                col.uncompressed_size()
            );

            if let Some(stats) = col.statistics() {
                match stats {
                    Statistics::Int32(s) => {
                        print!(
                            "  min={:<6} max={:<6}",
                            s.min_opt().unwrap(),
                            s.max_opt().unwrap()
                        );
                    }
                    Statistics::Int64(s) => {
                        print!(
                            "  min={:<6} max={:<6}",
                            s.min_opt().unwrap(),
                            s.max_opt().unwrap()
                        );
                    }
                    Statistics::ByteArray(s) => {
                        let min = std::str::from_utf8(s.min_opt().unwrap().as_bytes()).unwrap();
                        let max = std::str::from_utf8(s.max_opt().unwrap().as_bytes()).unwrap();
                        print!("  min={min:<6} max={max:<6}");
                    }
                    Statistics::Boolean(s) => {
                        print!(
                            "  min={:<6} max={:<6}",
                            s.min_opt().unwrap(),
                            s.max_opt().unwrap()
                        );
                    }
                    _ => {}
                }
                print!("  nulls={}", stats.null_count_opt().unwrap_or(0));
            }
            println!();
        }
        println!();
    }

    // --- FILE SIZE COMPARISON ---
    let file_size = std::fs::metadata(path).unwrap().len();
    let arrow_size: usize = batch
        .columns()
        .iter()
        .map(|c| c.get_array_memory_size())
        .sum();
    println!("=== Size Comparison ===");
    println!("  Arrow in-memory: {} bytes", arrow_size);
    println!("  Parquet on-disk:  {} bytes", file_size);
    println!(
        "  ratio: {:.1}x smaller on disk",
        arrow_size as f64 / file_size as f64
    );

    // std::fs::remove_file(path).unwrap();
}
