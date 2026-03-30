use std::fs::File;
use std::path::Path;

use arrow::array::RecordBatch;
use olap_core::sample_data::create_sample_batch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};

fn write_parquet(batch: &RecordBatch, path: &Path, compression: Compression) -> u64 {
    let props = WriterProperties::builder()
        .set_compression(compression)
        .build();

    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();

    std::fs::metadata(path).unwrap().len()
}

fn inspect_encodings(path: &Path) {
    let file = File::open(path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();
    let rg = metadata.row_group(0);

    for col_idx in 0..rg.num_columns() {
        let col = rg.column(col_idx);
        let name = col.column_path().to_string();
        let encodings: Vec<_> = col.encodings().iter().map(|e| format!("{e:?}")).collect();
        println!(
            "  {name:<15} compressed={:<6} uncompressed={:<6} encodings={:?}",
            col.compressed_size(),
            col.uncompressed_size(),
            encodings
        );
    }
}

fn main() {
    let batch = create_sample_batch();
    let path = Path::new("compression_test.parquet");

    let arrow_size: usize = batch
        .columns()
        .iter()
        .map(|c| c.get_array_memory_size())
        .sum();
    println!("Arrow in-memory size: {} bytes\n", arrow_size);

    let codecs = vec![
        ("NONE", Compression::UNCOMPRESSED),
        ("SNAPPY", Compression::SNAPPY),
        ("ZSTD(3)", Compression::ZSTD(Default::default())),
        ("LZ4_RAW", Compression::LZ4_RAW),
    ];

    println!("{:<12} {:>10} {:>8}", "Codec", "File Size", "Ratio");
    println!("{}", "-".repeat(32));

    for (name, codec) in &codecs {
        let file_size = write_parquet(&batch, path, codec.clone());
        println!(
            "{:<12} {:>7} B {:>7.1}x",
            name,
            file_size,
            arrow_size as f64 / file_size as f64
        );
    }

    println!("\n=== Encodings per Column (SNAPPY) ===");
    write_parquet(&batch, path, Compression::SNAPPY);
    inspect_encodings(path);

    std::fs::remove_file(path).unwrap();
}
