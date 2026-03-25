# simple-olap-db

A simple OLAP database built in Rust using the FDAP stack: Apache Arrow,
Parquet, DataFusion, and Delta Lake. Exposed as a web service via Axum,
with swappable query engines (DataFusion / DuckDB).

The goal is learning OLAP internals, columnar storage, compression,
query engines, and Rust web development from first principles.
