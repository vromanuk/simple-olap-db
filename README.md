# simple-olap-db

A simple OLAP database built in Rust using the FDAP stack: Apache Arrow, Parquet, DataFusion, and Delta Lake. Exposed as a web service via Axum with swappable query engines and streaming ingestion.

The goal is learning OLAP internals, columnar storage, compression, query engines, and Rust web development from first principles.

## Quick Start

```bash
cargo run --bin olap-web
```

The server starts on `http://127.0.0.1:8080` with a sample `events` Delta table. Data persists across restarts in `data/events/`.

## API

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Liveness check |
| POST | `/query` | Execute SQL, return JSON results |
| POST | `/explain` | Show logical + physical query plan |
| GET | `/tables` | List registered tables |
| POST | `/tables` | Register a new table (delta or parquet) |
| GET | `/tables/{name}/schema` | Column names, types, nullability |
| GET | `/tables/{name}/stats` | Row count, column count |
| POST | `/tables/{name}/optimize` | Trigger Delta table compaction |
| POST | `/tables/{name}/vacuum` | Clean up old Delta files |
| POST | `/ingest` | Ingest events (DataFusion engine only) |

Only `SELECT` and `EXPLAIN` statements are allowed via `/query`.

### Examples

```bash
# Query
curl -s -X POST http://127.0.0.1:8080/query -H "Content-Type: application/json" -d '{"sql": "SELECT country, COUNT(*) as cnt FROM events GROUP BY country"}'

# Explain
curl -s -X POST http://127.0.0.1:8080/explain -H "Content-Type: application/json" -d '{"sql": "SELECT * FROM events WHERE is_mobile = true"}'

# List tables
curl -s http://127.0.0.1:8080/tables

# Table schema
curl -s http://127.0.0.1:8080/tables/events/schema

# Table stats
curl -s http://127.0.0.1:8080/tables/events/stats

# Ingest new events
curl -s -X POST http://127.0.0.1:8080/ingest -H "Content-Type: application/json" -d '{"events": [{"event_id": 100, "page_url": "/new", "user_id": 5000, "country": "FR", "duration_ms": 500, "timestamp_us": 1705400000000000, "is_mobile": true}]}'

# Register a parquet file as a table
curl -s -X POST http://127.0.0.1:8080/tables -H "Content-Type: application/json" -d '{"name": "data", "path": "sample_data.parquet", "format": "parquet"}'
```

## Ingestion Pipeline

Events are ingested via `POST /ingest`, buffered in a lock-free channel, coalesced into larger batches, and flushed to the Delta table periodically. Data becomes queryable after the next flush.

```
POST /ingest → channel (lock-free) → flush task (every 30s) → coalesce → Delta write
```

## Project Structure

```
crates/
  core/       Arrow types, compressed arrays, sample data
  storage/    Parquet, Delta Lake
  engine/     DataFusion, DuckDB, QueryEngine, SQL validation, ingestion, compaction
  web/        Axum HTTP API, configuration, telemetry
```

## Query Engines

| Engine | Description |
|--------|-------------|
| `datafusion` (default) | Apache DataFusion — Rust-native, Arrow-native, with Delta Lake + ingestion |
| `duckdb` | DuckDB — embedded C++ OLAP database (in-memory only, no ingestion) |

```bash
# DataFusion (default)
cargo run --bin olap-web

# DuckDB
APP_ENGINE=duckdb cargo run --bin olap-web
```

## Configuration

Settings in `configuration/base.yaml`, override with env vars (`APP_` prefix, `__` separator):

```bash
APP_APPLICATION__PORT=9090 cargo run --bin olap-web
APP_ENGINE=duckdb cargo run --bin olap-web
APP_INGEST__FLUSH_INTERVAL_SECS=10 cargo run --bin olap-web
APP_INGEST__MAX_BATCH_ROWS=5000 cargo run --bin olap-web
RUST_LOG=debug cargo run --bin olap-web
```
