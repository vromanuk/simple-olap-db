# simple-olap-db

A simple OLAP database built in Rust using the FDAP stack: Apache Arrow, Parquet, DataFusion, and Delta Lake. Exposed as a web service via Axum with swappable query engines.

The goal is learning OLAP internals, columnar storage, compression, query engines, and Rust web development from first principles.

## Quick Start

```bash
cargo run --bin olap-web
```

The server starts on `http://127.0.0.1:8080` with a sample `events` table pre-loaded.

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

Only `SELECT` and `EXPLAIN` statements are allowed via `/query`. `INSERT`, `DROP`, etc. are rejected.

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

# Register a parquet file as a table
curl -s -X POST http://127.0.0.1:8080/tables -H "Content-Type: application/json" -d '{"name": "data", "path": "sample_data.parquet", "format": "parquet"}'
```

## Project Structure

```
crates/
  core/       Arrow types, sample data, shared types
  storage/    Parquet, Delta Lake
  engine/     DataFusion, QueryEngine enum, SQL validation
  web/        Axum HTTP API, configuration, telemetry
```

## Configuration

Settings are loaded from `configuration/base.yaml` + `configuration/local.yaml`. Override with environment variables:

```bash
APP_APPLICATION__PORT=9090 cargo run --bin olap-web
```

Debug logging (shows query execution spans):

```bash
RUST_LOG=debug cargo run --bin olap-web
```
