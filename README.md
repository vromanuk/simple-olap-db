# simple-olap-db

A simple OLAP database built in Rust using the FDAP stack: Apache Arrow, Parquet, DataFusion, and Delta Lake. Exposed as a web service via Axum with swappable query engines.

The goal is learning OLAP internals, columnar storage, compression, query engines, and Rust web development from first principles.

## Quick Start

```bash
cargo run --bin olap-web
```

The server starts on `http://127.0.0.1:8080` with a sample `events` table pre-loaded.

## API

**Health check:**
```bash
curl http://127.0.0.1:8080/health
```

**Run a SQL query:**
```bash
curl -s -X POST http://127.0.0.1:8080/query -H "Content-Type: application/json" -d '{"sql": "SELECT * FROM events"}'
```

**Aggregation example:**
```bash
curl -s -X POST http://127.0.0.1:8080/query -H "Content-Type: application/json" -d '{"sql": "SELECT country, COUNT(*) as cnt, AVG(duration_ms) as avg_dur FROM events GROUP BY country ORDER BY cnt DESC"}'
```

Only `SELECT` and `EXPLAIN` statements are allowed. `INSERT`, `DROP`, etc. are rejected.

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
