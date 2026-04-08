use std::sync::Arc;

use olap_core::sample_data::{create_sample_batch, web_analytics_schema};
use olap_engine::datafusion_engine::DataFusionEngine;
use olap_engine::ingest::create_ingest_channel;
use olap_engine::query_engine::QueryEngine;
use olap_web::configuration::{
    ApplicationSettings, CompactionSettings, EngineType, IngestSettings, Settings,
};
use olap_web::startup::Application;

struct TestApp {
    address: String,
    client: reqwest::Client,
    _ingest_receiver: Option<olap_engine::ingest::IngestReceiver>,
}

async fn spawn_app() -> TestApp {
    let settings = Settings {
        application: ApplicationSettings {
            host: "127.0.0.1".to_string(),
            port: 0,
        },
        engine: EngineType::DataFusion,
        compaction: CompactionSettings {
            enabled: false,
            interval_secs: 3600,
            file_count_threshold: 100,
        },
        ingest: IngestSettings {
            max_batch_rows: 10_000,
            channel_capacity: 10_000,
            flush_interval_secs: 30,
        },
    };

    let df_engine = DataFusionEngine::new();
    df_engine
        .register_batch("events", create_sample_batch())
        .unwrap();
    let engine = QueryEngine::DataFusion(df_engine);

    let app = Application::build(&settings, engine, None).await.unwrap();
    let port = app.port();
    tokio::spawn(app.run());

    TestApp {
        address: format!("http://127.0.0.1:{port}"),
        client: reqwest::Client::new(),
        _ingest_receiver: None,
    }
}

#[tokio::test]
async fn health_returns_200() {
    let app = spawn_app().await;
    let res = app
        .client
        .get(format!("{}/health", app.address))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
}

#[tokio::test]
async fn query_returns_results() {
    let app = spawn_app().await;
    let res = app
        .client
        .post(format!("{}/query", app.address))
        .json(&serde_json::json!({"sql": "SELECT * FROM events"}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);

    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["columns"].as_array().unwrap().len(), 7);
    assert_eq!(body["rows"].as_array().unwrap().len(), 8);
}

#[tokio::test]
async fn query_with_filter() {
    let app = spawn_app().await;
    let res = app
        .client
        .post(format!("{}/query", app.address))
        .json(&serde_json::json!({"sql": "SELECT * FROM events WHERE is_mobile = true"}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);

    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["rows"].as_array().unwrap().len(), 5);
}

#[tokio::test]
async fn query_rejects_drop() {
    let app = spawn_app().await;
    let res = app
        .client
        .post(format!("{}/query", app.address))
        .json(&serde_json::json!({"sql": "DROP TABLE events"}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn query_rejects_invalid_sql() {
    let app = spawn_app().await;
    let res = app
        .client
        .post(format!("{}/query", app.address))
        .json(&serde_json::json!({"sql": "SELEKT garbage"}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn tables_returns_registered() {
    let app = spawn_app().await;
    let res = app
        .client
        .get(format!("{}/tables", app.address))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);

    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["tables"].as_array().unwrap(), &["events"]);
}

#[tokio::test]
async fn schema_returns_columns() {
    let app = spawn_app().await;
    let res = app
        .client
        .get(format!("{}/tables/events/schema", app.address))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);

    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["table"], "events");
    assert_eq!(body["columns"].as_array().unwrap().len(), 7);
    assert_eq!(body["columns"][0]["name"], "event_id");
}

#[tokio::test]
async fn schema_not_found_returns_error() {
    let app = spawn_app().await;
    let res = app
        .client
        .get(format!("{}/tables/nonexistent/schema", app.address))
        .send()
        .await
        .unwrap();
    assert!(res.status().is_server_error() || res.status().is_client_error());
}

#[tokio::test]
async fn stats_returns_counts() {
    let app = spawn_app().await;
    let res = app
        .client
        .get(format!("{}/tables/events/stats", app.address))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);

    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["table"], "events");
    assert_eq!(body["num_columns"], 7);
    assert_eq!(body["num_rows"], 8);
}

#[tokio::test]
async fn explain_returns_plan() {
    let app = spawn_app().await;
    let res = app
        .client
        .post(format!("{}/explain", app.address))
        .json(&serde_json::json!({"sql": "SELECT * FROM events"}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);

    let body: serde_json::Value = res.json().await.unwrap();
    let plan = body["plan"].as_str().unwrap();
    assert!(plan.contains("Logical Plan"));
    assert!(plan.contains("Physical Plan"));
}

#[tokio::test]
async fn query_nonexistent_table_returns_error() {
    let app = spawn_app().await;
    let res = app
        .client
        .post(format!("{}/query", app.address))
        .json(&serde_json::json!({"sql": "SELECT * FROM nonexistent"}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn query_missing_body_returns_error() {
    let app = spawn_app().await;
    let res = app
        .client
        .post(format!("{}/query", app.address))
        .header("Content-Type", "application/json")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert!(res.status().is_client_error());
}

#[tokio::test]
async fn register_parquet_invalid_path_returns_error() {
    let app = spawn_app().await;
    let res = app
        .client
        .post(format!("{}/tables", app.address))
        .json(&serde_json::json!({
            "name": "bad",
            "path": "/nonexistent/path.parquet",
            "format": "parquet"
        }))
        .send()
        .await
        .unwrap();
    assert!(res.status().is_server_error() || res.status().is_client_error());
}

#[tokio::test]
async fn register_invalid_format_returns_error() {
    let app = spawn_app().await;
    let res = app
        .client
        .post(format!("{}/tables", app.address))
        .json(&serde_json::json!({
            "name": "bad",
            "path": "some.csv",
            "format": "csv"
        }))
        .send()
        .await
        .unwrap();
    assert!(res.status().is_client_error());
}

async fn spawn_app_with_ingest() -> TestApp {
    let settings = Settings {
        application: ApplicationSettings {
            host: "127.0.0.1".to_string(),
            port: 0,
        },
        engine: EngineType::DataFusion,
        compaction: CompactionSettings {
            enabled: false,
            interval_secs: 3600,
            file_count_threshold: 100,
        },
        ingest: IngestSettings {
            max_batch_rows: 100,
            channel_capacity: 1000,
            flush_interval_secs: 30,
        },
    };

    let df_engine = DataFusionEngine::new();
    df_engine
        .register_batch("events", create_sample_batch())
        .unwrap();
    let engine = QueryEngine::DataFusion(df_engine);

    let schema = Arc::new(web_analytics_schema());
    let (sender, receiver) = create_ingest_channel(schema, 1000, vec![], 100);

    let app = Application::build(&settings, engine, Some(sender))
        .await
        .unwrap();
    let port = app.port();
    tokio::spawn(app.run());

    TestApp {
        address: format!("http://127.0.0.1:{port}"),
        client: reqwest::Client::new(),
        _ingest_receiver: Some(receiver),
    }
}

#[tokio::test]
async fn ingest_accepts_valid_events() {
    let app = spawn_app_with_ingest().await;
    let res = app
        .client
        .post(format!("{}/ingest", app.address))
        .json(&serde_json::json!({
            "events": [
                {"event_id": 100, "page_url": "/test", "user_id": 5000, "country": "FR",
                 "duration_ms": 300, "timestamp_us": 1705400000000000_i64, "is_mobile": true}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);

    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["rows_accepted"], 1);
}

#[tokio::test]
async fn ingest_accepts_multiple_events() {
    let app = spawn_app_with_ingest().await;
    let res = app
        .client
        .post(format!("{}/ingest", app.address))
        .json(&serde_json::json!({
            "events": [
                {"event_id": 100, "page_url": "/a", "user_id": 1, "country": "US",
                 "duration_ms": 100, "timestamp_us": 1705400000000000_i64, "is_mobile": true},
                {"event_id": 101, "page_url": "/b", "user_id": 2, "country": "DE",
                 "duration_ms": 200, "timestamp_us": 1705400001000000_i64, "is_mobile": false}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);

    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["rows_accepted"], 2);
    assert!(body["pending_total"].as_u64().unwrap() >= 2);
}

#[tokio::test]
async fn ingest_rejects_empty_events() {
    let app = spawn_app_with_ingest().await;
    let res = app
        .client
        .post(format!("{}/ingest", app.address))
        .json(&serde_json::json!({"events": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn ingest_not_available_without_buffer() {
    let app = spawn_app().await;
    let res = app
        .client
        .post(format!("{}/ingest", app.address))
        .json(&serde_json::json!({
            "events": [{"event_id": 1, "page_url": "/x", "country": "US"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 404);
}
