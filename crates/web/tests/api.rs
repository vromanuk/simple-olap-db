use olap_core::sample_data::create_sample_batch;
use olap_engine::datafusion_engine::DataFusionEngine;
use olap_engine::query_engine::QueryEngine;
use olap_web::configuration::{ApplicationSettings, EngineType, Settings};
use olap_web::startup::Application;

struct TestApp {
    address: String,
    client: reqwest::Client,
}

async fn spawn_app() -> TestApp {
    let settings = Settings {
        application: ApplicationSettings {
            host: "127.0.0.1".to_string(),
            port: 0,
        },
        engine: EngineType::DataFusion,
    };

    let df_engine = DataFusionEngine::new();
    df_engine
        .register_batch("events", create_sample_batch())
        .unwrap();
    let engine = QueryEngine::DataFusion(df_engine);

    let app = Application::build(&settings, engine).await.unwrap();
    let port = app.port();
    tokio::spawn(app.run());

    TestApp {
        address: format!("http://127.0.0.1:{port}"),
        client: reqwest::Client::new(),
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
