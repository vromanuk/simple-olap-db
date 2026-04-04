use olap_core::sample_data::create_sample_batch;
use olap_engine::datafusion_engine::DataFusionEngine;
use olap_engine::duckdb_engine::DuckDBEngine;
use olap_engine::query_engine::{EngineError, QueryEngine};
use olap_web::configuration::{get_configuration, EngineType};
use olap_web::startup::Application;
use olap_web::telemetry::init_tracing;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    init_tracing();

    let settings = get_configuration().expect("failed to read configuration");
    let engine = build_engine(&settings.engine).expect("failed to build query engine");
    let app = Application::build(&settings, engine).await?;

    tracing::info!(
        "listening on http://{}:{}",
        settings.application.host,
        app.port()
    );

    app.run().await
}

fn build_engine(engine_type: &EngineType) -> Result<QueryEngine, EngineError> {
    match engine_type {
        EngineType::DataFusion => {
            tracing::info!("using DataFusion engine");
            let engine = DataFusionEngine::new();
            engine.register_batch("events", create_sample_batch())?;
            tracing::info!("registered table: events");
            Ok(QueryEngine::DataFusion(engine))
        }
        EngineType::DuckDB => {
            tracing::info!("using DuckDB engine");
            let engine = DuckDBEngine::new()?;
            engine.register_sample_data()?;
            tracing::info!("registered table: events");
            Ok(QueryEngine::DuckDB(engine))
        }
    }
}
