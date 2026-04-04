use olap_core::sample_data::create_sample_batch;
use olap_engine::datafusion_engine::DataFusionEngine;
use olap_engine::query_engine::{EngineError, QueryEngine};
use olap_web::configuration::get_configuration;
use olap_web::startup::Application;
use olap_web::telemetry::init_tracing;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    init_tracing();

    let settings = get_configuration().expect("failed to read configuration");
    let engine = build_engine().expect("failed to build query engine");
    let app = Application::build(&settings, engine).await?;

    tracing::info!(
        "listening on http://{}:{}",
        settings.application.host,
        app.port()
    );

    app.run().await
}

fn build_engine() -> Result<QueryEngine, EngineError> {
    let df_engine = DataFusionEngine::new();

    df_engine.register_batch("events", create_sample_batch())?;
    tracing::info!("registered table: events");

    Ok(QueryEngine::DataFusion(df_engine))
}
