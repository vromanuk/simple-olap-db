use olap_core::sample_data::create_sample_batch;
use olap_engine::compactor::{compaction_loop, CompactionConfig};
use olap_engine::datafusion_engine::DataFusionEngine;
use olap_engine::duckdb_engine::DuckDBEngine;
use olap_engine::query_engine::{EngineError, QueryEngine};
use olap_web::configuration::{get_configuration, CompactionSettings, EngineType};
use olap_web::startup::Application;
use olap_web::telemetry::init_tracing;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    init_tracing();

    let settings = get_configuration().expect("failed to read configuration");
    let engine = build_engine(&settings.engine).expect("failed to build query engine");

    let cancel = CancellationToken::new();

    if settings.compaction.enabled {
        let cancel_clone = cancel.clone();
        start_compaction_loop(&settings.compaction, cancel_clone);
    }

    let app = Application::build(&settings, engine).await?;

    tracing::info!(
        "listening on http://{}:{}",
        settings.application.host,
        app.port()
    );

    app.run().await?;
    cancel.cancel();

    Ok(())
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

fn start_compaction_loop(settings: &CompactionSettings, cancel: CancellationToken) {
    let config = CompactionConfig {
        interval: std::time::Duration::from_secs(settings.interval_secs),
        file_count_threshold: settings.file_count_threshold,
    };

    // TODO: populate from registered Delta table paths
    let table_paths: Vec<String> = vec![];

    tokio::spawn(compaction_loop(table_paths, config, cancel));
}
