use std::sync::Arc;

use olap_core::sample_data::{create_sample_batch, web_analytics_schema};
use olap_engine::compactor::{compaction_loop, CompactionConfig};
use olap_engine::datafusion_engine::DataFusionEngine;
use olap_engine::duckdb_engine::DuckDBEngine;
use olap_engine::ingest::{create_ingest_channel, flush_loop, IngestSender};
use olap_engine::query_engine::{EngineError, QueryEngine};
use olap_web::configuration::{get_configuration, CompactionSettings, EngineType};
use olap_web::startup::Application;
use olap_web::telemetry::init_tracing;
use tokio_util::sync::CancellationToken;

const EVENTS_DELTA_PATH: &str = "data/events";

#[tokio::main]
async fn main() -> std::io::Result<()> {
    init_tracing();

    let settings = get_configuration().expect("failed to read configuration");
    let cancel = CancellationToken::new();

    let (engine, ingest_sender) = build_engine(&settings.engine, &settings.ingest, &cancel)
        .await
        .expect("failed to build query engine");

    if settings.compaction.enabled {
        start_compaction_loop(&settings.compaction, cancel.clone());
    }

    let app = Application::build(&settings, engine, ingest_sender).await?;

    tracing::info!(
        "listening on http://{}:{}",
        settings.application.host,
        app.port()
    );

    app.run().await?;
    cancel.cancel();

    Ok(())
}

use olap_web::configuration::IngestSettings;

async fn build_engine(
    engine_type: &EngineType,
    ingest_settings: &IngestSettings,
    cancel: &CancellationToken,
) -> Result<(QueryEngine, Option<Arc<IngestSender>>), EngineError> {
    match engine_type {
        EngineType::DataFusion => {
            tracing::info!("using DataFusion engine");
            let engine = DataFusionEngine::new();

            init_delta_table().await?;
            engine
                .register_delta_table("events", EVENTS_DELTA_PATH)
                .await?;
            tracing::info!("registered Delta table: events");

            let schema = Arc::new(web_analytics_schema());
            let (sender, receiver) = create_ingest_channel(
                schema,
                ingest_settings.channel_capacity,
                vec!["country".to_string()],
                ingest_settings.max_batch_rows,
            );

            let flush_interval =
                std::time::Duration::from_secs(ingest_settings.flush_interval_secs);
            tokio::spawn(flush_loop(
                Arc::clone(&sender),
                receiver,
                EVENTS_DELTA_PATH.to_string(),
                flush_interval,
                cancel.clone(),
            ));
            tracing::info!(
                flush_interval_secs = ingest_settings.flush_interval_secs,
                max_batch_rows = ingest_settings.max_batch_rows,
                channel_capacity = ingest_settings.channel_capacity,
                "ingest pipeline started"
            );

            Ok((QueryEngine::DataFusion(engine), Some(sender)))
        }
        EngineType::DuckDB => {
            tracing::info!("using DuckDB engine");
            let engine = DuckDBEngine::new()?;
            engine.register_sample_data()?;
            tracing::info!("registered table: events (in-memory, no ingest support)");
            Ok((QueryEngine::DuckDB(engine), None))
        }
    }
}

async fn init_delta_table() -> Result<(), EngineError> {
    if std::path::Path::new(EVENTS_DELTA_PATH)
        .join("_delta_log")
        .exists()
    {
        tracing::info!("Delta table already exists at {EVENTS_DELTA_PATH}");
        return Ok(());
    }

    tracing::info!("creating Delta table at {EVENTS_DELTA_PATH} with sample data");
    let batch = create_sample_batch();

    deltalake::operations::DeltaOps::try_from_uri(EVENTS_DELTA_PATH)
        .await
        .map_err(EngineError::DeltaTable)?
        .write(vec![batch])
        .with_save_mode(deltalake::protocol::SaveMode::ErrorIfExists)
        .with_partition_columns(vec!["country"])
        .await
        .map_err(EngineError::DeltaTable)?;

    Ok(())
}

fn start_compaction_loop(settings: &CompactionSettings, cancel: CancellationToken) {
    let config = CompactionConfig {
        interval: std::time::Duration::from_secs(settings.interval_secs),
        file_count_threshold: settings.file_count_threshold,
    };

    let table_paths = vec![EVENTS_DELTA_PATH.to_string()];
    tokio::spawn(compaction_loop(table_paths, config, cancel));
}
