use deltalake::operations::DeltaOps;
use tokio_util::sync::CancellationToken;

use crate::query_engine::{EngineError, EngineResult};

pub struct CompactionConfig {
    pub interval: std::time::Duration,
    pub file_count_threshold: usize,
}

pub async fn optimize_table(table_path: &str) -> EngineResult<CompactionResult> {
    let table = deltalake::open_table(table_path)
        .await
        .map_err(EngineError::DeltaTable)?;

    let file_count = table.get_files_iter()?.count();

    let (table, _metrics) = DeltaOps(table)
        .optimize()
        .await
        .map_err(EngineError::DeltaTable)?;

    let new_file_count = table.get_files_iter()?.count();

    Ok(CompactionResult {
        files_before: file_count,
        files_after: new_file_count,
        version: table.version(),
    })
}

pub async fn vacuum_table(
    table_path: &str,
    retention_hours: Option<u64>,
) -> EngineResult<VacuumResult> {
    let table = deltalake::open_table(table_path)
        .await
        .map_err(EngineError::DeltaTable)?;

    let mut vacuum_op = DeltaOps(table).vacuum();
    if let Some(hours) = retention_hours {
        vacuum_op = vacuum_op.with_retention_period(chrono::Duration::hours(hours as i64));
        vacuum_op = vacuum_op.with_enforce_retention_duration(false);
    }

    let (table, metrics) = vacuum_op.await.map_err(EngineError::DeltaTable)?;

    Ok(VacuumResult {
        files_deleted: metrics.files_deleted.len(),
        files_remaining: table.get_files_iter().map(|f| f.count()).unwrap_or(0),
    })
}

/// Run compaction loop in the background until cancelled.
pub async fn compaction_loop(
    table_paths: Vec<String>,
    config: CompactionConfig,
    cancel: CancellationToken,
) {
    tracing::info!(
        interval_secs = config.interval.as_secs(),
        threshold = config.file_count_threshold,
        tables = ?table_paths,
        "compaction loop started"
    );

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!("compaction loop stopping");
                return;
            }
            _ = tokio::time::sleep(config.interval) => {
                for path in &table_paths {
                    match check_and_compact(path, config.file_count_threshold).await {
                        Ok(Some(result)) => {
                            tracing::info!(
                                table = path,
                                before = result.files_before,
                                after = result.files_after,
                                version = result.version,
                                "compaction completed"
                            );
                        }
                        Ok(None) => {
                            tracing::debug!(table = path, "no compaction needed");
                        }
                        Err(e) => {
                            tracing::error!(table = path, error = %e, "compaction failed");
                        }
                    }
                }
            }
        }
    }
}

async fn check_and_compact(
    table_path: &str,
    threshold: usize,
) -> EngineResult<Option<CompactionResult>> {
    let table = deltalake::open_table(table_path)
        .await
        .map_err(EngineError::DeltaTable)?;

    let file_count = table.get_files_iter()?.count();

    if file_count <= threshold {
        return Ok(None);
    }

    tracing::info!(
        table = table_path,
        file_count,
        threshold,
        "compaction triggered"
    );

    let result = optimize_table(table_path).await?;
    Ok(Some(result))
}

pub struct CompactionResult {
    pub files_before: usize,
    pub files_after: usize,
    pub version: i64,
}

pub struct VacuumResult {
    pub files_deleted: usize,
    pub files_remaining: usize,
}
