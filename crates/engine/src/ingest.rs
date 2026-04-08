use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use deltalake::operations::DeltaOps;
use deltalake::protocol::SaveMode;
use tokio::sync::mpsc;

use crate::query_engine::{EngineError, EngineResult};

/// Channel-based ingest buffer. Producers send RecordBatches lock-free via the
/// channel sender. The flush task receives, coalesces into one large batch, and
/// writes to Delta Lake.
pub struct IngestSender {
    schema: SchemaRef,
    sender: mpsc::Sender<RecordBatch>,
    pending_rows: AtomicUsize,
    partition_columns: Vec<String>,
    max_batch_rows: usize,
}

pub struct FlushResult {
    pub rows_flushed: usize,
    pub batches_coalesced: usize,
}

pub fn create_ingest_channel(
    schema: SchemaRef,
    capacity: usize,
    partition_columns: Vec<String>,
    max_batch_rows: usize,
) -> (Arc<IngestSender>, IngestReceiver) {
    let (sender, receiver) = mpsc::channel(capacity);
    let ingest_sender = Arc::new(IngestSender {
        schema,
        sender,
        pending_rows: AtomicUsize::new(0),
        partition_columns,
        max_batch_rows,
    });
    let ingest_receiver = IngestReceiver { receiver };
    (ingest_sender, ingest_receiver)
}

impl IngestSender {
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn partition_columns(&self) -> &[String] {
        &self.partition_columns
    }

    pub async fn send(&self, batch: RecordBatch) -> EngineResult<()> {
        if batch.schema() != self.schema {
            return Err(EngineError::Table(format!(
                "schema mismatch: expected {:?}, got {:?}",
                self.schema,
                batch.schema()
            )));
        }

        if batch.num_rows() > self.max_batch_rows {
            return Err(EngineError::Table(format!(
                "batch too large: {} rows exceeds max of {}",
                batch.num_rows(),
                self.max_batch_rows
            )));
        }

        let row_count = batch.num_rows();
        self.sender.send(batch).await.map_err(|_| {
            EngineError::Table("ingest channel closed — flush task may have stopped".to_string())
        })?;
        self.pending_rows.fetch_add(row_count, Ordering::Relaxed);

        Ok(())
    }

    pub fn pending_row_count(&self) -> usize {
        self.pending_rows.load(Ordering::Relaxed)
    }
}

pub struct IngestReceiver {
    receiver: mpsc::Receiver<RecordBatch>,
}

impl IngestReceiver {
    fn drain(&mut self) -> Vec<RecordBatch> {
        let mut batches = Vec::new();
        while let Ok(batch) = self.receiver.try_recv() {
            batches.push(batch);
        }
        batches
    }
}

fn coalesce_batches(schema: &SchemaRef, batches: &[RecordBatch]) -> EngineResult<RecordBatch> {
    concat_batches(schema, batches)
        .map_err(|e| EngineError::Table(format!("failed to coalesce batches: {e}")))
}

async fn write_to_delta(
    table_path: &str,
    batch: RecordBatch,
    partition_columns: &[String],
) -> EngineResult<()> {
    let mut write_op = DeltaOps::try_from_uri(table_path)
        .await
        .map_err(EngineError::DeltaTable)?
        .write(vec![batch])
        .with_save_mode(SaveMode::Append);

    if !partition_columns.is_empty() {
        write_op = write_op.with_partition_columns(partition_columns.to_vec());
    }

    write_op.await.map_err(EngineError::DeltaTable)?;
    Ok(())
}

pub async fn flush_loop(
    sender: Arc<IngestSender>,
    mut receiver: IngestReceiver,
    table_path: String,
    interval: std::time::Duration,
    cancel: tokio_util::sync::CancellationToken,
) {
    tracing::info!(
        interval_secs = interval.as_secs(),
        channel_capacity = sender.pending_row_count(),
        "flush loop started"
    );

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!("flush loop stopping, draining remaining batches");
                flush_once(&sender, &mut receiver, &table_path).await;
                return;
            }
            _ = tokio::time::sleep(interval) => {
                flush_once(&sender, &mut receiver, &table_path).await;
            }
        }
    }
}

async fn flush_once(sender: &IngestSender, receiver: &mut IngestReceiver, table_path: &str) {
    let batches = receiver.drain();
    if batches.is_empty() {
        return;
    }

    let batch_count = batches.len();
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

    tracing::info!(
        rows = row_count,
        batches_received = batch_count,
        "coalescing and flushing to Delta"
    );

    match coalesce_batches(&sender.schema(), &batches) {
        Ok(coalesced) => {
            tracing::info!(
                coalesced_rows = coalesced.num_rows(),
                "batch coalesced from {} batches into 1",
                batch_count
            );

            match write_to_delta(table_path, coalesced, sender.partition_columns()).await {
                Ok(()) => {
                    sender.pending_rows.fetch_sub(row_count, Ordering::Relaxed);
                    tracing::info!(rows = row_count, "flush complete");
                }
                Err(e) => {
                    tracing::error!(error = %e, "flush to Delta failed");
                }
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "batch coalescing failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn test_batch(ids: Vec<i64>, names: Vec<&str>) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn send_and_drain() {
        let (sender, mut receiver) = create_ingest_channel(test_schema(), 100, vec![], 10_000);
        sender
            .send(test_batch(vec![1, 2], vec!["a", "b"]))
            .await
            .unwrap();
        sender.send(test_batch(vec![3], vec!["c"])).await.unwrap();

        assert_eq!(sender.pending_row_count(), 3);

        let batches = receiver.drain();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[1].num_rows(), 1);
    }

    #[tokio::test]
    async fn send_rejects_wrong_schema() {
        let (sender, _receiver) = create_ingest_channel(test_schema(), 100, vec![], 10_000);
        let wrong_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        assert!(sender.send(wrong_batch).await.is_err());
    }

    #[test]
    fn coalesce_merges_batches() {
        let schema = test_schema();
        let b1 = test_batch(vec![1, 2], vec!["a", "b"]);
        let b2 = test_batch(vec![3], vec!["c"]);
        let coalesced = coalesce_batches(&schema, &[b1, b2]).unwrap();
        assert_eq!(coalesced.num_rows(), 3);
    }

    #[test]
    fn empty_drain_returns_nothing() {
        let (_sender, mut receiver) = create_ingest_channel(test_schema(), 100, vec![], 10_000);
        let batches = receiver.drain();
        assert!(batches.is_empty());
    }

    #[tokio::test]
    async fn send_rejects_batch_exceeding_max_rows() {
        let (sender, _receiver) = create_ingest_channel(test_schema(), 100, vec![], 2);
        let batch = test_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
        let err = sender.send(batch).await.unwrap_err();
        assert!(err.to_string().contains("batch too large"));
    }

    #[tokio::test]
    async fn send_accepts_batch_within_max_rows() {
        let (sender, _receiver) = create_ingest_channel(test_schema(), 100, vec![], 2);
        let batch = test_batch(vec![1, 2], vec!["a", "b"]);
        sender.send(batch).await.unwrap();
        assert_eq!(sender.pending_row_count(), 2);
    }

    #[tokio::test]
    async fn multiple_sends_coalesce_on_drain() {
        let (sender, mut receiver) = create_ingest_channel(test_schema(), 100, vec![], 10_000);
        for i in 0..5 {
            sender.send(test_batch(vec![i], vec!["x"])).await.unwrap();
        }

        let batches = receiver.drain();
        assert_eq!(batches.len(), 5);

        let schema = test_schema();
        let coalesced = coalesce_batches(&schema, &batches).unwrap();
        assert_eq!(coalesced.num_rows(), 5);
    }

    #[tokio::test]
    async fn drain_clears_channel() {
        let (sender, mut receiver) = create_ingest_channel(test_schema(), 100, vec![], 10_000);
        sender.send(test_batch(vec![1], vec!["a"])).await.unwrap();

        let first = receiver.drain();
        assert_eq!(first.len(), 1);

        let second = receiver.drain();
        assert!(second.is_empty());
    }
}
