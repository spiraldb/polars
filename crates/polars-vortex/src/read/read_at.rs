//! Positional reads through the Polars byte source.
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use polars_io::utils::byte_source::{ByteSource, DynByteSource};
use vortex::array::buffer::BufferHandle;
use vortex::buffer::{Alignment, ByteBuffer};
use vortex::error::{VortexResult, vortex_err};
use vortex::io::{CoalesceConfig, VortexReadAt};

use crate::session::ExecutionContext;

static REQUESTS: AtomicU64 = AtomicU64::new(0);
static REQUESTED: AtomicU64 = AtomicU64::new(0);
static RECEIVED: AtomicU64 = AtomicU64::new(0);
static BATCHES: AtomicU64 = AtomicU64::new(0);
static COLUMNS: AtomicU64 = AtomicU64::new(0);
static FILTERS: AtomicU64 = AtomicU64::new(0);

/// Process counters for isolated benchmark trials. Counts describe the adapter, not storage reads.
pub fn io_metrics() -> (u64, u64, u64, u64, u64, u64) {
    (
        REQUESTS.load(Ordering::Relaxed),
        REQUESTED.load(Ordering::Relaxed),
        RECEIVED.load(Ordering::Relaxed),
        BATCHES.load(Ordering::Relaxed),
        COLUMNS.load(Ordering::Relaxed),
        FILTERS.load(Ordering::Relaxed),
    )
}

pub fn reset_io_metrics() {
    for counter in [
        &REQUESTS, &REQUESTED, &RECEIVED, &BATCHES, &COLUMNS, &FILTERS,
    ] {
        counter.store(0, Ordering::Relaxed);
    }
}

pub fn record_batch(columns: u64) {
    BATCHES.fetch_add(1, Ordering::Relaxed);
    COLUMNS.fetch_add(columns, Ordering::Relaxed);
}

pub fn record_filter() {
    FILTERS.fetch_add(1, Ordering::Relaxed);
}

#[derive(Clone)]
pub struct PolarsVortexReadAt {
    inner: Arc<DynByteSource>,
    context: ExecutionContext,
}
impl PolarsVortexReadAt {
    pub fn new(inner: Arc<DynByteSource>) -> Self {
        Self::with_context(inner, ExecutionContext::default())
    }

    pub fn with_context(inner: Arc<DynByteSource>, context: ExecutionContext) -> Self {
        Self { inner, context }
    }
}
impl VortexReadAt for PolarsVortexReadAt {
    fn coalesce_config(&self) -> Option<CoalesceConfig> {
        Some(if self.inner.is_cloud() {
            CoalesceConfig::object_storage()
        } else {
            CoalesceConfig::file()
        })
    }
    fn concurrency(&self) -> usize {
        polars_config::config().max_threads().max(1)
    }
    fn size(&self) -> BoxFuture<'static, VortexResult<u64>> {
        let source = self.inner.clone();
        let context = self.context.clone();
        async move {
            let output = context
                .handle()
                .spawn_io(async move {
                    source
                        .get_size()
                        .await
                        .map(|n| n as u64)
                        .map_err(|e| vortex_err!("Polars source size: {e}"))
                })
                .await;
            drop(context);
            output
        }
        .boxed()
    }
    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> BoxFuture<'static, VortexResult<BufferHandle>> {
        let source = self.inner.clone();
        let context = self.context.clone();
        async move {
            // Vortex can poll its segment driver on the Polars CPU scheduler.
            // Cloud byte sources require Tokio for their network and timer futures.
            let buffer = context
                .handle()
                .spawn_io(async move {
                    let start = usize::try_from(offset)
                        .map_err(|_| vortex_err!("read offset exceeds usize"))?;
                    let end = start
                        .checked_add(length)
                        .ok_or_else(|| vortex_err!("read range overflow"))?;
                    let size = source
                        .get_size()
                        .await
                        .map_err(|e| vortex_err!("Polars source size: {e}"))?;
                    if end > size {
                        return Err(vortex_err!(
                            "Vortex read range {start}..{end} exceeds file size {size}"
                        ));
                    }
                    REQUESTS.fetch_add(1, Ordering::Relaxed);
                    REQUESTED.fetch_add(length as u64, Ordering::Relaxed);
                    source
                        .get_range(start..end)
                        .await
                        .map_err(|e| vortex_err!("Polars range read: {e}"))
                })
                .await?;
            RECEIVED.fetch_add(buffer.len() as u64, Ordering::Relaxed);
            let bytes = Bytes::from_owner(buffer);
            // Cloud responses and sliced buffers need not meet Vortex's alignment.
            // Keep the owner without a copy when its address already aligns.
            let buffer =
                ByteBuffer::from_bytes_aligned(bytes, Alignment::none()).aligned(alignment);
            drop(context);
            Ok(BufferHandle::new_host(buffer))
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use polars_async::executor::TaskMetricAggregator;
    use polars_buffer::Buffer;
    use polars_core::runtime::ASYNC;
    use polars_io::utils::byte_source::BufferByteSource;

    use super::*;

    #[test]
    fn unpolled_reads_keep_the_query_executor_alive() {
        let metrics = Arc::new(TaskMetricAggregator::default());
        let weak = Arc::downgrade(&metrics);
        let context = ExecutionContext::new(Some(metrics.clone()));
        let source = Arc::new(DynByteSource::Buffer(BufferByteSource(Buffer::from_vec(
            vec![1, 2, 3, 4],
        ))));
        let adapter = PolarsVortexReadAt::with_context(source, context.clone());
        let size = adapter.size();
        let read = adapter.read_at(1, 2, Alignment::none());
        drop(adapter);
        drop(context);
        drop(metrics);
        assert!(weak.upgrade().is_some());
        ASYNC.block_on(async {
            assert_eq!(size.await.unwrap(), 4);
            let buffer = read.await.unwrap().unwrap_host();
            assert_eq!(&buffer[..], &[2, 3]);
        });
        assert!(weak.upgrade().is_none());
    }
}
