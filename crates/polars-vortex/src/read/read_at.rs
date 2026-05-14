//! `PolarsInstrumentedVortexReadAt`: a thin decorator over Vortex's native
//! [`VortexReadAt`](vortex::io::VortexReadAt) implementations
//! ([`FileReadAt`](vortex::io::std_file::FileReadAt) for local files,
//! [`ObjectStoreReadAt`](vortex::io::object_store::ObjectStoreReadAt) for cloud) that
//! splices Polars' observability and concurrency budget into the I/O path.
//!
//! ## What this gives us
//!
//! 1. **`with_concurrency_budget`** — every `read_at` acquires a permit from Polars'
//!    global semaphore (`POLARS_CONCURRENCY_BUDGET`, default ~ rayon thread count).
//!    Without this wrapper, Vortex's coalescer respects its own
//!    [`concurrency()`](vortex::io::VortexReadAt::concurrency) value but is invisible to
//!    Polars' cross-format budget. By going through the budget, Vortex reads compete with
//!    Parquet reads under the same cap.
//! 2. **`IOMetrics`** — every read updates `bytes_requested`, `bytes_received`, and the
//!    `io_timer`. This is what surfaces in Polars' `EXPLAIN ... STREAMING` output and
//!    debug logs.
//! 3. **No buffer copy.** Vortex's `FileReadAt`/`ObjectStoreReadAt` already produce
//!    zero-copy `BufferHandle`s. We pass them through unchanged.

use std::sync::Arc;

use futures::FutureExt;
use futures::future::BoxFuture;
use polars_io::metrics::{IOMetrics, OptIOMetrics};
use vortex::buffer::Alignment;
use vortex::buffer::ByteBuffer;
use vortex::error::VortexResult;
use vortex::io::{CoalesceConfig, VortexReadAt};

/// A decorator over an inner [`VortexReadAt`] that adds Polars' concurrency budget and
/// I/O metrics.
pub struct PolarsInstrumentedVortexReadAt {
    inner: Arc<dyn VortexReadAt>,
    uri: Option<Arc<str>>,
    metrics: Option<Arc<IOMetrics>>,
    /// Concurrency reported via [`VortexReadAt::concurrency`]. We delegate to the inner
    /// reader's reported value so the file-vs-cloud heuristics in vortex-io still apply.
    concurrency: usize,
}

impl PolarsInstrumentedVortexReadAt {
    pub fn new(
        inner: Arc<dyn VortexReadAt>,
        uri: Option<Arc<str>>,
        metrics: Option<Arc<IOMetrics>>,
    ) -> Self {
        let concurrency = inner.concurrency();
        Self {
            inner,
            uri,
            metrics,
            concurrency,
        }
    }
}

impl VortexReadAt for PolarsInstrumentedVortexReadAt {
    fn uri(&self) -> Option<&Arc<str>> {
        self.uri.as_ref()
    }

    fn coalesce_config(&self) -> Option<CoalesceConfig> {
        self.inner.coalesce_config()
    }

    fn concurrency(&self) -> usize {
        self.concurrency
    }

    fn size(&self) -> BoxFuture<'static, VortexResult<u64>> {
        self.inner.size()
    }

    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> BoxFuture<'static, VortexResult<BufferHandle>> {
        let inner = Arc::clone(&self.inner);
        let metrics = OptIOMetrics(self.metrics.clone());
        async move {
            // Wrap the read in Polars' concurrency budget (1 permit per read) so Vortex
            // reads share the global cap with Parquet and other formats. The budget is
            // requested-budget=1; the assertion in `with_concurrency_budget` only fires
            // if you exceed the *initial* (i.e. max) budget, which 1 never does.
            polars_io::pl_async::with_concurrency_budget(1, || async {
                let len_bytes = length as u64;
                metrics
                    .record_io_read(len_bytes, inner.read_at(offset, length, alignment))
                    .await
            })
            .await
        }
        .boxed()
    }
}

// Re-export the buffer handle type so callers can use it via the polars_vortex path
// without taking a direct `vortex-array` dependency.
pub use vortex::array::buffer::BufferHandle;

/// Convenience: open a local file with [`vortex::io::std_file::FileReadAt`] using the
/// global Polars Vortex session handle, and wrap it in [`PolarsInstrumentedVortexReadAt`].
/// Cloud paths are handled in the streaming source node (where `ScanSource` is in scope).
pub fn local_file_read_at(
    path: &std::path::Path,
    io_metrics: Option<Arc<IOMetrics>>,
) -> polars_error::PolarsResult<Arc<dyn VortexReadAt>> {
    use polars_error::polars_err;
    use vortex::io::std_file::FileReadAt;

    let handle = crate::session::handle();
    let uri: Arc<str> = path.to_string_lossy().to_string().into();
    let inner = Arc::new(
        FileReadAt::open(path, handle)
            .map_err(|e| polars_err!(ComputeError: "vortex open file: {e}"))?,
    );
    Ok(Arc::new(PolarsInstrumentedVortexReadAt::new(
        inner,
        Some(uri),
        io_metrics,
    )))
}

/// Convenience: wrap an in-memory buffer in [`PolarsInstrumentedVortexReadAt`].
pub fn in_memory_read_at(
    bytes: Vec<u8>,
    uri: Option<Arc<str>>,
    io_metrics: Option<Arc<IOMetrics>>,
) -> Arc<dyn VortexReadAt> {
    let inner = Arc::new(ByteBuffer::from(bytes));
    Arc::new(PolarsInstrumentedVortexReadAt::new(inner, uri, io_metrics))
}

/// Build a cloud-backed [`VortexReadAt`] for the given path, going through Polars'
/// `polars_io::cloud::build_object_store` (so `CloudOptions` — auth, retry, region
/// overrides, etc. — are honored) and wrapping the resulting `ObjectStore` with
/// `vortex::io::object_store::ObjectStoreReadAt`.
#[cfg(feature = "cloud")]
pub async fn cloud_read_at(
    path: polars_utils::pl_path::PlRefPath,
    cloud_options: Option<&polars_io::cloud::CloudOptions>,
    io_metrics: Option<Arc<IOMetrics>>,
) -> polars_error::PolarsResult<Arc<dyn VortexReadAt>> {
    use vortex::io::object_store::ObjectStoreReadAt;

    let handle = crate::session::handle();
    let uri_str: Arc<str> = path.as_str().to_string().into();
    let (location, polars_store) =
        polars_io::cloud::build_object_store(path, cloud_options, false).await?;
    let store = polars_store.to_dyn_object_store().await.into_owned();

    // Vortex uses `object_store::path::Path` internally; construct from the location's
    // `prefix` (the full key, since `glob=false`).
    let object_path = ::object_store::path::Path::from(location.prefix.as_str());
    let inner = Arc::new(ObjectStoreReadAt::new(store, object_path, handle));

    let _ = uri_str; // silence "unused if metrics is None" for clarity
    Ok(Arc::new(PolarsInstrumentedVortexReadAt::new(
        inner,
        Some(uri_str),
        io_metrics,
    )))
}
