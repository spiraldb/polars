//! Streaming Vortex source node.
//!
//! `initialize()` is wired up: it opens the file via `VortexOpenOptions`, caches the
//! `Footer`, and exposes the schema. `begin_read()` is still a stub — the projection /
//! predicate / pre_slice → `ScanRequest` translation plus the morsel-emitting loop
//! land in subsequent sub-PRs (see PR-2-impl in the plan).

use std::sync::Arc;

use arrow::datatypes::{ArrowSchema, ArrowSchemaRef, ArrowDataType};
use async_trait::async_trait;
use futures::StreamExt;
use polars_core::runtime::ASYNC;
use polars_core::schema::SchemaRef;
use polars_error::{PolarsResult, polars_bail, polars_err};
use polars_io::cloud::CloudOptions;
use polars_io::metrics::IOMetrics;
use polars_plan::dsl::{ScanSource, ScanSourceRef};
use polars_utils::slice_enum::Slice;
use polars_vortex::VortexScanOptions;
use polars_vortex::read::array_bridge::{
    arrow_dtypes_from_schema, record_batch_to_dataframe,
};
use polars_vortex::read::predicate::polars_to_vortex_predicate;
use polars_vortex::read::read_at::local_file_read_at;
use polars_vortex::read::schema::vortex_dtype_to_schema;
use polars_vortex::session::{handle as vortex_handle, session};
use polars_vortex::vortex;
use polars_vortex::read::array_bridge::ArrowUpstreamSchema;
use polars_vortex::vortex::array::ArrayRef as VortexArrayRef;
use polars_vortex::vortex::array::VortexSessionExecute;
use polars_vortex::vortex::array::arrow::ArrowArrayExecutor;
use polars_vortex::vortex::array::stream::ArrayStreamExt;
use polars_vortex::vortex::file::{Footer, OpenOptionsSessionExt, VortexFile};

use crate::async_executor::{self, JoinHandle};
use crate::metrics::OptIOMetrics;
use crate::morsel::{Morsel, MorselSeq, SourceToken};
use crate::nodes::TaskPriority;
use crate::nodes::io_sources::multi_scan::reader_interface::output::{
    FileReaderOutputRecv, FileReaderOutputSend,
};
use crate::nodes::io_sources::multi_scan::reader_interface::{BeginReadArgs, FileReader};

pub mod builder;

pub struct VortexFileReader {
    pub scan_source: ScanSource,
    pub cloud_options: Option<Arc<CloudOptions>>,
    pub options: Arc<VortexScanOptions>,
    pub footer: Option<Arc<Footer>>,
    pub io_metrics: OptIOMetrics,

    /// Set by `initialize()`.
    pub init_data: Option<InitializedState>,
}

/// State produced by [`VortexFileReader::initialize`].
pub struct InitializedState {
    pub vxf: VortexFile,
    /// polars-arrow schema (Polars' internal fork).
    pub arrow_schema: ArrowSchemaRef,
    /// Per-column polars-arrow `ArrowDataType` — cached so we can hand it to the bridge
    /// without re-deriving on every batch.
    pub arrow_dtypes: Vec<ArrowDataType>,
    /// Upstream `arrow_schema::Schema` — needed to drive `execute_record_batch` (Vortex
    /// emits upstream-Arrow types).
    pub upstream_schema: Arc<ArrowUpstreamSchema>,
    pub pl_schema: SchemaRef,
    pub row_count: u64,
}

impl VortexFileReader {
    /// Build the underlying `VortexReadAt` based on the scan source. Local files go
    /// through `polars-vortex`'s `local_file_read_at`. Cloud paths are PR-5.
    async fn build_read_at(
        &self,
    ) -> PolarsResult<Arc<dyn vortex::io::VortexReadAt>> {
        let io_metrics = self.io_metrics.0.clone();
        match self.scan_source.as_scan_source_ref() {
            ScanSourceRef::Path(path) if !path.has_scheme() => {
                let path = path.as_std_path().to_path_buf();
                tokio::task::spawn_blocking(move || local_file_read_at(&path, io_metrics))
                    .await
                    .map_err(|e| polars_err!(ComputeError: "spawn_blocking failed: {e}"))?
            }
            #[cfg(feature = "cloud")]
            ScanSourceRef::Path(path) => {
                let cloud_opts = self.cloud_options.as_deref();
                polars_vortex::read::read_at::cloud_read_at(
                    path.clone(),
                    cloud_opts,
                    io_metrics,
                )
                .await
            }
            #[cfg(not(feature = "cloud"))]
            ScanSourceRef::Path(_) => {
                polars_bail!(ComputeError:
                    "Vortex was built without the `cloud` feature; rebuild Polars with \
                     `--features vortex,cloud` to enable S3/GCS/Azure reads.")
            }
            ScanSourceRef::Buffer(buf) => {
                let bytes = buf.as_slice().to_vec();
                let uri: Arc<str> = self.scan_source.as_scan_source_ref().to_include_path_name().into();
                Ok(polars_vortex::read::read_at::in_memory_read_at(
                    bytes,
                    Some(uri),
                    io_metrics,
                ))
            }
            ScanSourceRef::File(_) => {
                polars_bail!(ComputeError:
                    "Vortex reads from open File handles are not yet supported; \
                     pass a path or in-memory buffer instead.")
            }
        }
    }
}

#[async_trait]
impl FileReader for VortexFileReader {
    async fn initialize(&mut self) -> PolarsResult<()> {
        if self.init_data.is_some() {
            return Ok(());
        }

        // Touch the global session to ensure the runtime handle is installed before any
        // Vortex async work is spawned.
        let session = session();

        let read_at = self.build_read_at().await?;

        let mut open_opts = session.open_options();
        if let Some(footer) = self.footer.clone() {
            // First-file fast path: reuse the cached footer to avoid the postscript read.
            open_opts = open_opts.with_footer(Arc::unwrap_or_clone(footer));
        }
        if let Some(n) = self.options.initial_read_size {
            open_opts = open_opts.with_initial_read_size(n);
        }
        // Attach the process-global segment cache so successive scans of the same file
        // can reuse decompressed segments.
        let segment_cache = polars_vortex::session::segment_cache();
        open_opts = open_opts.with_segment_cache(segment_cache);

        let vxf = ASYNC
            .spawn(async move {
                open_opts
                    .open(read_at)
                    .await
                    .map_err(|e| polars_err!(ComputeError: "vortex open: {e}"))
            })
            .await
            .map_err(|e| polars_err!(ComputeError: "tokio spawn join: {e}"))??;

        let (pl_schema, arrow_schema) = vortex_dtype_to_schema(vxf.dtype())?;
        let arrow_dtypes = arrow_dtypes_from_schema(arrow_schema.as_ref());
        let upstream_schema = Arc::new(
            vxf.dtype()
                .to_arrow_schema()
                .map_err(|e| polars_err!(ComputeError: "vortex DType -> upstream arrow schema: {e}"))?,
        );
        let row_count = vxf.row_count();

        self.init_data = Some(InitializedState {
            vxf,
            arrow_schema: arrow_schema as ArrowSchemaRef,
            arrow_dtypes,
            upstream_schema,
            pl_schema,
            row_count,
        });
        Ok(())
    }

    async fn file_schema(&mut self) -> PolarsResult<SchemaRef> {
        self.initialize().await?;
        Ok(self.init_data.as_ref().expect("initialized").pl_schema.clone())
    }

    async fn file_arrow_schema(&mut self) -> PolarsResult<Option<ArrowSchemaRef>> {
        self.initialize().await?;
        Ok(Some(
            self.init_data
                .as_ref()
                .expect("initialized")
                .arrow_schema
                .clone(),
        ))
    }

    async fn fast_n_rows_in_file(&mut self) -> PolarsResult<Option<polars_utils::IdxSize>> {
        self.initialize().await?;
        let n = self.init_data.as_ref().expect("initialized").row_count;
        Ok(Some(
            polars_utils::IdxSize::try_from(n).unwrap_or(polars_utils::IdxSize::MAX),
        ))
    }

    fn begin_read(
        &mut self,
        args: BeginReadArgs,
    ) -> PolarsResult<(FileReaderOutputRecv, JoinHandle<PolarsResult<()>>)> {
        let st = self
            .init_data
            .as_ref()
            .expect("VortexFileReader::begin_read called before initialize()");

        // Clone the per-batch state into the spawned task. `VortexFile` is cheaply
        // cloneable (it's effectively an Arc-wrapped open file plus metadata).
        let vxf = st.vxf.clone();
        let arrow_dtypes = st.arrow_dtypes.clone();
        let upstream_schema = Arc::clone(&st.upstream_schema);
        let pl_schema = st.pl_schema.clone();

        // Apply pre_slice as a row range. Negative slices get a cheap `row_count()`
        // probe (the file's row count is in the cached footer — free) and translate
        // to positive via `restrict_to_bounds`.
        let row_range = args.pre_slice.as_ref().map(|slice| {
            let positive = match slice {
                Slice::Positive { .. } => slice.clone(),
                Slice::Negative { .. } => slice.clone().restrict_to_bounds(st.row_count as usize),
            };
            match positive {
                Slice::Positive { offset, len } => {
                    let start = offset as u64;
                    let end = start.saturating_add(len as u64);
                    start..end
                }
                Slice::Negative { .. } => unreachable!("restrict_to_bounds always returns Positive"),
            }
        });

        // Translate the pushable bits of args.predicate into a Vortex `Expression`. We
        // advertise `PARTIAL_FILTER` capability, so the multi-scan layer keeps the
        // original predicate around to apply post-decode — pushing only what we can
        // convert is safe (over-conservative pushdown would drop rows incorrectly).
        let filter_expr = if self.options.push_predicate {
            args.predicate.as_ref().and_then(polars_to_vortex_predicate)
        } else {
            None
        };

        let (mut tx, rx) = FileReaderOutputSend::new_serial();

        // Spawn the decode loop on the streaming async executor (Low priority — I/O work,
        // not on the critical path for the next pipeline operator).
        let handle = async_executor::spawn(TaskPriority::Low, async move {
            // Touch the session so its runtime is installed before any Vortex spawns.
            let session_ref = session();
            let _ = vortex_handle();

            // Build the scan.
            let mut scan = vxf
                .scan()
                .map_err(|e| polars_err!(ComputeError: "vortex scan: {e}"))?;
            if let Some(range) = row_range {
                scan = scan.with_row_range(range);
            }
            if let Some(filter) = filter_expr {
                scan = scan.with_filter(filter);
            }

            let stream = scan
                .into_array_stream()
                .map_err(|e| polars_err!(ComputeError: "vortex into_array_stream: {e}"))?;

            futures::pin_mut!(stream);
            let mut seq = MorselSeq::default();
            let source_token = SourceToken::default();

            while let Some(array_res) = stream.next().await {
                let array: VortexArrayRef = array_res
                    .map_err(|e| polars_err!(ComputeError: "vortex stream item: {e}"))?;
                // Convert vortex ArrayRef -> upstream RecordBatch via ArrowArrayExecutor.
                let mut ctx = session_ref.create_execution_ctx();
                let record_batch = array
                    .execute_record_batch(upstream_schema.as_ref(), &mut ctx)
                    .map_err(|e| polars_err!(ComputeError:
                        "vortex execute_record_batch: {e}"))?;
                // Bridge upstream RecordBatch -> Polars DataFrame via the C ABI.
                let df =
                    record_batch_to_dataframe(record_batch, &pl_schema, &arrow_dtypes)?;
                let morsel = Morsel::new(df, seq, source_token.clone());
                seq = seq.successor();
                if tx.send_morsel(morsel).await.is_err() {
                    // Downstream stopped consuming — drop the stream to abort any
                    // in-flight Vortex tasks.
                    break;
                }
            }
            Ok(())
        });

        Ok((rx, handle))
    }
}

// Re-export the IOMetrics import so unused-import lints are happy when this module is
// compiled standalone.
#[allow(unused_imports)]
use IOMetrics as _;
