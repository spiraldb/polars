//! Streaming Vortex source node.
//!
//! `initialize()` is wired up: it opens the file via `VortexOpenOptions`, caches the
//! `Footer`, and exposes the schema. `begin_read()` is still a stub — the projection /
//! predicate / pre_slice → `ScanRequest` translation plus the morsel-emitting loop
//! land in subsequent sub-PRs (see PR-2-impl in the plan).

use std::sync::Arc;

use arrow::datatypes::{ArrowSchema, ArrowSchemaRef};
use async_trait::async_trait;
use polars_core::runtime::ASYNC;
use polars_core::schema::SchemaRef;
use polars_error::{PolarsResult, polars_bail, polars_err};
use polars_io::cloud::CloudOptions;
use polars_io::metrics::IOMetrics;
use polars_plan::dsl::{ScanSource, ScanSourceRef};
use polars_vortex::VortexScanOptions;
use polars_vortex::read::read_at::local_file_read_at;
use polars_vortex::read::schema::vortex_dtype_to_schema;
use polars_vortex::session::session;
use polars_vortex::vortex;
use polars_vortex::vortex::file::{Footer, OpenOptionsSessionExt, VortexFile};

use crate::async_executor::JoinHandle;
use crate::metrics::OptIOMetrics;
use crate::nodes::io_sources::multi_scan::reader_interface::output::FileReaderOutputRecv;
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
    pub arrow_schema: ArrowSchemaRef,
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
            ScanSourceRef::Path(_) => {
                polars_bail!(ComputeError:
                    "Vortex cloud reads are not yet wired up; pass a local path \
                     (cloud support is PR-5).")
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
        let row_count = vxf.row_count();

        self.init_data = Some(InitializedState {
            vxf,
            arrow_schema: arrow_schema as ArrowSchemaRef,
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
        _args: BeginReadArgs,
    ) -> PolarsResult<(FileReaderOutputRecv, JoinHandle<PolarsResult<()>>)> {
        // The scan / decode loop is the next sub-PR. `initialize()` is enough for
        // schema-only consumers (e.g. `pl.scan_vortex(...).schema` or row-count probes
        // via the trait's default `n_rows_in_file`).
        polars_bail!(ComputeError:
            "Vortex streaming reader: begin_read() not yet implemented. \
             `initialize()` (file open + schema discovery) works; the morsel-emitting \
             ScanBuilder loop is the next sub-PR.")
    }
}

// Re-export the IOMetrics import so unused-import lints are happy when this module is
// compiled standalone.
#[allow(unused_imports)]
use IOMetrics as _;
