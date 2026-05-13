//! Streaming Vortex source node.
//!
//! TODO(vortex): this is a skeleton FileReader that compiles and slots into `lower_ir.rs`.
//! The actual read loop — VortexOpenOptions::open → ScanBuilder → morsel emission — is
//! filled in by a follow-up PR. See [`crate::nodes::io_sources::parquet`] for the model.

use std::sync::Arc;

use async_trait::async_trait;
use polars_error::{PolarsResult, polars_bail};
use polars_io::cloud::CloudOptions;
use polars_plan::dsl::ScanSource;
use polars_vortex::VortexScanOptions;
use polars_vortex::vortex;

use crate::async_executor::JoinHandle;
use crate::metrics::OptIOMetrics;
use crate::nodes::io_sources::multi_scan::reader_interface::output::FileReaderOutputRecv;
use crate::nodes::io_sources::multi_scan::reader_interface::{BeginReadArgs, FileReader};

pub mod builder;

pub struct VortexFileReader {
    pub scan_source: ScanSource,
    pub cloud_options: Option<Arc<CloudOptions>>,
    pub options: Arc<VortexScanOptions>,
    pub footer: Option<Arc<vortex::file::Footer>>,
    pub io_metrics: OptIOMetrics,
}

#[async_trait]
impl FileReader for VortexFileReader {
    async fn initialize(&mut self) -> PolarsResult<()> {
        // TODO(vortex): open the file via VortexOpenOptions, cache footer + dtype.
        polars_bail!(ComputeError:
            "Vortex streaming reader: initialize() not yet implemented. \
             The integration scaffolding is in place but the open/scan loop is pending.")
    }

    fn begin_read(
        &mut self,
        _args: BeginReadArgs,
    ) -> PolarsResult<(FileReaderOutputRecv, JoinHandle<PolarsResult<()>>)> {
        // TODO(vortex): translate args.projection / args.predicate / args.pre_slice into a
        // Vortex ScanRequest; drive `VortexFile::scan().into_stream()` and emit morsels.
        polars_bail!(ComputeError:
            "Vortex streaming reader: begin_read() not yet implemented.")
    }
}
