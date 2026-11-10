use std::sync::{Arc, OnceLock};

use polars_async::executor::TaskMetricAggregator;
use polars_error::PolarsResult;
use polars_io::cloud::CloudOptions;
use polars_io::cloud::concurrency_config::FetchConfig;
use polars_io::metrics::IOMetrics;
use polars_io::utils::byte_source::{DynByteSourceBuilder, FileReadContext};
use polars_plan::dsl::ScanSource;
use polars_utils::pl_str::PlSmallStr;
use polars_vortex::{VortexScanOptions, vortex};

use super::VortexFileReader;
use crate::metrics::OptIOMetrics;
use crate::nodes::io_sources::multi_scan::reader_interface::FileReader;
use crate::nodes::io_sources::multi_scan::reader_interface::builder::FileReaderBuilder;
use crate::nodes::io_sources::multi_scan::reader_interface::capabilities::ReaderCapabilities;

pub struct VortexReaderBuilder {
    pub options: Arc<VortexScanOptions>,
    pub first_metadata: Option<Arc<vortex::file::Footer>>,
    pub io_metrics: OnceLock<Arc<IOMetrics>>,
    pub task_metrics: OnceLock<Arc<TaskMetricAggregator>>,
}

impl std::fmt::Debug for VortexReaderBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VortexReaderBuilder")
            .field("options", &self.options)
            .finish()
    }
}

impl FileReaderBuilder for VortexReaderBuilder {
    fn reader_name(&self) -> PolarsResult<PlSmallStr> {
        Ok(PlSmallStr::from_static("vortex"))
    }

    fn reader_capabilities(&self) -> PolarsResult<ReaderCapabilities> {
        use ReaderCapabilities as RC;
        Ok(RC::PRE_SLICE | RC::NEGATIVE_PRE_SLICE | RC::PARTIAL_FILTER)
    }

    fn set_execution_state(&self, execution_state: &crate::execute::StreamingExecutionState) {
        if let Some(metrics) = execution_state.task_metrics.clone() {
            let _ = self.task_metrics.set(metrics);
        }
    }

    fn set_io_metrics(&self, io_metrics: Arc<IOMetrics>) {
        let _ = self.io_metrics.set(io_metrics);
    }

    fn build_file_reader(
        &self,
        source: ScanSource,
        cloud_options: Option<Arc<CloudOptions>>,
        scan_source_idx: usize,
    ) -> PolarsResult<Box<dyn FileReader>> {
        let byte_source_builder = if source.is_cloud_url() || polars_config::config().force_async()
        {
            DynByteSourceBuilder::ObjectStore(FetchConfig::random_access())
        } else if source.is_buffer() {
            DynByteSourceBuilder::Mmap
        } else {
            DynByteSourceBuilder::FilePread(FileReadContext::from_config("VORTEX"))
        };
        Ok(Box::new(VortexFileReader {
            scan_source: source,
            cloud_options,
            options: self.options.clone(),
            footer: if scan_source_idx == 0 {
                self.first_metadata.clone()
            } else {
                None
            },
            byte_source_builder,
            io_metrics: OptIOMetrics(self.io_metrics.get().cloned()),
            task_metrics: self.task_metrics.get().cloned(),
            init_data: None,
        }))
    }
}
