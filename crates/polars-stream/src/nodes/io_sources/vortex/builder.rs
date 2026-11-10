use std::sync::{Arc, Mutex, OnceLock};

use polars_async::executor::TaskMetricAggregator;
use polars_async::primitives::wait_group::WaitGroup;
use polars_error::PolarsResult;
use polars_io::cloud::CloudOptions;
use polars_io::cloud::concurrency_config::FetchConfig;
use polars_io::metrics::IOMetrics;
use polars_io::utils::byte_source::{DynByteSourceBuilder, FileReadContext};
use polars_plan::dsl::ScanSource;
use polars_utils::pl_str::PlSmallStr;
use polars_vortex::session::ExecutionContext;
use polars_vortex::{VortexScanOptions, vortex};

use super::{PrefetchSync, VortexFileReader};
use crate::metrics::OptIOMetrics;
use crate::nodes::io_sources::multi_scan::reader_interface::FileReader;
use crate::nodes::io_sources::multi_scan::reader_interface::builder::FileReaderBuilder;
use crate::nodes::io_sources::multi_scan::reader_interface::capabilities::ReaderCapabilities;
use crate::nodes::io_sources::shared::pipeline_budget::{
    ActiveScan, PipelineBudget, env_nonzero_usize,
};

pub struct VortexReaderBuilder {
    pub options: Arc<VortexScanOptions>,
    pub first_metadata: Option<Arc<vortex::file::Footer>>,
    pub aexpr_filter: Option<(vortex::expr::Expression, polars_core::schema::SchemaRef)>,
    pub io_metrics: OnceLock<Arc<IOMetrics>>,
    pub task_metrics: OnceLock<Arc<TaskMetricAggregator>>,
    pub execution_context: OnceLock<ExecutionContext>,
    pub pipeline_budget: OnceLock<PipelineBudget>,
    pub shared_prefetch_wait_group_slot: Arc<Mutex<Option<WaitGroup>>>,
    pub file_read_context: OnceLock<FileReadContext>,
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
        self.execution_context
            .set(ExecutionContext::new(execution_state.task_metrics.clone()))
            .unwrap();
        self.pipeline_budget
            .set(PipelineBudget::new(
                env_nonzero_usize("POLARS_VORTEX_PREFETCH_SIZE"),
                env_nonzero_usize("POLARS_VORTEX_PREFETCH_KBYTES_BUDGET"),
                execution_state.num_pipelines,
            ))
            .unwrap();
    }

    fn active_scan(&self) -> Option<ActiveScan> {
        self.pipeline_budget.get().map(PipelineBudget::active_scan)
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
            DynByteSourceBuilder::FilePread(
                self.file_read_context
                    .get_or_init(|| FileReadContext::from_config("VORTEX"))
                    .clone(),
            )
        };
        Ok(Box::new(VortexFileReader {
            scan_source: source,
            cloud_options,
            options: self.options.clone(),
            aexpr_filter: self.aexpr_filter.clone(),
            footer: if scan_source_idx == 0 {
                self.first_metadata.clone()
            } else {
                None
            },
            byte_source_builder,
            io_metrics: OptIOMetrics(self.io_metrics.get().cloned()),
            task_metrics: self.task_metrics.get().cloned(),
            execution_context: self.execution_context.get().unwrap().clone(),
            prefetch_sync: PrefetchSync {
                budget: self.pipeline_budget.get().unwrap().clone(),
                shared_slot: Arc::clone(&self.shared_prefetch_wait_group_slot),
                previous: None,
                all_spawned: None,
            },
            init_data: None,
        }))
    }
}
