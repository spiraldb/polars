//! Vortex `FileReaderBuilder` impl. See [`super`].

use std::sync::Arc;

use polars_io::cloud::CloudOptions;
use polars_io::metrics::IOMetrics;
use polars_plan::dsl::ScanSource;
use polars_vortex::{VortexScanOptions, vortex};

use super::VortexFileReader;
use crate::metrics::OptIOMetrics;
use crate::nodes::io_sources::multi_scan::reader_interface::FileReader;
use crate::nodes::io_sources::multi_scan::reader_interface::builder::FileReaderBuilder;
use crate::nodes::io_sources::multi_scan::reader_interface::capabilities::ReaderCapabilities;

pub struct VortexReaderBuilder {
    pub options: Arc<VortexScanOptions>,
    pub first_metadata: Option<Arc<vortex::file::Footer>>,
    pub io_metrics: std::sync::OnceLock<Arc<IOMetrics>>,
}

impl std::fmt::Debug for VortexReaderBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VortexReaderBuilder")
            .field("options", &self.options)
            .finish()
    }
}

impl FileReaderBuilder for VortexReaderBuilder {
    fn reader_name(&self) -> &str {
        "vortex"
    }

    fn reader_capabilities(&self) -> ReaderCapabilities {
        use ReaderCapabilities as RC;
        // The multi-scan layer reapplies the full predicate post-decode, so PARTIAL_FILTER
        // is always safe; FULL_FILTER would require the convertor to consume every shape
        // it sees (out-of-scope while we lean on `SpecializedColumnPredicate`).
        // EXTERNAL_FILTER_MASK would need Vortex `Selection` bitmap plumbing.
        RC::ROW_INDEX
            | RC::PRE_SLICE
            | RC::NEGATIVE_PRE_SLICE
            | RC::PARTIAL_FILTER
            | RC::MAPPED_COLUMN_PROJECTION
    }

    fn set_io_metrics(&self, io_metrics: Arc<IOMetrics>) {
        let _ = self.io_metrics.set(io_metrics);
    }

    fn build_file_reader(
        &self,
        source: ScanSource,
        cloud_options: Option<Arc<CloudOptions>>,
        scan_source_idx: usize,
    ) -> Box<dyn FileReader> {
        Box::new(VortexFileReader {
            scan_source: source,
            cloud_options,
            options: self.options.clone(),
            // Only the first source uses the cached footer (mirroring Parquet's pattern).
            footer: (scan_source_idx == 0)
                .then(|| self.first_metadata.clone())
                .flatten(),
            io_metrics: OptIOMetrics(self.io_metrics.get().cloned()),
            init_data: None,
        }) as _
    }
}
