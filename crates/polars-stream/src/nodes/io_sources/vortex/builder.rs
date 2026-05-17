//! Vortex `FileReaderBuilder` impl. See [`super`].

use std::sync::Arc;

use polars_io::cloud::CloudOptions;
use polars_io::metrics::IOMetrics;
use polars_plan::dsl::ScanSource;
use polars_vortex::read::VortexSegmentCacheRef;
use polars_vortex::vortex::expr::Expression as VortexExpression;
use polars_vortex::{VortexScanOptions, vortex};

use super::VortexFileReader;
use crate::metrics::OptIOMetrics;
use crate::nodes::io_sources::multi_scan::reader_interface::FileReader;
use crate::nodes::io_sources::multi_scan::reader_interface::builder::FileReaderBuilder;
use crate::nodes::io_sources::multi_scan::reader_interface::capabilities::ReaderCapabilities;

pub struct VortexReaderBuilder {
    pub options: Arc<VortexScanOptions>,
    pub first_metadata: Option<Arc<vortex::file::Footer>>,
    /// Resolved segment cache threaded from IR-build (`FileScanIR::Vortex::segment_cache`).
    /// When `Some`, the streaming source uses this Arc for the data read so it shares one
    /// Moka cache instance with the IR-build-time postscript read. When `None`, the
    /// streaming source falls back to `options.segment_cache.resolve()` (e.g., user-supplied
    /// schema path where no postscript read happened at IR-build).
    pub segment_cache: Option<VortexSegmentCacheRef>,
    /// AExpr-direct convertor result (PR-13.2, sole pushdown path as of PR-2.6): when
    /// the predicate translated cleanly via
    /// `polars_plan::plans::predicates::vortex_convertor::aexpr_to_vortex_expression`
    /// (the `aexpr` module is `pub(crate)`; the externally-resolvable path goes through
    /// the `pub use aexpr::*` re-export at `plans/mod.rs`),
    /// the Vortex `Expression` is captured here at IR-build time (where we still have
    /// `expr_arena` access). `VortexFileReader::begin_read` uses it directly. The
    /// multi-scan layer reapplies the full predicate post-decode regardless (we
    /// advertise `PARTIAL_FILTER`), so it is safe to push only a subset; shapes the
    /// convertor returns `None` for fall through to no-pushdown + post-decode reapply.
    pub aexpr_filter: Option<VortexExpression>,
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
        // is always safe; FULL_FILTER would require the AExpr-direct convertor to be a
        // strict superset of every AExpr predicate Polars constructs (still out of scope:
        // the convertor returns None for unhandled shapes — Sort/Gather/Filter/Agg/Ternary/
        // AnonymousFunction/Over/Rolling/temporal extracts/etc. — and the multi-scan
        // reapply handles them). EXTERNAL_FILTER_MASK would need Vortex `Selection` bitmap
        // plumbing.
        RC::ROW_INDEX
            | RC::PRE_SLICE
            | RC::NEGATIVE_PRE_SLICE
            | RC::PARTIAL_FILTER
            | RC::MAPPED_COLUMN_PROJECTION
    }

    fn set_io_metrics(&self, io_metrics: Arc<IOMetrics>) {
        // Mirrors the CSV / IPC / NDJSON sibling builders (3 of 4 consensus). Panicking
        // on a second `set` surfaces refactor regressions immediately: the multi-scan
        // layer currently invokes `set_io_metrics` exactly once per scan, and any future
        // change that calls it twice should fail loudly rather than silently use stale
        // metrics. (Parquet uses `let _ =` but is the lone outlier.)
        self.io_metrics.set(io_metrics).ok().unwrap()
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
            // Threaded resolved cache for the data read; `None` triggers fallback resolve()
            // inside `VortexFileReader::initialize`. Same pattern as `footer` above.
            segment_cache: self.segment_cache.clone(),
            // AExpr-direct convertor result (sole pushdown path as of PR-2.6). Shared
            // across all sources in a multi-source scan — the convertor result is
            // purely a function of the (predicate, schema) pair, both of which are
            // constant across the scan's sources.
            aexpr_filter: self.aexpr_filter.clone(),
            io_metrics: OptIOMetrics(self.io_metrics.get().cloned()),
            init_data: None,
        }) as _
    }
}
