//! Options for `pl.scan_vortex` / `pl.read_vortex`.

use std::num::NonZeroUsize;

use polars_core::schema::SchemaRef;

/// Read-side options for a Vortex scan. Lives in [`polars_plan::dsl::FileScanIR::Vortex`].
///
/// Kept compact (the IR enum has a `size_of <= 80` assertion).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "dsl-schema", derive(schemars::JsonSchema))]
pub struct VortexScanOptions {
    /// User-provided schema. If supplied, schema inference at DSL→IR conversion is skipped.
    pub schema: Option<SchemaRef>,
    /// Whether to populate `UnifiedScanArgs::table_statistics` from the file's footer stats,
    /// enabling whole-file pruning at the optimizer level.
    pub use_statistics: bool,
    /// If true (default), translate Polars predicates to Vortex `Expression` and push them as
    /// the scan filter.
    pub push_predicate: bool,
    /// If true (default), push column projection as a Vortex `pack(...)` projection expression.
    pub push_projection: bool,
    /// Initial postscript read size, passed to `VortexOpenOptions::with_initial_read_size`.
    pub initial_read_size: Option<usize>,
    /// Per-file scan concurrency, passed to `ScanBuilder::with_concurrency`. Defaults to a value
    /// derived from `num_pipelines`.
    pub scan_concurrency: Option<NonZeroUsize>,
    /// Segment cache mode for this scan.
    pub cache: VortexCacheMode,
    /// Turn on additional convertor coverage (e.g. temporal extracts, list ops). Off by default
    /// to keep the convertor surface predictable.
    pub aggressive_pushdown: bool,
}

/// Controls how the Vortex segment cache is wired up per-scan.
///
/// Default is [`VortexCacheMode::Global`] — the process-wide cache (sized via
/// `POLARS_VORTEX_CACHE_BYTES`, default 512 MiB). Vortex's segment cache stores decompressed
/// segments across queries on the same file, which is one of its biggest perf wins over Parquet.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "dsl-schema", derive(schemars::JsonSchema))]
pub enum VortexCacheMode {
    /// Use the process-global segment cache.
    #[default]
    Global,
    /// Use a no-op cache — segments are not retained across queries.
    Off,
    /// Build a fresh per-scan cache of the given byte budget.
    Dedicated(u64),
}
