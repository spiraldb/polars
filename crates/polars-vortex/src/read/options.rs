//! Options for `pl.scan_vortex` / `pl.read_vortex`.

use std::num::NonZeroUsize;
use std::sync::Arc;

use polars_core::schema::SchemaRef;
use vortex::layout::segments::SegmentCache;

/// Read-side options for a Vortex scan. Lives in [`polars_plan::dsl::FileScanIR::Vortex`].
///
/// Kept compact (the IR enum has a `size_of <= 80` assertion).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "dsl-schema", derive(schemars::JsonSchema))]
pub struct VortexScanOptions {
    /// User-provided schema. If supplied, schema inference at DSL→IR conversion is skipped.
    pub schema: Option<SchemaRef>,
    /// If true (default), translate Polars predicates to Vortex `Expression` and push them as
    /// the scan filter.
    pub push_predicate: bool,
    /// Initial postscript read size, passed to `VortexOpenOptions::with_initial_read_size`.
    pub initial_read_size: Option<usize>,
    /// Per-file scan concurrency, passed to `ScanBuilder::with_concurrency`. `None` lets
    /// Vortex pick a default based on the layout's natural splits.
    pub scan_concurrency: Option<NonZeroUsize>,
    /// Segment cache mode for this scan. Named `segment_cache` (not `cache`) to
    /// avoid collision with the LazyFrame query cache (`ScanArgsVortex::cache: bool`).
    pub segment_cache: VortexCacheMode,
}

impl Default for VortexScanOptions {
    fn default() -> Self {
        Self {
            schema: None,
            push_predicate: true,
            initial_read_size: None,
            scan_concurrency: None,
            segment_cache: VortexCacheMode::default(),
        }
    }
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

impl VortexCacheMode {
    /// Resolve this mode into the concrete [`SegmentCache`] the scan should use.
    ///
    /// `Global` returns the shared process-wide cache (a cheap `Arc` clone);
    /// `Off` returns a fresh `NoOpSegmentCache`; `Dedicated(N)` allocates a fresh
    /// `MokaSegmentCache::new(N)` scoped to this scan.
    pub fn resolve(self) -> Arc<dyn SegmentCache> {
        use vortex::layout::segments::{MokaSegmentCache, NoOpSegmentCache};
        match self {
            VortexCacheMode::Global => crate::session::segment_cache(),
            VortexCacheMode::Off => Arc::new(NoOpSegmentCache),
            VortexCacheMode::Dedicated(bytes) => Arc::new(MokaSegmentCache::new(bytes)),
        }
    }
}

#[cfg(test)]
mod cache_mode_tests {
    use super::*;

    #[test]
    fn global_returns_same_arc_as_session_segment_cache() {
        // `Global` should hand out the exact same `Arc<dyn SegmentCache>` that
        // `session::segment_cache()` returns — that's the whole point of "the
        // process-wide cache". Two `Global::resolve()` calls within the same
        // process should point at the same underlying cache.
        let a = VortexCacheMode::Global.resolve();
        let b = VortexCacheMode::Global.resolve();
        assert!(
            Arc::ptr_eq(&a, &b),
            "VortexCacheMode::Global should always hand out the same Arc"
        );
        let g = crate::session::segment_cache();
        assert!(
            Arc::ptr_eq(&a, &g),
            "VortexCacheMode::Global should equal session::segment_cache()"
        );
    }

    #[test]
    fn off_returns_distinct_noop_caches() {
        // `Off` should hand out fresh `NoOpSegmentCache` instances, never the
        // global cache. The point is to opt OUT of shared caching.
        let off_a = VortexCacheMode::Off.resolve();
        let off_b = VortexCacheMode::Off.resolve();
        assert!(
            !Arc::ptr_eq(&off_a, &off_b),
            "VortexCacheMode::Off should produce distinct Arcs"
        );
        assert!(
            !Arc::ptr_eq(&off_a, &VortexCacheMode::Global.resolve()),
            "VortexCacheMode::Off should NOT alias the global cache"
        );
    }

    #[test]
    fn dedicated_returns_distinct_caches_per_call() {
        // `Dedicated(N)` should allocate a fresh per-scan cache every call so
        // two scans configured with the same budget don't accidentally share
        // state.
        let a = VortexCacheMode::Dedicated(4 * 1024 * 1024).resolve();
        let b = VortexCacheMode::Dedicated(4 * 1024 * 1024).resolve();
        assert!(
            !Arc::ptr_eq(&a, &b),
            "Two Dedicated(N) resolves should yield distinct Arcs"
        );
        assert!(
            !Arc::ptr_eq(&a, &VortexCacheMode::Global.resolve()),
            "Dedicated should not alias the global cache"
        );
    }
}
