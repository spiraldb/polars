//! Vortex read path: open files, build scans, decode arrays.

use std::sync::Arc;

use vortex::file::Footer;
use vortex::layout::segments::SegmentCache;

pub mod array_bridge;
pub mod file_stats;
pub mod options;
pub mod predicate;
pub mod read_at;
pub mod schema;

/// Wrapper around `Arc<Footer>` used as the cache slot inside
/// [`polars_plan::dsl::FileScanIR::Vortex::footer`]. Lives here (rather than in a
/// dedicated module) because it's a single type alias.
pub type VortexFooterRef = Arc<Footer>;

/// Resolved-cache handle threaded through the IR (via
/// [`polars_plan::dsl::FileScanIR::Vortex::segment_cache`]) and into the streaming source so
/// the schema-discovery read and the data read share one Moka cache instance for the entire
/// logical scan. Without the thread-through, `VortexCacheMode::Dedicated(N).resolve()` is
/// called twice — once at IR-build time, once at streaming-source-time — producing two
/// independent caches per scan and losing the schema-discovery → data-read prefetching
/// benefit. Resolved by callers via [`crate::read::options::VortexCacheMode::resolve`].
///
/// Newtype wrapper around `Arc<dyn SegmentCache>` because the bare trait-object Arc doesn't
/// implement `Debug` (upstream Vortex's `SegmentCache` trait doesn't require `Debug`), and
/// `FileScanIR` derives `Debug`. The wrapper provides an opaque `Debug` impl ("<segment
/// cache>") and `Deref`s to the inner Arc for transparent use at upstream call sites.
#[derive(Clone)]
pub struct VortexSegmentCacheRef(pub Arc<dyn SegmentCache>);

impl std::fmt::Debug for VortexSegmentCacheRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("VortexSegmentCacheRef")
            .field(&format_args!("<dyn SegmentCache>"))
            .finish()
    }
}

impl std::ops::Deref for VortexSegmentCacheRef {
    type Target = Arc<dyn SegmentCache>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Arc<dyn SegmentCache>> for VortexSegmentCacheRef {
    fn from(arc: Arc<dyn SegmentCache>) -> Self {
        Self(arc)
    }
}

#[cfg(test)]
mod segment_cache_ref_tests {
    //! PR-2.0 commit-3 introduced [`VortexSegmentCacheRef`] as the thread-through type for
    //! `FileScanIR::Vortex::segment_cache`. The single-cache invariant rests on the wrapper
    //! preserving `Arc<dyn SegmentCache>` IDENTITY across clone / Deref / From; if any of
    //! these silently allocate or copy, the IR-build-time and streaming-source caches would
    //! diverge and re-introduce the cycle-3 Dedicated double-resolve bug.
    //!
    //! These tests verify the contract directly on the wrapper (without spinning up a real
    //! scan). The corresponding end-to-end integration test (a `pl.scan_vortex(...,
    //! cache_mode=Dedicated(N))` that confirms ONE Moka cache instance is used across the
    //! IR-build postscript read AND the streaming data read) is deferred to PR-3.1's
    //! `table_statistics` work — PR-3.1 already touches `vortex_file_info` and is the
    //! natural place to add the end-to-end harness. (cycle-1 PR-2.0 must-fix C-003.)
    use super::*;
    use crate::read::options::VortexCacheMode;

    #[test]
    fn clone_preserves_arc_identity() {
        // The whole point of the wrapper: cloning it must NOT clone the inner cache.
        // Cloning ANY non-trivial Arc<dyn SegmentCache> would re-introduce the double-cache
        // bug because the IR thread-through clones across builders / readers / files.
        let inner: Arc<dyn SegmentCache> = VortexCacheMode::Dedicated(1 << 20).resolve();
        let wrapper = VortexSegmentCacheRef(inner.clone());
        let cloned = wrapper.clone();

        let p1 = Arc::as_ptr(&wrapper.0) as *const ();
        let p2 = Arc::as_ptr(&cloned.0) as *const ();
        let p_orig = Arc::as_ptr(&inner) as *const ();

        assert_eq!(
            p1, p2,
            "VortexSegmentCacheRef::clone must preserve Arc<dyn SegmentCache> identity"
        );
        assert_eq!(
            p1, p_orig,
            "Construction via VortexSegmentCacheRef(arc) must preserve Arc identity"
        );
    }

    #[test]
    fn from_arc_preserves_identity() {
        // `.into()` must be a thin newtype wrap, not a clone of the inner contents.
        let inner: Arc<dyn SegmentCache> = VortexCacheMode::Dedicated(1 << 20).resolve();
        let p_orig = Arc::as_ptr(&inner) as *const ();

        let wrapper: VortexSegmentCacheRef = inner.clone().into();
        let p_wrapped = Arc::as_ptr(&wrapper.0) as *const ();

        assert_eq!(
            p_orig, p_wrapped,
            "From<Arc<dyn SegmentCache>> must preserve identity (newtype wrap, not clone-then-allocate)"
        );
    }

    #[test]
    fn deref_returns_inner_arc_by_reference() {
        // `Deref` must hand out `&Arc<dyn SegmentCache>` pointing at the SAME inner Arc;
        // it MUST NOT clone or rewrap. Without this, `&*wrapper` at the call site would
        // produce a different Arc::as_ptr than `&wrapper.0`.
        let inner: Arc<dyn SegmentCache> = VortexCacheMode::Dedicated(1 << 20).resolve();
        let wrapper = VortexSegmentCacheRef(inner.clone());

        // Manually deref the wrapper to get `&Arc<dyn SegmentCache>`.
        let derefed: &Arc<dyn SegmentCache> = &wrapper;
        let p_via_deref = Arc::as_ptr(derefed) as *const ();
        let p_via_field = Arc::as_ptr(&wrapper.0) as *const ();
        let p_orig = Arc::as_ptr(&inner) as *const ();

        assert_eq!(
            p_via_deref, p_via_field,
            "Deref must reference the same Arc as .0"
        );
        assert_eq!(p_via_deref, p_orig, "Deref must reference the original Arc");
    }
}
