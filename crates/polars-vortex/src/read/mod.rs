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
