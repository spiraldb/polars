//! Vortex read path: open files, build scans, decode arrays.

use std::sync::Arc;

use vortex::file::Footer;

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
