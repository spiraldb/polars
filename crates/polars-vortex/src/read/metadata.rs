//! Cached Vortex `Footer` reference, plus the `vortex_file_info` schema-discovery helper used
//! by the DSL→IR conversion pass.

use std::sync::Arc;

use vortex::file::Footer;

/// Wrapper around `Arc<Footer>` used as the cache slot inside
/// [`polars_plan::dsl::FileScanIR::Vortex::footer`].
pub type VortexFooterRef = Arc<Footer>;
