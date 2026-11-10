use std::sync::Arc;

use vortex::file::Footer;
pub mod array_bridge;
pub mod options;
pub mod predicate;
pub mod read_at;
pub mod schema;
pub type VortexFooterRef = Arc<Footer>;
