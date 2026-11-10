//! Native Vortex file IO for Polars.

pub mod read;
pub mod session;

pub mod vortex {
    pub use ::vortex::{array, arrow, dtype, error, expr, file, io, layout, scalar};
}
pub use read::options::{VortexCacheMode, VortexScanOptions};
pub use session::session;
