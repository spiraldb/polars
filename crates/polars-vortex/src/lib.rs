//! Native Vortex file IO for Polars.

pub mod read;
pub mod session;
pub mod write;

pub mod vortex {
    pub use ::vortex::{
        array, arrow, buffer, dtype, error, expr, file, io, layout, scalar, session,
    };
}
pub use read::options::{VortexCacheMode, VortexScanOptions};
pub use session::session;
pub use write::options::{VortexCompression, VortexWriteOptions};
