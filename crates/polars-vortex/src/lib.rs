//! Vortex file-format I/O for Polars.
//!
//! Provides native, first-class support for the Vortex columnar file format, plumbed through
//! Polars' [`FileScanIR`](polars_plan::dsl::FileScanIR) variant and streaming source node.
//!
//! The integration runs entirely on Polars' global Tokio runtime via [`session::session`] which
//! constructs a single process-wide [`vortex::session::VortexSession`] backed by a
//! [`vortex::io::runtime::tokio::TokioRuntime`] adapter over [`polars_core::runtime::ASYNC`].

pub mod read;
pub mod session;
pub mod write;

pub use read::options::{VortexCacheMode, VortexScanOptions};
pub use session::session;
pub use write::options::{VortexCompression, VortexLayoutKind, VortexWriteOptions};
