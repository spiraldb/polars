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

/// Re-exports of upstream Vortex types/macros used across polars-stream's Vortex source/sink,
/// polars-plan's IR conversion, and polars-vortex tests. The 5 sub-modules below cover the
/// actual cross-crate surface (8 paths: array::{ArrayRef, VortexSessionExecute,
/// arrow::ArrowArrayExecutor, stream::ArrayStreamAdapter}; error::{vortex_err!, VortexResult};
/// file::{Footer, OpenOptionsSessionExt, VortexFile}; io::{VortexReadAt, std_file::FileReadAt};
/// layout::segments::SegmentCache). Narrowed from `pub use ::vortex;` in PR-1.4 so the BAN
/// against new `vortex`-internal symbol use outside the bridge files becomes machine-checkable
/// — anything outside the 5 sub-modules fails to compile.
pub mod vortex {
    pub use ::vortex::{array, error, file, io, layout};
}
pub use read::options::{VortexCacheMode, VortexScanOptions};
pub use session::session;
pub use write::options::{VortexCompression, VortexWriteOptions};
