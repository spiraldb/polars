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
/// polars-plan's IR conversion, and polars-vortex tests. **Invariant**: anything outside
/// `vortex::{array, dtype, error, expr, file, io, layout}` fails to compile — narrowed from
/// `pub use ::vortex;` in PR-1.4 so the BAN against new `vortex`-internal symbol use outside
/// the bridge files is machine-checkable. Re-run the actual-use audit (grep for
/// `polars_vortex::vortex::`) before adding a new sub-module to this list.
///
/// `dtype` added in PR-2.3 so the AExpr-direct convertor's CAST arm can build target
/// `vortex::dtype::DType` values without going through the polars-arrow→upstream-arrow
/// schema bridge (overkill for a single dtype). See `vortex_convertor::polars_dtype_to_vortex_dtype`.
pub mod vortex {
    pub use ::vortex::{array, dtype, error, expr, file, io, layout};
}
pub use read::options::{VortexCacheMode, VortexScanOptions};
pub use session::session;
pub use write::options::{VortexCompression, VortexWriteOptions};
