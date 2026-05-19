//! `VortexSink` — a unified writer that `VortexWriteOptions::write` can drain into,
//! regardless of whether the underlying sink is a local file or a cloud writer.
//!
//! Vortex's writer is generic over `W: VortexWrite + Unpin`. We have two concrete W
//! candidates from Polars' sink machinery:
//!
//! - `Writeable::Local(std::fs::File)` → `tokio::fs::File::from_std(...)`, which
//!   has a direct `impl VortexWrite for tokio::fs::File` in `vortex-io`.
//!
//! - `Writeable::Cloud(CloudWriterIoTraitWrap)` → a `tokio::io::AsyncWrite`. Vortex's
//!   `AsyncWriteAdapter` wants `futures::AsyncWrite`, so we bridge with
//!   `tokio_util::compat::Compat`. The full chain is
//!   `AsyncWriteAdapter<Compat<CloudWriterIoTraitWrap>>`.
//!
//! Rather than threading two generic types through the sink, we collapse both into
//! this enum and implement `VortexWrite` once by matching.

use std::io;

#[cfg(feature = "cloud")]
use polars_io::cloud::cloud_writer::CloudWriterIoTraitWrap;
#[cfg(feature = "cloud")]
use tokio_util::compat::Compat;
#[cfg(feature = "cloud")]
use vortex::io::AsyncWriteAdapter;
use vortex::io::{IoBuf, VortexWrite};

/// Unified Vortex sink. Pass an instance of this enum to
/// `VortexWriteOptions::write(sink, stream)`.
pub enum VortexSink {
    /// Local file. `tokio::fs::File` has its own `impl VortexWrite`.
    Local(tokio::fs::File),
    /// Cloud writer wrapped through tokio-util's compat layer and Vortex's
    /// `AsyncWriteAdapter`.
    #[cfg(feature = "cloud")]
    Cloud(AsyncWriteAdapter<Compat<CloudWriterIoTraitWrap>>),
}

impl VortexSink {
    /// Build a `VortexSink::Cloud` from a Polars `CloudWriterIoTraitWrap`. Returns
    /// the writer wrapped through the compat layer and Vortex's adapter.
    #[cfg(feature = "cloud")]
    pub fn cloud(cw: CloudWriterIoTraitWrap) -> Self {
        use tokio_util::compat::TokioAsyncWriteCompatExt;
        VortexSink::Cloud(AsyncWriteAdapter(cw.compat_write()))
    }
}

impl VortexWrite for VortexSink {
    async fn write_all<B: IoBuf>(&mut self, buffer: B) -> io::Result<B> {
        match self {
            Self::Local(f) => VortexWrite::write_all(f, buffer).await,
            #[cfg(feature = "cloud")]
            Self::Cloud(c) => VortexWrite::write_all(c, buffer).await,
        }
    }

    async fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Local(f) => VortexWrite::flush(f).await,
            #[cfg(feature = "cloud")]
            Self::Cloud(c) => VortexWrite::flush(c).await,
        }
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        match self {
            Self::Local(f) => VortexWrite::shutdown(f).await,
            #[cfg(feature = "cloud")]
            Self::Cloud(c) => VortexWrite::shutdown(c).await,
        }
    }
}
