//! Local adapter for eager writes.

use std::io;

use vortex::io::{IoBuf, VortexWrite};

pub struct VortexSink(pub tokio::fs::File);

impl VortexWrite for VortexSink {
    async fn write_all<B: IoBuf>(&mut self, buffer: B) -> io::Result<B> {
        tokio::io::AsyncWriteExt::write_all(&mut self.0, buffer.as_slice()).await?;
        Ok(buffer)
    }
    async fn flush(&mut self) -> io::Result<()> {
        tokio::io::AsyncWriteExt::flush(&mut self.0).await
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        tokio::io::AsyncWriteExt::shutdown(&mut self.0).await
    }
}
