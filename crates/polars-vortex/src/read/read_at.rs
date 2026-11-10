//! Positional reads through the Polars byte source.
use std::sync::Arc;

use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use polars_io::utils::byte_source::{ByteSource, DynByteSource};
use vortex::array::buffer::BufferHandle;
use vortex::buffer::{Alignment, ByteBuffer};
use vortex::error::{VortexResult, vortex_err};
use vortex::io::VortexReadAt;

#[derive(Clone)]
pub struct PolarsVortexReadAt {
    inner: Arc<DynByteSource>,
}
impl PolarsVortexReadAt {
    pub fn new(inner: Arc<DynByteSource>) -> Self {
        Self { inner }
    }
}
impl VortexReadAt for PolarsVortexReadAt {
    fn concurrency(&self) -> usize {
        polars_config::config().max_threads().max(1)
    }
    fn size(&self) -> BoxFuture<'static, VortexResult<u64>> {
        let source = self.inner.clone();
        crate::session::handle()
            .spawn_io(async move {
                source
                    .get_size()
                    .await
                    .map(|n| n as u64)
                    .map_err(|e| vortex_err!("Polars source size: {e}"))
            })
            .boxed()
    }
    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> BoxFuture<'static, VortexResult<BufferHandle>> {
        let source = self.inner.clone();
        async move {
            // Vortex can poll its segment driver on the Polars CPU scheduler.
            // Cloud byte sources require Tokio for their network and timer futures.
            let buffer = crate::session::handle()
                .spawn_io(async move {
                    let start = usize::try_from(offset)
                        .map_err(|_| vortex_err!("read offset exceeds usize"))?;
                    let end = start
                        .checked_add(length)
                        .ok_or_else(|| vortex_err!("read range overflow"))?;
                    let size = source
                        .get_size()
                        .await
                        .map_err(|e| vortex_err!("Polars source size: {e}"))?;
                    if end > size {
                        return Err(vortex_err!(
                            "Vortex read range {start}..{end} exceeds file size {size}"
                        ));
                    }

                    source
                        .get_range(start..end)
                        .await
                        .map_err(|e| vortex_err!("Polars range read: {e}"))
                })
                .await?;

            let bytes = Bytes::from_owner(buffer);
            // Cloud responses and sliced buffers need not meet Vortex's alignment.
            // Keep the owner without a copy when its address already aligns.
            let buffer =
                ByteBuffer::from_bytes_aligned(bytes, Alignment::none()).aligned(alignment);
            Ok(BufferHandle::new_host(buffer))
        }
        .boxed()
    }
}
