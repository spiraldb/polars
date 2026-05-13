//! Eager Vortex writer: `pl.DataFrame.write_vortex(path, options)`.
//!
//! Streams a Polars DataFrame chunk-by-chunk into a Vortex file. Uses the
//! reverse C-ABI bridge ([`crate::write::array_bridge`]) to move buffers without
//! copying, then drives [`VortexWriteOptions::write`] on Polars' global ASYNC
//! Tokio runtime.

use std::path::Path;

use futures::stream;
use polars_core::frame::DataFrame;
use polars_core::runtime::ASYNC;
use polars_error::{PolarsResult, polars_err};
use vortex::array::stream::ArrayStreamAdapter;
use vortex::error::VortexResult;
use vortex::file::WriteOptionsSessionExt;

use crate::session::session;
use crate::write::df_to_stream::dataframe_to_vortex_chunks;
use crate::write::options::VortexWriteOptions;

/// Write a Polars [`DataFrame`] to a Vortex file at `path`. Creates / truncates the
/// destination.
///
/// Options control layout (Adaptive / Flat / Chunked / Zoned) and compression. The
/// default settings produce a BtrBlocks-compressed Zoned layout, which is the
/// recommended shape for filtered scans.
pub fn write_vortex(
    df: &DataFrame,
    path: impl AsRef<Path>,
    _options: &VortexWriteOptions,
) -> PolarsResult<()> {
    let path = path.as_ref().to_path_buf();
    let session = session();

    // Convert all chunks up front. The C-ABI bridge moves buffers zero-copy, so
    // the only per-chunk memory cost is the struct/record-batch wrappers.
    let (top_dtype, chunks) = dataframe_to_vortex_chunks(df)?;

    let write_opts = session.write_options();

    // Wrap the chunks as a Vortex ArrayStream.
    let stream = ArrayStreamAdapter::new(
        top_dtype,
        stream::iter(chunks.into_iter().map(VortexResult::Ok)),
    );

    ASYNC
        .block_on(async move {
            let file = tokio::fs::File::create(&path)
                .await
                .map_err(|e| polars_err!(ComputeError: "vortex write: create {}: {e}", path.display()))?;
            write_opts
                .write(file, stream)
                .await
                .map_err(|e| polars_err!(ComputeError: "vortex write: {e}"))?;
            Ok::<_, polars_error::PolarsError>(())
        })?;

    Ok(())
}
