//! Eager local-file writer.

use std::path::Path;

use futures::stream;
use polars_core::frame::DataFrame;
use polars_core::runtime::ASYNC;
use polars_error::{PolarsResult, polars_err};
use vortex::array::stream::ArrayStreamAdapter;
use vortex::error::VortexResult;

use super::sink_writer::VortexSink;
use crate::write::df_to_stream::dataframe_to_vortex_chunks;
use crate::write::options::VortexWriteOptions;
use crate::write::strategy::build_write_options;

pub fn write_vortex(
    df: &DataFrame,
    path: impl AsRef<Path>,
    options: &VortexWriteOptions,
) -> PolarsResult<()> {
    let path = path.as_ref().to_path_buf();

    let (top_dtype, chunks) = dataframe_to_vortex_chunks(df)?;

    let write_opts = build_write_options(options)?;

    let stream = ArrayStreamAdapter::new(
        top_dtype,
        stream::iter(chunks.into_iter().map(VortexResult::Ok)),
    );

    ASYNC.block_in_place_on(async move {
        let file = tokio::fs::File::create(&path).await.map_err(
            |e| polars_err!(ComputeError: "vortex write: create {}: {e}", path.display()),
        )?;
        write_opts
            .write(VortexSink(file), stream)
            .await
            .map_err(|e| polars_err!(ComputeError: "vortex write: {e}"))?;
        Ok::<_, polars_error::PolarsError>(())
    })?;

    Ok(())
}
