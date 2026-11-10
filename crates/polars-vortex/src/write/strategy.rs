//! Vortex layout and compression options.

use polars_error::{PolarsResult, polars_ensure, polars_err};
use vortex::compressor::BtrBlocksCompressorBuilder;
use vortex::file::{
    VortexWriteOptions as FileWriteOptions, WriteOptionsSessionExt, WriteStrategyBuilder,
};
use vortex::session::VortexSession;

use super::options::{VortexCompression, VortexWriteOptions};
use crate::session::session;

pub fn build_write_options(options: &VortexWriteOptions) -> PolarsResult<FileWriteOptions> {
    build_write_options_with_session(options, session())
}

pub fn build_write_options_with_session(
    options: &VortexWriteOptions,
    session: &VortexSession,
) -> PolarsResult<FileWriteOptions> {
    polars_ensure!(options.include_dtype, InvalidOperation:
        "Vortex requires an embedded schema. include_dtype=False is not supported");
    let compressor = match options.compression {
        VortexCompression::BtrBlocks => BtrBlocksCompressorBuilder::from_session(session),
        VortexCompression::Uncompressed => BtrBlocksCompressorBuilder::empty(),
    };
    let mut strategy =
        WriteStrategyBuilder::from_session(session).with_btrblocks_builder(compressor);
    if let Some(rows) = options.row_block_size {
        polars_ensure!(rows != 0, InvalidOperation: "Vortex row_block_size must be greater than zero");
        let rows = usize::try_from(rows).map_err(
            |_| polars_err!(InvalidOperation: "Vortex row_block_size exceeds the platform limit"),
        )?;
        strategy = strategy.with_row_block_size(rows);
    }
    Ok(session.write_options().with_strategy(strategy.build()))
}
