//! Translate the public [`VortexWriteOptions`] into a Vortex-side
//! `VortexWriteOptions` builder, applied to a freshly-built `write_options()`
//! from the global session.

use vortex::compressor::BtrBlocksCompressorBuilder;
use vortex::file::{
    VortexWriteOptions as VortexFileWriteOptions, WriteOptionsSessionExt, WriteStrategyBuilder,
};

use crate::session::session;
use crate::write::options::{VortexCompression, VortexWriteOptions};

/// Build the configured `VortexWriteOptions` for the global session, applying
/// our public knobs (compression, row_block_size, include_dtype).
pub fn build_write_options(opts: &VortexWriteOptions) -> VortexFileWriteOptions {
    // Pick the compressor builder.
    let compressor_builder = match opts.compression {
        VortexCompression::BtrBlocks => BtrBlocksCompressorBuilder::default(),
        VortexCompression::Uncompressed => BtrBlocksCompressorBuilder::empty(),
    };

    // Build the layout strategy with the requested row-block size.
    let mut strategy_builder =
        WriteStrategyBuilder::default().with_btrblocks_builder(compressor_builder);
    if let Some(rbs) = opts.row_block_size {
        // Clamp the u64 row_block_size to usize::MAX on 32-bit platforms; matches the
        // BAN-compliant pattern at scans.rs:349 and io_sources/vortex/mod.rs:217.
        let rbs_usize = usize::try_from(rbs).unwrap_or(usize::MAX);
        strategy_builder = strategy_builder.with_row_block_size(rbs_usize);
    }
    let strategy = strategy_builder.build();

    let mut write_opts = session().write_options().with_strategy(strategy);
    if !opts.include_dtype {
        write_opts = write_opts.exclude_dtype();
    }
    write_opts
}
