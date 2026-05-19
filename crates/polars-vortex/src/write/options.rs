//! Options for `LazyFrame::sink_vortex` / `DataFrame::write_vortex`.

/// Write-side options for a Vortex file. Lives in
/// [`polars_plan::dsl::FileWriteFormat::Vortex`].
///
/// Vortex's `WriteStrategyBuilder` always produces a layered Flat→Chunked→
/// Buffered→Zoned strategy; there is no "select your layout shape" knob to
/// surface. What we *can* tune is the inner block granularity (`row_block_size`)
/// and the compression schemes the BtrBlocks sampler is allowed to pick from.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "dsl-schema", derive(schemars::JsonSchema))]
pub struct VortexWriteOptions {
    pub compression: VortexCompression,
    /// Row-block size that controls the granularity of zone-level pruning.
    /// `None` → Vortex's default (8192). Smaller blocks → finer pruning but more
    /// metadata; larger blocks → coarser pruning, less metadata overhead.
    pub row_block_size: Option<u64>,
    /// Embed the Vortex `DType` in the file metadata. `true` (default) is what
    /// readers expect when no out-of-band schema is provided.
    pub include_dtype: bool,
}

impl Default for VortexWriteOptions {
    fn default() -> Self {
        Self {
            compression: VortexCompression::default(),
            row_block_size: None,
            include_dtype: true,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "dsl-schema", derive(schemars::JsonSchema))]
pub enum VortexCompression {
    /// BtrBlocks sampling compressor — picks an encoding per column.
    #[default]
    BtrBlocks,
    /// Empty compressor — no encoding schemes selected. Useful for benchmarks or
    /// strict-compliance scenarios where compression is not desired.
    Uncompressed,
}
