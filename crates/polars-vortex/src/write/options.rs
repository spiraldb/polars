//! Options for `LazyFrame::sink_vortex` / `DataFrame::write_vortex`.

/// Write-side options for a Vortex file. Lives in
/// [`polars_plan::dsl::FileWriteFormat::Vortex`].
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "dsl-schema", derive(schemars::JsonSchema))]
pub struct VortexWriteOptions {
    pub layout: VortexLayoutKind,
    pub compression: VortexCompression,
    pub target_chunk_size: Option<u64>,
    pub include_dtype: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "dsl-schema", derive(schemars::JsonSchema))]
pub enum VortexLayoutKind {
    /// Vortex's default adaptive layout (BtrBlocks + Zoned + Chunked).
    #[default]
    Adaptive,
    /// Single flat layout — no pruning, smallest writes.
    Flat,
    /// Chunked-of-flat — natural parallelism, no zone pruning.
    Chunked,
    /// Chunked-of-zoned-of-flat — full pruning, the recommended default for filtered scans.
    Zoned,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "dsl-schema", derive(schemars::JsonSchema))]
pub enum VortexCompression {
    /// BtrBlocks sampling compressor — picks an encoding per column.
    #[default]
    BtrBlocks,
    /// Uncompressed (flat encodings only).
    Uncompressed,
}
