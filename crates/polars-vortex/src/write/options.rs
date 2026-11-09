//! Options for native Vortex writes.

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "dsl-schema", derive(schemars::JsonSchema))]
pub struct VortexWriteOptions {
    pub compression: VortexCompression,
    /// Number of rows per statistics zone. None uses the Vortex default.
    pub row_block_size: Option<u64>,
    /// Include the schema in the file. False returns an error.
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
    #[default]
    BtrBlocks,
    Uncompressed,
}
