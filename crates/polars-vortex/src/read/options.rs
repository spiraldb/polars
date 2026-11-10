use std::num::NonZeroUsize;
use std::sync::Arc;

use polars_core::schema::SchemaRef;
use vortex::layout::segments::SegmentCache;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "dsl-schema", derive(schemars::JsonSchema))]
pub struct VortexScanOptions {
    pub schema: Option<SchemaRef>,
    pub push_predicate: bool,
    pub initial_read_size: Option<usize>,
    pub scan_concurrency: Option<NonZeroUsize>,
    pub segment_cache: VortexCacheMode,
}
impl Default for VortexScanOptions {
    fn default() -> Self {
        Self {
            schema: None,
            push_predicate: true,
            initial_read_size: None,
            scan_concurrency: None,
            segment_cache: VortexCacheMode::default(),
        }
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "dsl-schema", derive(schemars::JsonSchema))]
pub enum VortexCacheMode {
    /// Use the process default budget for a new cache for each file open.
    #[default]
    Global,
    Off,
    /// Use this budget for a new cache for each file open.
    Dedicated(u64),
}
impl VortexCacheMode {
    pub fn resolve(self) -> Arc<dyn SegmentCache> {
        match self {
            Self::Global => crate::session::segment_cache(),
            Self::Off => crate::session::build_cache(0),
            Self::Dedicated(bytes) => crate::session::build_cache(bytes),
        }
    }
}
