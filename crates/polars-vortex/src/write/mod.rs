//! Vortex write path: encode Polars DataFrames as Vortex files.
//!
//! The eager API is [`write_vortex`]. The streaming sink (`LazyFrame::sink_vortex`)
//! plugs into Polars' `FileWriteFormat::Vortex` variant — landed in PR-10.

pub mod array_bridge;
pub mod df_to_stream;
pub mod options;
pub mod writer;

pub use writer::write_vortex;
