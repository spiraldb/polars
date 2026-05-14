//! Roundtrip integration tests for the polars-vortex writer.
//!
//! Exercise the full eager-write path (`write_vortex`) end-to-end, then verify
//! the resulting file is non-empty and re-openable through the global Vortex
//! session. They live here so they only need polars-core + the global Vortex
//! session — no streaming-engine surface required, which would create a cycle.

use std::sync::Arc;

use polars_core::frame::DataFrame;
use polars_core::prelude::Column;
use polars_vortex::session::{handle, session};
use polars_vortex::vortex::file::{OpenOptionsSessionExt, VortexFile};
use polars_vortex::vortex::io::VortexReadAt;
use polars_vortex::vortex::io::std_file::FileReadAt;
use polars_vortex::write::write_vortex;
use polars_vortex::{VortexCompression, VortexWriteOptions};
use tempfile::tempdir;

async fn open_back(path: &std::path::Path) -> VortexFile {
    let read_at: Arc<dyn VortexReadAt> =
        Arc::new(FileReadAt::open(path, handle()).expect("FileReadAt::open"));
    session()
        .open_options()
        .open(read_at)
        .await
        .expect("vortex open")
}

fn make_df() -> DataFrame {
    let s0 = Column::new("ints".into(), [1_i64, 2, 3, 4, 5].as_ref());
    let s1 = Column::new("floats".into(), [1.0_f64, 2.0, 3.0, 4.0, 5.0].as_ref());
    let s2 = Column::new("strs".into(), ["a", "b", "c", "d", "e"].as_ref());
    DataFrame::new(5, vec![s0, s1, s2]).expect("build df")
}

#[test]
fn roundtrip_default_options() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("default.vortex");

    let df = make_df();
    write_vortex(&df, &path, &VortexWriteOptions::default()).expect("write");

    let vxf = polars_core::runtime::ASYNC.block_on(async { open_back(&path).await });
    assert_eq!(vxf.row_count(), 5);
    assert!(
        path.metadata().expect("stat").len() > 0,
        "file should be non-empty"
    );
}

#[test]
fn roundtrip_uncompressed() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("uncompressed.vortex");

    let df = make_df();
    let opts = VortexWriteOptions {
        compression: VortexCompression::Uncompressed,
        row_block_size: None,
        include_dtype: true,
    };
    write_vortex(&df, &path, &opts).expect("write");

    let vxf = polars_core::runtime::ASYNC.block_on(async { open_back(&path).await });
    assert_eq!(vxf.row_count(), 5);
}

#[test]
fn roundtrip_small_row_block() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("small_blocks.vortex");

    let df = make_df();
    let opts = VortexWriteOptions {
        compression: VortexCompression::BtrBlocks,
        row_block_size: Some(2),
        include_dtype: true,
    };
    write_vortex(&df, &path, &opts).expect("write");

    let vxf = polars_core::runtime::ASYNC.block_on(async { open_back(&path).await });
    assert_eq!(vxf.row_count(), 5);
}
