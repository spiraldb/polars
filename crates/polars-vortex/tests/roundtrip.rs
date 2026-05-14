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

#[test]
fn schema_preserved_field_names() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("schema.vortex");

    let df = make_df();
    write_vortex(&df, &path, &VortexWriteOptions::default()).expect("write");

    let vxf = polars_core::runtime::ASYNC.block_on(async { open_back(&path).await });
    let names: Vec<&str> = vxf
        .dtype()
        .as_struct_fields_opt()
        .expect("top-level struct dtype")
        .names()
        .iter()
        .map(|n| n.as_ref())
        .collect();
    assert_eq!(names, vec!["ints", "floats", "strs"]);
}

#[test]
fn roundtrip_nullable() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("nullable.vortex");

    let s0 = Column::new("a".into(), &[Some(1_i32), None, Some(3), None, Some(5)]);
    let s1 = Column::new(
        "b".into(),
        &[Some("x"), Some("y"), None, Some("z"), None],
    );
    let df = DataFrame::new(5, vec![s0, s1]).expect("build df");

    write_vortex(&df, &path, &VortexWriteOptions::default()).expect("write");
    let vxf = polars_core::runtime::ASYNC.block_on(async { open_back(&path).await });
    assert_eq!(vxf.row_count(), 5);
}

#[test]
fn roundtrip_boolean() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("bool.vortex");

    let s0 = Column::new("flag".into(), &[true, false, true, true, false]);
    let df = DataFrame::new(5, vec![s0]).expect("build df");

    write_vortex(&df, &path, &VortexWriteOptions::default()).expect("write");
    let vxf = polars_core::runtime::ASYNC.block_on(async { open_back(&path).await });
    assert_eq!(vxf.row_count(), 5);
}

#[test]
fn roundtrip_empty_dataframe() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("empty.vortex");

    let s0 = Column::new("a".into(), &[] as &[i64]);
    let df = DataFrame::new(0, vec![s0]).expect("build df");

    write_vortex(&df, &path, &VortexWriteOptions::default()).expect("write");
    let vxf = polars_core::runtime::ASYNC.block_on(async { open_back(&path).await });
    assert_eq!(vxf.row_count(), 0);
}

#[test]
fn roundtrip_omit_dtype_then_supply_at_read() {
    // include_dtype=false produces a smaller file but requires the reader to
    // pass the DType back in. We don't currently expose that on the Polars side
    // (and probably never will — there's no realistic Polars workflow where
    // you'd lose the schema), so this test just verifies the writer succeeds
    // and the file exists.
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("no_dtype.vortex");

    let df = make_df();
    let opts = VortexWriteOptions {
        compression: VortexCompression::BtrBlocks,
        row_block_size: None,
        include_dtype: false,
    };
    write_vortex(&df, &path, &opts).expect("write");
    assert!(path.metadata().expect("stat").len() > 0);
}
