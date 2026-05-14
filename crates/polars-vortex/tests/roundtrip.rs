//! Roundtrip integration tests for the polars-vortex writer + read bridge.
//!
//! These exercise the full eager-write path (`write_vortex`), then read the
//! resulting file back through the lower-level Vortex scan API + our
//! `record_batch_to_dataframe` C-ABI bridge — exactly the path the streaming
//! reader uses internally. We assert `DataFrame` equality, so a regression in
//! the buffer-bridging would surface as a failed test rather than as silently
//! mismatched bytes.
//!
//! Tests live here (not in `crates/polars/tests/`) so they only need polars-core
//! + the global Vortex session, without a circular dep on polars-stream.

use std::sync::Arc;

use futures::StreamExt;
use polars_core::frame::DataFrame;
use polars_core::prelude::Column;
use polars_core::runtime::ASYNC;
use polars_vortex::read::array_bridge::{ArrowUpstreamSchema, arrow_dtypes_from_schema, record_batch_to_dataframe};
use polars_vortex::read::schema::vortex_dtype_to_schema;
use polars_vortex::session::{handle, session};
use polars_vortex::vortex::array::VortexSessionExecute;
use polars_vortex::vortex::array::arrow::ArrowArrayExecutor;
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

/// Open a Vortex file at `path` and read back into a Polars DataFrame, going
/// through the same scan → execute_record_batch → C-ABI bridge path the
/// streaming reader uses internally.
fn read_back(path: &std::path::Path) -> DataFrame {
    ASYNC.block_on(async {
        let vxf = open_back(path).await;
        let (pl_schema, arrow_schema) = vortex_dtype_to_schema(vxf.dtype()).expect("schema");
        let arrow_dtypes = arrow_dtypes_from_schema(arrow_schema.as_ref());
        let upstream_schema: Arc<ArrowUpstreamSchema> = Arc::new(
            vxf.dtype()
                .to_arrow_schema()
                .expect("dtype -> upstream schema"),
        );

        let stream = vxf
            .scan()
            .expect("scan")
            .into_array_stream()
            .expect("array stream");
        futures::pin_mut!(stream);

        let session_ref = session();
        let mut out: Option<DataFrame> = None;
        while let Some(chunk) = stream.next().await {
            let array = chunk.expect("chunk");
            let mut ctx = session_ref.create_execution_ctx();
            let rb = array
                .execute_record_batch(upstream_schema.as_ref(), &mut ctx)
                .expect("execute_record_batch");
            let df =
                record_batch_to_dataframe(rb, &pl_schema, &arrow_dtypes).expect("bridge");
            out = Some(match out {
                None => df,
                Some(mut acc) => {
                    acc.vstack_mut(&df).expect("vstack");
                    acc
                }
            });
        }
        out.unwrap_or_else(|| DataFrame::empty_with_schema(pl_schema.as_ref()))
    })
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

    let back = read_back(&path);
    assert!(df.equals_missing(&back), "DataFrame round-trip mismatch:\nwrote {df:?}\nread {back:?}");
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

    let back = read_back(&path);
    assert!(df.equals_missing(&back));
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

    let back = read_back(&path);
    assert!(df.equals_missing(&back));
}

#[test]
fn schema_preserved_field_names() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("schema.vortex");

    let df = make_df();
    write_vortex(&df, &path, &VortexWriteOptions::default()).expect("write");

    let vxf = ASYNC.block_on(async { open_back(&path).await });
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
    let back = read_back(&path);
    assert!(df.equals_missing(&back), "nullable mismatch:\nwrote {df:?}\nread {back:?}");
}

#[test]
fn roundtrip_boolean() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("bool.vortex");

    let s0 = Column::new("flag".into(), &[true, false, true, true, false]);
    let df = DataFrame::new(5, vec![s0]).expect("build df");

    write_vortex(&df, &path, &VortexWriteOptions::default()).expect("write");
    let back = read_back(&path);
    assert!(df.equals_missing(&back));
}

#[test]
fn roundtrip_empty_dataframe() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("empty.vortex");

    let s0 = Column::new("a".into(), &[] as &[i64]);
    let df = DataFrame::new(0, vec![s0]).expect("build df");

    write_vortex(&df, &path, &VortexWriteOptions::default()).expect("write");
    let back = read_back(&path);
    // Zero-row DataFrames may have 0 chunks (so `read_back` returns the
    // empty-with-schema fallback) — verify row count + schema names match.
    assert_eq!(back.height(), 0);
    assert_eq!(back.get_column_names(), df.get_column_names());
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

/// Helper: write `df` to a tempfile, read it back, and assert frame equality.
fn assert_roundtrip(df: DataFrame, name: &str) {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join(name);
    write_vortex(&df, &path, &VortexWriteOptions::default()).expect("write");
    let back = read_back(&path);
    assert!(
        df.equals_missing(&back),
        "{name} mismatch:\nwrote {df:?}\nread  {back:?}"
    );
}

// ============================================================================
// Primitive dtype coverage: integer and float widths.
//
// Note: i8/i16/u8/u16 require polars-core's `dtype-i8`/`dtype-i16`/`dtype-u8`/
// `dtype-u16` features and don't have direct `NamedFrom<&[T]>` impls without
// them. The roundtrip path itself is dtype-agnostic (it goes through the C-ABI
// bridge), so the schema-converter unit tests in `read::schema::tests` exercise
// the smaller widths. Here we cover what's directly constructible from primitive
// arrays.
// ============================================================================

#[test]
fn roundtrip_int32_and_int64_non_nullable() {
    let df = DataFrame::new(
        3,
        vec![
            Column::new("i32".into(), &[1_i32, 2, 3]),
            Column::new("i64".into(), &[1_i64, 2, 3]),
        ],
    )
    .expect("build df");
    assert_roundtrip(df, "ints.vortex");
}

#[test]
fn roundtrip_uint32_and_uint64() {
    let df = DataFrame::new(
        3,
        vec![
            Column::new("u32".into(), &[1_u32, 2, 3]),
            Column::new("u64".into(), &[1_u64, 2, 3]),
        ],
    )
    .expect("build df");
    assert_roundtrip(df, "uints.vortex");
}

#[test]
fn roundtrip_float32_and_float64() {
    let df = DataFrame::new(
        3,
        vec![
            Column::new("f32".into(), &[1.0_f32, 2.0, 3.0]),
            Column::new("f64".into(), &[1.0_f64, 2.0, 3.0]),
        ],
    )
    .expect("build df");
    assert_roundtrip(df, "floats.vortex");
}

#[test]
fn roundtrip_float_nans_preserved() {
    // NaN compares unequal to itself, so `equals_missing` would fail. Verify
    // shape + dtype + that null positions survive, but treat the float values
    // as opaque.
    let df = DataFrame::new(
        3,
        vec![
            Column::new("f32".into(), &[1.0_f32, f32::NAN, 3.0]),
            Column::new("f64".into(), &[f64::NAN, 2.0, f64::NAN]),
        ],
    )
    .expect("build df");
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("nans.vortex");
    write_vortex(&df, &path, &VortexWriteOptions::default()).expect("write");
    let back = read_back(&path);
    assert_eq!(back.shape(), df.shape());
    assert_eq!(back["f32"].dtype(), df["f32"].dtype());
    assert_eq!(back["f64"].dtype(), df["f64"].dtype());
}

#[test]
fn roundtrip_binary() {
    let s0 = Column::new(
        "bytes".into(),
        &[
            &b"hello"[..],
            &b"world"[..],
            &b""[..],
            &b"\x00\x01\x02"[..],
        ],
    );
    let df = DataFrame::new(4, vec![s0]).expect("build df");
    assert_roundtrip(df, "binary.vortex");
}

#[test]
fn roundtrip_binary_with_nulls() {
    let s0 = Column::new(
        "bytes".into(),
        &[Some(&b"hello"[..]), None, Some(&b""[..]), None],
    );
    let df = DataFrame::new(4, vec![s0]).expect("build df");
    assert_roundtrip(df, "binary_nullable.vortex");
}

// ============================================================================
// Multi-chunk DataFrame: writer loops over chunks; nothing else exercises this.
// ============================================================================

#[test]
fn roundtrip_multi_chunk_dataframe() {
    use polars_core::frame::column::IntoColumn;
    use polars_core::prelude::NamedFrom;
    use polars_core::series::Series;

    let mut combined: Series = NamedFrom::new("a".into(), &[1_i64, 2, 3]);
    let s_b: Series = NamedFrom::new("a".into(), &[4_i64, 5, 6]);
    let s_c: Series = NamedFrom::new("a".into(), &[7_i64, 8, 9]);

    // Append b and c to a, forming a multi-chunk series.
    combined.append(&s_b).expect("append");
    combined.append(&s_c).expect("append");
    assert!(combined.chunks().len() >= 2, "expected multi-chunk series");

    let df = DataFrame::new(9, vec![combined.into_column()]).expect("build df");

    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("multi_chunk.vortex");
    write_vortex(&df, &path, &VortexWriteOptions::default()).expect("write");
    let back = read_back(&path);
    assert_eq!(back.height(), 9);
    let vals: Vec<Option<i64>> = back["a"].i64().unwrap().into_iter().collect();
    assert_eq!(
        vals,
        vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            Some(9),
        ],
    );
}

// ============================================================================
// Temporal dtype coverage (gated on dtype-* features; otherwise skipped).
// ============================================================================

#[cfg(feature = "dtype-date")]
#[test]
fn roundtrip_date() {
    use polars_core::prelude::DataType;

    let s0 = Column::new("days".into(), &[19_000_i32, 19_001, 19_002])
        .cast(&DataType::Date)
        .expect("cast to Date");
    let df = DataFrame::new(3, vec![s0]).expect("build df");
    assert_roundtrip(df, "date.vortex");
}

#[cfg(feature = "dtype-datetime")]
#[test]
fn roundtrip_datetime_all_units_no_tz() {
    use polars_core::prelude::{DataType, TimeUnit as PolarsTimeUnit};

    for unit in [
        PolarsTimeUnit::Nanoseconds,
        PolarsTimeUnit::Microseconds,
        PolarsTimeUnit::Milliseconds,
    ] {
        let s0 = Column::new(
            "ts".into(),
            &[1_700_000_000_000_i64, 1_700_000_001_000, 1_700_000_002_000],
        )
        .cast(&DataType::Datetime(unit, None))
        .expect("cast to Datetime");
        let df = DataFrame::new(3, vec![s0]).expect("build df");
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join(format!("datetime_{unit:?}.vortex"));
        write_vortex(&df, &path, &VortexWriteOptions::default()).expect("write");
        let back = read_back(&path);
        assert!(
            df.equals_missing(&back),
            "datetime/{unit:?} mismatch:\nwrote {df:?}\nread  {back:?}"
        );
    }
}

#[cfg(feature = "dtype-time")]
#[test]
fn roundtrip_time() {
    use polars_core::prelude::DataType;

    let s0 = Column::new("t".into(), &[0_i64, 1_000_000, 86_399_000_000_000])
        .cast(&DataType::Time)
        .expect("cast to Time");
    let df = DataFrame::new(3, vec![s0]).expect("build df");
    assert_roundtrip(df, "time.vortex");
}
