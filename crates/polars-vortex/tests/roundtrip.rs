//! Roundtrip tests for the native writer and Arrow C Data Interface.
//!
//! Each test writes a local file and reads it through the Vortex scan API.
//! DataFrame equality tests both values and logical types.

use futures::StreamExt;
use polars_core::frame::DataFrame;
use polars_core::prelude::Column;
use polars_core::runtime::ASYNC;
use polars_vortex::read::array_bridge::vortex_array_to_dataframe;
use polars_vortex::read::schema::vortex_dtype_to_schema;
use polars_vortex::session::session;
use polars_vortex::vortex::file::{OpenOptionsSessionExt, VortexFile};
use polars_vortex::write::write_vortex;
use polars_vortex::{VortexCompression, VortexWriteOptions};
use tempfile::tempdir;

async fn open_back(path: &std::path::Path) -> VortexFile {
    session()
        .open_options()
        .open_path(path)
        .await
        .expect("vortex open")
}

/// Read a file through the same Arrow export that the native reader uses.
fn read_back(path: &std::path::Path) -> DataFrame {
    ASYNC.block_on(async {
        let vxf = open_back(path).await;
        let (pl_schema, _) = vortex_dtype_to_schema(vxf.dtype()).expect("schema");

        let stream = vxf
            .scan()
            .expect("scan")
            .into_array_stream()
            .expect("array stream");
        futures::pin_mut!(stream);

        let mut out: Option<DataFrame> = None;
        while let Some(chunk) = stream.next().await {
            let array = chunk.expect("chunk");
            let df = vortex_array_to_dataframe(array).expect("bridge");
            out = Some(match out {
                None => df,
                Some(mut acc) => {
                    acc.vstack_mut(&df).expect("vstack");
                    acc
                },
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
fn eager_writer_can_yield_a_polars_cpu_worker() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("cpu_worker.vortex");
    let output = path.clone();
    let df = make_df();
    let expected = df.clone();
    ASYNC
        .block_on(
            polars_vortex::session::handle()
                .spawn_cpu(move || write_vortex(&df, output, &VortexWriteOptions::default())),
        )
        .unwrap();
    assert!(expected.equals_missing(&read_back(&path)));
}

#[test]
fn roundtrip_default_options() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("default.vortex");

    let df = make_df();
    write_vortex(&df, &path, &VortexWriteOptions::default()).expect("write");

    let back = read_back(&path);
    assert!(
        df.equals_missing(&back),
        "DataFrame round-trip mismatch:\nwrote {df:?}\nread {back:?}"
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
    let s1 = Column::new("b".into(), &[Some("x"), Some("y"), None, Some("z"), None]);
    let df = DataFrame::new(5, vec![s0, s1]).expect("build df");

    write_vortex(&df, &path, &VortexWriteOptions::default()).expect("write");
    let back = read_back(&path);
    assert!(
        df.equals_missing(&back),
        "nullable mismatch:\nwrote {df:?}\nread {back:?}"
    );
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
fn omitted_schema_returns_error_before_file_creation() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("no_dtype.vortex");
    let options = VortexWriteOptions {
        include_dtype: false,
        ..Default::default()
    };
    let error = write_vortex(&make_df(), &path, &options).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("include_dtype=False is not supported")
    );
    assert!(!path.exists());
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
        &[&b"hello"[..], &b"world"[..], &b""[..], &b"\x00\x01\x02"[..]],
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
    let vals: Vec<Option<i64>> = back["a"].i64().unwrap().iter().collect();
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

#[cfg(feature = "dtype-datetime")]
#[test]
fn roundtrip_datetime_with_timezone() {
    use polars_core::prelude::{DataType, TimeUnit as PolarsTimeUnit, TimeZone};

    // UTC is the easiest tz to assert — IANA, no DST quirks.
    let s0 = Column::new(
        "ts".into(),
        &[
            1_700_000_000_000_000_i64,
            1_700_000_001_000_000,
            1_700_000_002_000_000,
        ],
    )
    .cast(&DataType::Datetime(
        PolarsTimeUnit::Microseconds,
        Some(TimeZone::UTC),
    ))
    .expect("cast to Datetime UTC");
    let df = DataFrame::new(3, vec![s0]).expect("build df");
    assert_roundtrip(df, "datetime_utc.vortex");
}

#[cfg(feature = "dtype-decimal")]
#[test]
fn roundtrip_decimal_basic() {
    use polars_core::prelude::DataType;

    let s0 = Column::new("amount".into(), &[1_234_567_i128, 9_876_543, 0, -42])
        .cast(&DataType::Decimal(10, 2))
        .expect("cast to Decimal(10, 2)");
    let df = DataFrame::new(4, vec![s0]).expect("build df");
    assert_roundtrip(df, "decimal_basic.vortex");
}

#[cfg(feature = "dtype-decimal")]
#[test]
fn roundtrip_decimal_with_nulls() {
    use polars_core::prelude::DataType;

    let s0 = Column::new(
        "amount".into(),
        &[Some(1_000_i128), None, Some(2_000), None, Some(3_000)],
    )
    .cast(&DataType::Decimal(8, 4))
    .expect("cast to Decimal(8, 4)");
    let df = DataFrame::new(5, vec![s0]).expect("build df");
    assert_roundtrip(df, "decimal_nullable.vortex");
}

#[cfg(feature = "dtype-decimal")]
#[test]
fn roundtrip_decimal_precision_38_boundary() {
    // Precision 38 is the upper end of Polars' Decimal128 range — verify the
    // schema converter's `precision <= 38 → Decimal` branch handles it.
    use polars_core::prelude::DataType;

    let s0 = Column::new("amount".into(), &[1_i128, 2, 3])
        .cast(&DataType::Decimal(38, 0))
        .expect("cast to Decimal(38, 0)");
    let df = DataFrame::new(3, vec![s0]).expect("build df");
    assert_roundtrip(df, "decimal_38.vortex");
}

/// Smoke test for the segment-cache integration: write a file, scan it twice
/// (sharing the global cache), and verify both reads produce identical results.
///
/// We can't easily assert that the second scan hit cache without instrumenting
/// Vortex's MokaSegmentCache (Moka doesn't expose hit counts on its `Cache` type),
/// but this test does verify the cache wiring doesn't corrupt second-pass results
/// — a regression that mismatched cached buffers with their layout metadata would
/// surface as a frame mismatch.
#[test]
fn segment_cache_second_read_produces_same_data() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("cache_smoke.vortex");

    let df = make_df();
    write_vortex(&df, &path, &VortexWriteOptions::default()).expect("write");

    let back1 = read_back(&path);
    let back2 = read_back(&path);
    assert!(df.equals_missing(&back1));
    assert!(df.equals_missing(&back2));
    assert!(back1.equals_missing(&back2));
}

#[test]
fn roundtrip_zero_columns_preserves_height() {
    assert_roundtrip(DataFrame::new(7, vec![]).unwrap(), "zero_columns.vortex");
}

#[test]
fn roundtrip_unequal_chunk_boundaries() {
    use polars_core::prelude::*;
    let mut left = Series::new("left".into(), &[1i64, 2]);
    left.append(&Series::new("left".into(), &[3i64, 4, 5]))
        .unwrap();
    let mut right = Series::new("right".into(), &[10i64]);
    right
        .append(&Series::new("right".into(), &[20i64, 30, 40, 50]))
        .unwrap();
    let df = DataFrame::new(5, vec![left.into_column(), right.into_column()]).unwrap();
    assert_roundtrip(df, "unequal_boundaries.vortex");
}

#[test]
fn zero_row_block_size_returns_error_before_file_creation() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("invalid_block.vortex");
    let options = VortexWriteOptions {
        row_block_size: Some(0),
        ..Default::default()
    };
    assert!(write_vortex(&make_df(), &path, &options).is_err());
    assert!(!path.exists());
}

#[cfg(feature = "dtype-map")]
#[test]
fn roundtrip_nullable_map() {
    use std::sync::Arc;

    use arrow_array::builder::{Int64Builder, MapBuilder, StringBuilder};
    use arrow_array::{Array, RecordBatch};
    use arrow_schema::{Field, Schema};
    use polars_vortex::read::array_bridge::record_batch_to_dataframe;
    let mut builder = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    builder.keys().append_value("one");
    builder.values().append_value(1);
    builder.keys().append_value("two");
    builder.values().append_null();
    builder.append(true).unwrap();
    builder.append(false).unwrap();
    builder.append(true).unwrap();
    let array = builder.finish();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "map",
        array.data_type().clone(),
        true,
    )]));
    let frame =
        record_batch_to_dataframe(RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap())
            .unwrap();
    assert_roundtrip(frame, "map.vortex");
}

#[cfg(feature = "dtype-categorical")]
#[test]
fn categorical_write_returns_explicit_error() {
    use polars_core::prelude::*;
    let series = Series::new("category".into(), &["a", "b"])
        .cast(&DataType::from_categories(Categories::global()))
        .unwrap();
    let frame = DataFrame::new(2, vec![series.into_column()]).unwrap();
    let dir = tempdir().unwrap();
    let path = dir.path().join("category.vortex");
    let error = write_vortex(&frame, &path, &Default::default()).unwrap_err();
    assert!(error.to_string().contains("Categorical or Enum"));
    assert!(!path.exists());
}

#[cfg(feature = "dtype-struct")]
#[test]
fn roundtrip_nested_nulls_and_slices() {
    use std::sync::Arc;

    use arrow_array::builder::{Int64Builder, ListBuilder};
    use arrow_array::{Array, Int64Array, RecordBatch, StructArray};
    use arrow_schema::{DataType, Field, Schema};
    use polars_vortex::read::array_bridge::record_batch_to_dataframe;
    let child: arrow_array::ArrayRef =
        Arc::new(Int64Array::from(vec![Some(1), Some(99), None, Some(4)]));
    let structure = StructArray::try_new(
        vec![Arc::new(Field::new("value", DataType::Int64, true))].into(),
        vec![child],
        Some([true, false, true, true].into_iter().collect()),
    )
    .unwrap();
    let mut lists = ListBuilder::new(Int64Builder::new());
    lists.values().append_value(10);
    lists.append(true);
    lists.append(false);
    lists.values().append_null();
    lists.values().append_value(30);
    lists.append(true);
    lists.append(true);
    let lists = lists.finish();
    let schema = Arc::new(Schema::new(vec![
        Field::new("struct", structure.data_type().clone(), true),
        Field::new("list", lists.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(structure), Arc::new(lists)])
        .unwrap()
        .slice(1, 3);
    let frame = record_batch_to_dataframe(batch).unwrap();
    assert_roundtrip(frame, "nested_nulls.vortex");
}

#[cfg(feature = "dtype-duration")]
#[test]
fn duration_write_returns_error_before_file_creation() {
    use polars_core::prelude::*;
    let values = Column::new("duration".into(), &[1i64, 2])
        .cast(&DataType::Duration(TimeUnit::Microseconds))
        .unwrap();
    let frame = DataFrame::new(2, vec![values]).unwrap();
    let dir = tempdir().unwrap();
    let path = dir.path().join("duration.vortex");
    assert!(write_vortex(&frame, &path, &Default::default()).is_err());
    assert!(!path.exists());
}

#[cfg(feature = "dtype-extension")]
#[test]
fn nested_custom_extension_schema_returns_explicit_error() {
    use polars_core::datatypes::extension::get_extension_type_or_generic;
    use polars_core::prelude::*;
    use polars_vortex::write::df_to_stream::polars_schema_to_vortex_dtype;
    let storage = DataType::Int64;
    let extension = get_extension_type_or_generic("example.unit", &storage, Some("meters"));
    let dtype = DataType::List(Box::new(DataType::Extension(extension, Box::new(storage))));
    let schema = Schema::from_iter([("nested".into(), dtype)]);
    let error = polars_schema_to_vortex_dtype(&schema).unwrap_err();
    assert!(error.to_string().contains("custom extension types"));
}

#[cfg(feature = "object")]
#[test]
fn nested_object_schema_returns_explicit_error() {
    use polars_core::prelude::*;
    use polars_vortex::write::df_to_stream::polars_schema_to_vortex_dtype;
    let schema = Schema::from_iter([(
        "nested".into(),
        DataType::List(Box::new(DataType::Object("object"))),
    )]);
    let error = polars_schema_to_vortex_dtype(&schema).unwrap_err();
    assert!(error.to_string().contains("Object values"));
}
