//! Conservative file statistics for the Vortex scan optimizer.

use polars_core::prelude::{DataType, Schema};
use polars_vortex::vortex::array::expr::stats::{Precision, Stat};
use polars_vortex::vortex::dtype::{DType, FieldPath, Nullability, PType};
use polars_vortex::vortex::file::Footer;
use polars_vortex::vortex::scalar::Scalar;

use crate::plans::stats::{Card, ScanColumnStats, ScanColumnStatsMap};

pub(super) fn column_stats(
    footer: &Footer,
    schema: &Schema,
    n_sources: usize,
) -> ScanColumnStatsMap {
    let mut columns = ScanColumnStatsMap::default();
    // Only the first footer is available during schema inference.
    if n_sources != 1 {
        return columns;
    }
    let DType::Struct(fields, Nullability::NonNullable) = footer.dtype() else {
        return columns;
    };
    let Some(statistics) = footer.statistics() else {
        return columns;
    };
    for (name, dtype) in schema.iter() {
        let Some(ptype) = integer_ptype(dtype) else {
            continue;
        };
        let Some((aggregates, stat_dtype)) =
            statistics.get_by_path(&FieldPath::from_name(name.as_str()))
        else {
            continue;
        };
        // Both the logical scan schema and the actual footer field must agree.
        if !matches!(stat_dtype, DType::Primitive(p, _) if *p == ptype)
            || fields.field(name.as_str()).as_ref() != Some(stat_dtype)
        {
            continue;
        }
        let exact = |stat: Stat| match aggregates.get(&stat.aggregate_fn().unwrap()) {
            Precision::Exact(value) => Some(value),
            Precision::Inexact(_) | Precision::Absent => None,
        };
        let null_count = exact(Stat::NullCount)
            .filter(|value| matches!(value.dtype(), DType::Primitive(PType::U64, _)))
            .and_then(|value| u64::try_from(&value).ok())
            .filter(|&count| count <= footer.row_count())
            .map_or(Card::Unknown, Card::Exact);
        let int_range = exact(Stat::Min)
            .zip(exact(Stat::Max))
            .and_then(|(min, max)| Some((integer_value(&min, ptype)?, integer_value(&max, ptype)?)))
            .filter(|(min, max)| min <= max)
            .filter(|_| null_count != Card::Exact(footer.row_count()));
        columns.insert(
            name.clone(),
            ScanColumnStats {
                null_count,
                int_range,
                ..Default::default()
            },
        );
    }
    columns
}

fn integer_ptype(dtype: &DataType) -> Option<PType> {
    Some(match dtype {
        DataType::Int8 => PType::I8,
        DataType::Int16 => PType::I16,
        DataType::Int32 => PType::I32,
        DataType::Int64 => PType::I64,
        DataType::UInt8 => PType::U8,
        DataType::UInt16 => PType::U16,
        DataType::UInt32 => PType::U32,
        DataType::UInt64 => PType::U64,
        _ => return None,
    })
}

fn integer_value(value: &Scalar, ptype: PType) -> Option<i128> {
    if !matches!(value.dtype(), DType::Primitive(p, _) if *p == ptype) {
        return None;
    }
    match ptype {
        PType::U8 | PType::U16 | PType::U32 | PType::U64 => {
            u64::try_from(value).ok().map(i128::from)
        },
        PType::I8 | PType::I16 | PType::I32 | PType::I64 => {
            i64::try_from(value).ok().map(i128::from)
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use polars_core::prelude::{Column, DataFrame};
    use polars_core::runtime::ASYNC;
    use polars_vortex::vortex::file::OpenOptionsSessionExt;
    use polars_vortex::write::write_vortex;
    use polars_vortex::{VortexWriteOptions, session};

    use super::*;

    fn footer(df: &DataFrame) -> Footer {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("stats.vortex");
        write_vortex(df, &path, &VortexWriteOptions::default()).unwrap();
        ASYNC.block_in_place_on(async {
            session()
                .open_options()
                .open_path(&path)
                .await
                .unwrap()
                .footer()
                .clone()
        })
    }

    #[test]
    fn actual_footer_integer_bounds_and_null_counts() {
        let df = DataFrame::new(
            3,
            vec![
                Column::new("signed".into(), [Some(-17i64), None, Some(42)]),
                Column::new("unsigned".into(), [Some(0u64), Some(u64::MAX), None]),
                Column::new("all_null".into(), [None::<i64>; 3]),
                Column::new("float".into(), [1.0f64, f64::NAN, 2.0]),
            ],
        )
        .unwrap();
        let footer = footer(&df);
        let columns = column_stats(&footer, df.schema().as_ref(), 1);
        assert_eq!(columns["signed"].int_range, Some((-17, 42)));
        assert_eq!(columns["signed"].null_count, Card::Exact(1));
        assert_eq!(
            columns["unsigned"].int_range,
            Some((0, i128::from(u64::MAX)))
        );
        assert_eq!(columns["unsigned"].null_count, Card::Exact(1));
        assert_eq!(columns["all_null"].null_count, Card::Exact(3));
        assert_eq!(columns["all_null"].int_range, None);
        assert!(!columns.contains_key("float"));
    }

    #[test]
    fn first_footer_never_describes_multiple_files_or_schema_casts() {
        let df = DataFrame::new(2, vec![Column::new("key".into(), [1i64, 25])]).unwrap();
        let footer = footer(&df);
        assert!(column_stats(&footer, df.schema().as_ref(), 2).is_empty());
        let schema = Schema::from_iter([("key".into(), DataType::UInt64)]);
        assert!(column_stats(&footer, &schema, 1).is_empty());
    }

    #[test]
    fn integer_scalar_requires_matching_logical_type() {
        let value = Scalar::primitive(u64::MAX, Nullability::NonNullable);
        assert_eq!(
            integer_value(&value, PType::U64),
            Some(i128::from(u64::MAX))
        );
        assert_eq!(integer_value(&value, PType::I64), None);
    }
    #[test]
    fn empty_file_has_no_integer_domain() {
        let schema = Schema::from_iter([("key".into(), DataType::Int64)]);
        let df = DataFrame::empty_with_schema(&schema);
        let footer = footer(&df);
        assert_eq!(footer.row_count(), 0);
        let columns = column_stats(&footer, &schema, 1);
        if let Some(stats) = columns.get("key") {
            assert_eq!(stats.int_range, None);
            assert!(matches!(stats.null_count, Card::Unknown | Card::Exact(0)));
        }
    }

    #[test]
    fn dotted_field_name_and_integer_widths_match_actual_footer() {
        let df = DataFrame::new(
            3,
            vec![
                Column::new(
                    "key.with.dot".into(),
                    [Some(i32::MIN), None, Some(i32::MAX)],
                ),
                Column::new("unsigned".into(), [Some(0u32), Some(u32::MAX), None]),
            ],
        )
        .unwrap();
        let footer = footer(&df);
        let columns = column_stats(&footer, df.schema().as_ref(), 1);
        assert_eq!(
            columns["key.with.dot"].int_range,
            Some((i128::from(i32::MIN), i128::from(i32::MAX)))
        );
        assert_eq!(columns["key.with.dot"].null_count, Card::Exact(1));
        assert_eq!(
            columns["unsigned"].int_range,
            Some((0, i128::from(u32::MAX)))
        );
        assert_eq!(columns["unsigned"].null_count, Card::Exact(1));
    }

    #[test]
    #[cfg(feature = "dtype-u8")]
    fn unsigned_byte_bounds_match_actual_footer() {
        let df = DataFrame::new(
            3,
            vec![Column::new("key".into(), [Some(0u8), Some(u8::MAX), None])],
        )
        .unwrap();
        let footer = footer(&df);
        let columns = column_stats(&footer, df.schema().as_ref(), 1);
        assert_eq!(columns["key"].int_range, Some((0, 255)));
        assert_eq!(columns["key"].null_count, Card::Exact(1));
    }
}
