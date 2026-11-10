//! Conservative file statistics for the Vortex scan optimizer.

use polars_core::prelude::{DataType, Schema};
use polars_vortex::vortex::array::expr::stats::{Precision, Stat};
use polars_vortex::vortex::dtype::{DType, FieldPath, Nullability, PType};
use polars_vortex::vortex::file::Footer;
use polars_vortex::vortex::layout::layouts::file_stats::AggregateStats;
use polars_vortex::vortex::scalar::Scalar;

use crate::plans::stats::{Card, ScanColumnStats, ScanColumnStatsMap, ScanStats};

const FOOTER_BUDGET: usize = 8;

pub(super) fn footer_indices(n_sources: usize) -> Vec<usize> {
    let samples = n_sources.min(FOOTER_BUDGET);
    match samples {
        0 => Vec::new(),
        1 => vec![0],
        _ => (0..samples)
            .map(|index| {
                ((index as u128 * (n_sources - 1) as u128) / (samples - 1) as u128) as usize
            })
            .collect(),
    }
}

pub(super) fn aggregate_footers(
    footers: &[Option<polars_vortex::read::VortexFooterRef>],
    schema: &Schema,
    n_sources: usize,
) -> ScanStats {
    if n_sources == 0 {
        return ScanStats::exact_rows(0);
    }
    if footers.is_empty() || footers.len() > n_sources {
        return ScanStats::unknown();
    }
    let Some(parts) = footers
        .iter()
        .map(|footer| {
            let footer = footer.as_ref()?;
            Some(
                ScanStats::exact_rows(footer.row_count())
                    .with_columns(column_stats(footer, schema, 1)),
            )
        })
        .collect::<Option<Vec<_>>>()
    else {
        let rows = footers
            .first()
            .and_then(Option::as_ref)
            .and_then(|footer| {
                footer
                    .row_count()
                    .checked_mul(u64::try_from(n_sources).ok()?)
            })
            .map_or(Card::Unknown, Card::approx);
        return ScanStats::new(rows);
    };
    let combined = ScanStats::concat(&parts.iter().collect::<Vec<_>>());
    if footers.len() == n_sources {
        return combined;
    }
    let rows = footers
        .iter()
        .try_fold(0u128, |sum, footer| {
            sum.checked_add(u128::from(footer.as_ref()?.row_count()))
        })
        .and_then(|sum| sum.checked_mul(n_sources as u128))
        .map(|sum| sum / footers.len() as u128)
        .and_then(|rows| u64::try_from(rows).ok())
        .map_or(Card::Unknown, Card::approx);
    let columns = schema
        .iter_names()
        .filter_map(|name| {
            let mut stats = combined.column(name.as_str())?.clone();
            stats.distinct = Card::Unknown;
            stats.null_count = Card::Unknown;
            stats.int_range_bounds = None;
            stats.int_range_partial = true;
            stats.sampled_row_groups = None;
            Some((name.clone(), stats))
        })
        .collect();
    ScanStats::new(rows).with_columns(columns)
}

pub(super) fn column_stats(
    footer: &Footer,
    schema: &Schema,
    n_sources: usize,
) -> ScanColumnStatsMap {
    let mut columns = ScanColumnStatsMap::default();
    // This extractor describes one footer, never an extrapolated set of files.
    if n_sources != 1 {
        return columns;
    }
    let DType::Struct(fields, Nullability::NonNullable) = footer.dtype() else {
        return columns;
    };
    let statistics = footer.statistics();
    for (name, dtype) in schema.iter() {
        let Some(ptype) = integer_ptype(dtype) else {
            continue;
        };
        let Some(field_dtype) = fields.field(name.as_str()) else {
            continue;
        };
        if !matches!(&field_dtype, DType::Primitive(p, _) if *p == ptype) {
            continue;
        }
        let metadata = statistics
            .and_then(|statistics| statistics.get_by_path(&FieldPath::from_name(name.as_str())));
        let stats = match metadata {
            // The statistics must describe the actual root field, not its storage cast.
            Some((aggregates, stat_dtype)) if stat_dtype == &field_dtype => {
                integer_column_stats(aggregates, ptype, footer.row_count())
            },
            None if footer.row_count() == 0 => ScanColumnStats {
                distinct: Card::Exact(0),
                null_count: Card::Exact(0),
                ..Default::default()
            },
            _ => continue,
        };
        columns.insert(name.clone(), stats);
    }
    columns
}

fn integer_column_stats(aggregates: &AggregateStats, ptype: PType, rows: u64) -> ScanColumnStats {
    let get = |stat: Stat| aggregates.get(&stat.aggregate_fn().unwrap());
    let null_count = if rows == 0 {
        Card::Exact(0)
    } else {
        get(Stat::NullCount)
            .as_exact()
            .filter(|value| matches!(value.dtype(), DType::Primitive(PType::U64, _)))
            .and_then(|value| u64::try_from(&value).ok())
            .filter(|&count| count <= rows)
            .map_or(Card::Unknown, Card::Exact)
    };
    // Inexact minima are lower bounds. Inexact maxima are upper bounds.
    let bound = |stat| match get(stat) {
        Precision::Exact(value) => integer_value(&value, ptype).map(|value| (value, true)),
        Precision::Inexact(value) => integer_value(&value, ptype).map(|value| (value, false)),
        Precision::Absent => None,
    };
    let bounds = bound(Stat::Min)
        .zip(bound(Stat::Max))
        .filter(|((min, _), (max, _))| min <= max)
        .filter(|_| rows != 0 && null_count != Card::Exact(rows));
    let int_range = bounds.map(|((min, _), (max, _))| (min, max));
    let distinct = if rows == 0 || null_count == Card::Exact(rows) {
        Card::Exact(0)
    } else if bounds.is_some_and(|((min, min_exact), (max, max_exact))| {
        min == max
            && ((min_exact && max_exact)
                || matches!(null_count, Card::Exact(nulls) if nulls < rows))
    }) {
        // Exact extrema or an exact count prove that a non-null value occurs.
        Card::Exact(1)
    } else {
        Card::Unknown
    };
    ScanColumnStats {
        distinct,
        null_count,
        int_range,
        int_range_bounds: int_range,
        ..Default::default()
    }
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
        assert_eq!(columns["all_null"].distinct, Card::Exact(0));
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

    fn aggregates(
        min: Precision<Scalar>,
        max: Precision<Scalar>,
        nulls: Precision<Scalar>,
    ) -> AggregateStats {
        use polars_vortex::vortex::layout::layouts::file_stats::AggregateStat;
        AggregateStats::new(
            [(Stat::Min, min), (Stat::Max, max), (Stat::NullCount, nulls)]
                .into_iter()
                .map(|(stat, value)| AggregateStat::from_value(stat.aggregate_fn().unwrap(), value))
                .collect(),
        )
    }

    #[test]
    fn inexact_typed_bounds_are_outer_bounds_not_distinct_counts() {
        let stats = aggregates(
            Precision::Inexact(Scalar::primitive(-100i64, Nullability::NonNullable)),
            Precision::Inexact(Scalar::primitive(100i64, Nullability::NonNullable)),
            Precision::Exact(Scalar::primitive(2u64, Nullability::NonNullable)),
        );
        let column = integer_column_stats(&stats, PType::I64, 10);
        assert_eq!(column.int_range_bounds, Some((-100, 100)));
        assert_eq!(column.int_range, Some((-100, 100)));
        assert_eq!(column.null_count, Card::Exact(2));
        assert_eq!(column.distinct, Card::Unknown);

        let wrong_type = integer_column_stats(&stats, PType::U64, 10);
        assert_eq!(wrong_type.int_range_bounds, None);
        assert_eq!(wrong_type.distinct, Card::Unknown);
    }

    #[test]
    fn singleton_bounds_need_proof_that_a_non_null_value_exists() {
        let point = || Precision::Inexact(Scalar::primitive(7i64, Nullability::NonNullable));
        let stats = aggregates(point(), point(), Precision::Absent);
        assert_eq!(
            integer_column_stats(&stats, PType::I64, 10).distinct,
            Card::Unknown
        );
        let stats = aggregates(
            point(),
            point(),
            Precision::Inexact(Scalar::primitive(0u64, Nullability::NonNullable)),
        );
        let column = integer_column_stats(&stats, PType::I64, 10);
        assert_eq!(column.null_count, Card::Unknown);
        assert_eq!(column.distinct, Card::Unknown);
        let stats = aggregates(
            point(),
            point(),
            Precision::Exact(Scalar::primitive(2u64, Nullability::NonNullable)),
        );
        assert_eq!(
            integer_column_stats(&stats, PType::I64, 10).distinct,
            Card::Exact(1)
        );
        let stats = aggregates(
            point(),
            point(),
            Precision::Exact(Scalar::primitive(10u64, Nullability::NonNullable)),
        );
        let column = integer_column_stats(&stats, PType::I64, 10);
        assert_eq!(column.distinct, Card::Exact(0));
        assert_eq!(column.int_range_bounds, None);
    }

    #[test]
    fn absent_inverted_or_invalid_count_statistics_remain_unknown() {
        let empty = integer_column_stats(&AggregateStats::default(), PType::I64, 0);
        assert_eq!(empty.distinct, Card::Exact(0));
        assert_eq!(empty.null_count, Card::Exact(0));
        let absent = integer_column_stats(&AggregateStats::default(), PType::I64, 10);
        assert_eq!(absent.distinct, Card::Unknown);
        assert_eq!(absent.int_range_bounds, None);
        let stats = aggregates(
            Precision::Exact(Scalar::primitive(100i64, Nullability::NonNullable)),
            Precision::Exact(Scalar::primitive(-100i64, Nullability::NonNullable)),
            Precision::Exact(Scalar::primitive(11u64, Nullability::NonNullable)),
        );
        let column = integer_column_stats(&stats, PType::I64, 10);
        assert_eq!(column.int_range_bounds, None);
        assert_eq!(column.null_count, Card::Unknown);
        assert_eq!(column.distinct, Card::Unknown);
    }

    #[test]
    fn real_constant_files_union_without_counting_null_as_a_value() {
        use crate::plans::stats::ScanStats;
        let left = DataFrame::new(
            3,
            vec![Column::new("key".into(), [Some(7i64), None, Some(7)])],
        )
        .unwrap();
        let right = DataFrame::new(2, vec![Column::new("key".into(), [42i64, 42])]).unwrap();
        let all_null =
            DataFrame::new(2, vec![Column::new("key".into(), [None::<i64>; 2])]).unwrap();
        let scan = |df: &DataFrame| {
            ScanStats::exact_rows(df.height() as u64).with_columns(column_stats(
                &footer(df),
                df.schema().as_ref(),
                1,
            ))
        };
        let left = scan(&left);
        let right = scan(&right);
        let all_null = scan(&all_null);
        assert_eq!(left.column("key").unwrap().distinct, Card::Exact(1));
        assert_eq!(all_null.column("key").unwrap().distinct, Card::Exact(0));
        let combined = ScanStats::concat(&[&left, &all_null, &right]);
        let key = combined.column("key").unwrap();
        assert_eq!(combined.rows, Card::Exact(7));
        assert_eq!(key.distinct, Card::Exact(2));
        assert_eq!(key.null_count, Card::Exact(3));
        assert_eq!(key.int_range_bounds, Some((7, 42)));
    }

    #[test]
    fn sparse_file_bounds_do_not_become_exact_distinct_counts() {
        let df = DataFrame::new(
            4,
            vec![Column::new("key".into(), [1i64, 1_000_000, 1, 1_000_000])],
        )
        .unwrap();
        let columns = column_stats(&footer(&df), df.schema().as_ref(), 1);
        let key = &columns["key"];
        assert_eq!(key.int_range_bounds, Some((1, 1_000_000)));
        assert_eq!(key.distinct, Card::Unknown);
    }

    fn integer_footer(values: &[Option<i64>]) -> polars_vortex::read::VortexFooterRef {
        let df = DataFrame::new(values.len(), vec![Column::new("key".into(), values)]).unwrap();
        std::sync::Arc::new(footer(&df))
    }

    #[test]
    fn footer_sample_is_bounded_uniform_and_includes_both_ends() {
        assert!(footer_indices(0).is_empty());
        assert_eq!(footer_indices(1), vec![0]);
        assert_eq!(footer_indices(8), (0..8).collect::<Vec<_>>());
        assert_eq!(footer_indices(100), vec![0, 14, 28, 42, 56, 70, 84, 99]);
        let indices = footer_indices(usize::MAX);
        assert_eq!(indices.len(), 8);
        assert_eq!(indices[0], 0);
        assert_eq!(indices[7], usize::MAX - 1);
        assert!(indices.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn complete_footer_coverage_sums_actual_rows_and_proves_disjoint_constants() {
        let schema = Schema::from_iter([("key".into(), DataType::Int64)]);
        let footers = [
            Some(integer_footer(&[Some(7), None, Some(7)])),
            Some(integer_footer(&[Some(42), Some(42)])),
            Some(integer_footer(&[None; 4])),
        ];
        let stats = aggregate_footers(&footers, &schema, 3);
        assert_eq!(stats.rows, Card::Exact(9));
        let key = stats.column("key").unwrap();
        assert_eq!(key.distinct, Card::Exact(2));
        assert_eq!(key.null_count, Card::Exact(5));
        assert_eq!(key.int_range_bounds, Some((7, 42)));
        let overlap = [
            Some(integer_footer(&[Some(1), Some(3)])),
            Some(integer_footer(&[Some(3), Some(5)])),
        ];
        let stats = aggregate_footers(&overlap, &schema, 2);
        assert_eq!(stats.rows, Card::Exact(4));
        assert_eq!(stats.column("key").unwrap().distinct, Card::Unknown);
        assert_eq!(stats.column("key").unwrap().int_range_bounds, Some((1, 5)));
    }

    #[test]
    fn sampled_coverage_removes_all_global_guarantees() {
        let schema = Schema::from_iter([("key".into(), DataType::Int64)]);
        let footers = [
            Some(integer_footer(&[Some(7), None, Some(7)])),
            Some(integer_footer(&[Some(42), Some(42)])),
        ];
        let stats = aggregate_footers(&footers, &schema, 10);
        assert_eq!(stats.rows, Card::approx(25));
        let key = stats.column("key").unwrap();
        assert_eq!(key.distinct, Card::Unknown);
        assert_eq!(key.null_count, Card::Unknown);
        assert_eq!(key.int_range, Some((7, 42)));
        assert!(key.int_range_partial);
        assert_eq!(key.int_range_bounds, None);
        assert!(key.sampled_row_groups.is_none());
    }

    #[test]
    fn failed_extra_footer_uses_only_first_row_estimate_without_column_stats() {
        let schema = Schema::from_iter([("key".into(), DataType::Int64)]);
        let footers = [
            Some(integer_footer(&[Some(7); 3])),
            None,
            Some(integer_footer(&[Some(42); 20])),
        ];
        let stats = aggregate_footers(&footers, &schema, 10);
        assert_eq!(stats.rows, Card::approx(30));
        assert!(stats.column("key").is_none());
    }

    #[test]
    fn incompatible_sample_types_drop_stats_without_schema_inference_errors() {
        let schema = Schema::from_iter([("key".into(), DataType::Int64)]);
        let unsigned = DataFrame::new(2, vec![Column::new("key".into(), [7u64, 42])]).unwrap();
        let footers = [
            Some(integer_footer(&[Some(7); 3])),
            Some(std::sync::Arc::new(footer(&unsigned))),
        ];
        let stats = aggregate_footers(&footers, &schema, 2);
        assert_eq!(stats.rows, Card::Exact(5));
        assert!(stats.column("key").is_none());
    }
}
