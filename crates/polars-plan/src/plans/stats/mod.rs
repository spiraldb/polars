//! Plan-time statistics.
//!
//! [`ScanStats`] is what a scan leaf resolves at plan time. [`node_stats`]
//! estimates the same shape for any node of the IR, and [`subplan_cost`] sums that
//! over a whole subtree.

mod cost;
#[cfg(any(feature = "python", test))]
mod dataset;
mod node;

use std::sync::Arc;

pub(crate) use cost::subplan_cost;
#[cfg(feature = "python")]
pub(crate) use dataset::dataset_scan_stats;
pub use node::{
    NodeStats, composite_key_domain, join_cardinality, key_domain, node_stats, unique_side_rows,
};
pub(crate) use node::{SharedKeyColumns, StatsCache, node_stats_with_cache};
use polars_core::prelude::Scalar;
#[allow(clippy::disallowed_types)]
use polars_utils::aliases::PlHashMap;
use polars_utils::pl_str::PlSmallStr;

use crate::plans::IR;

/// Relative error for an estimate that carries no better information.
pub(crate) const DEFAULT_REL_ERR: f32 = 0.5;

/// A cardinality: a count that may be unknown, guaranteed, or estimated.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "dsl-schema", derive(schemars::JsonSchema))]
pub enum Card {
    #[default]
    Unknown,
    /// Guaranteed by the source, e.g. a parquet footer or an Iceberg manifest.
    Exact(u64),
    /// Derived. `rel_err` is a rough 1-sigma relative error.
    Approx { value: u64, rel_err: f32 },
}

impl Card {
    /// An estimate with the default relative error.
    pub fn approx(value: u64) -> Self {
        Card::Approx {
            value,
            rel_err: DEFAULT_REL_ERR,
        }
    }

    /// The value, exact or estimated.
    pub fn value(self) -> Option<u64> {
        match self {
            Card::Unknown => None,
            Card::Exact(v) | Card::Approx { value: v, .. } => Some(v),
        }
    }

    /// The value, only when it is known to within `max_rel_err`.
    pub fn confident(self, max_rel_err: f32) -> Option<u64> {
        match self {
            Card::Unknown => None,
            Card::Exact(v) => Some(v),
            Card::Approx { value, rel_err } => (rel_err <= max_rel_err).then_some(value),
        }
    }

    /// Turn a guarantee into an estimate.
    pub fn demote(self, rel_err: f32) -> Self {
        match self {
            Card::Unknown => Card::Unknown,
            Card::Exact(value) => Card::Approx { value, rel_err },
            Card::Approx { value, rel_err: e } => Card::Approx {
                value,
                rel_err: e.max(rel_err),
            },
        }
    }

    /// [`Card::demote`] with the default relative error.
    pub fn demote_default(self) -> Self {
        self.demote(DEFAULT_REL_ERR)
    }

    fn rel_err(self) -> f32 {
        match self {
            Card::Unknown => f32::INFINITY,
            Card::Exact(_) => 0.0,
            Card::Approx { rel_err, .. } => rel_err,
        }
    }

    fn checked_add(self, other: Self) -> Self {
        match (self, other) {
            (Card::Exact(left), Card::Exact(right)) => {
                left.checked_add(right).map_or(Card::Unknown, Card::Exact)
            },
            (left, right) => match left.value().zip(right.value()) {
                Some((left_value, right_value)) => {
                    left_value
                        .checked_add(right_value)
                        .map_or(Card::Unknown, |value| Card::Approx {
                            value,
                            rel_err: left.rel_err().max(right.rel_err()),
                        })
                },
                None => Card::Unknown,
            },
        }
    }

    /// Apply `f` to the value, keeping the confidence.
    pub fn map(self, f: impl FnOnce(u64) -> u64) -> Self {
        match self {
            Card::Unknown => Card::Unknown,
            Card::Exact(v) => Card::Exact(f(v)),
            Card::Approx { value, rel_err } => Card::Approx {
                value: f(value),
                rel_err,
            },
        }
    }
}

/// Statistics for one column, keyed on the output column name, i.e. after column
/// mapping.
#[derive(Clone, Debug, Default, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "dsl-schema", derive(schemars::JsonSchema))]
pub struct ScanColumnStats {
    /// Number of distinct non-null values.
    pub distinct: Card,
    pub null_count: Card,
    /// Average width of one value in the source, in bytes.
    pub avg_byte_width: Option<f32>,
    /// Inclusive value range of an integer column, folded over the chunks that were
    /// read. When the chunks were sampled, it is widened by the mean gap between
    /// them and is an estimate.
    #[cfg_attr(feature = "serde", serde(default))]
    pub int_range: Option<(i128, i128)>,
    /// Guaranteed inclusive outer bounds for every non-null integer value.
    /// Unlike `int_range`, these bounds never come from sampling or extrapolation.
    /// A wide interval does not prove that its values occur or are distinct.
    #[cfg_attr(feature = "serde", serde(default))]
    pub int_range_bounds: Option<(i128, i128)>,
    /// `int_range` misses sources whose statistics were not read, or comes from a
    /// single sampled row group, so values far outside it may exist.
    #[cfg_attr(feature = "serde", serde(default))]
    pub int_range_partial: bool,
    /// The statistics each sampled row group records of this column (bounds, counts
    /// and sizes), present only when `POLARS_SCAN_CHUNK_STATISTICS` is enabled.
    #[cfg_attr(feature = "serde", serde(skip))]
    #[cfg_attr(feature = "dsl-schema", schemars(skip))]
    /// One entry per sampled row group, ordered by source file, and by row group within a
    /// file.
    pub sampled_row_groups: Option<Arc<[RowGroupStats]>>,
}

/// How many bytes one stored value of a parquet column takes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValueWidth {
    /// Every value takes this many bytes, e.g. 4 for INT32 and 8 for INT64.
    Fixed(u32),
    /// Booleans, which are stored as bits.
    Bits,
    /// Byte arrays, whose values differ in length.
    Variable,
}

/// The statistics of one parquet column in one row group.
#[derive(Clone, Debug, PartialEq)]
pub struct RowGroupStats {
    pub rows: u64,
    pub null_count: Option<u64>,
    /// The number of distinct values, as read from the row group's statistics. Most
    /// writers don't write this.
    pub distinct_count: Option<u64>,
    /// How the column chunk stores its values, which writers choose differently for
    /// some types, e.g. a decimal as INT32 or as a fixed-length byte array.
    pub value_width: ValueWidth,
    /// The size in bytes of the column chunk once decompressed.
    pub uncompressed_size: u64,
    /// The size in bytes of the column chunk's dictionary page as stored on disk
    /// (compressed), if it has one.
    pub dictionary_page_size: Option<u64>,
    /// Whether some data pages aren't dictionary-encoded although the chunk has a
    /// dictionary page. Arrow and parquet-mr switch to plain encoding once the
    /// dictionary grows too large, so the dictionary then holds only part of the values.
    /// `None` when the writer doesn't record page encodings; Polars doesn't.
    pub dictionary_fallback: Option<bool>,
    /// Combined length in bytes of all string or binary values in the column chunk.
    /// Every occurrence counts, but nulls don't. `None` for other column types, or
    /// when the writer didn't write size statistics. Arrow, parquet-rs, parquet-java
    /// and Polars write them by default.
    pub unencoded_byte_array_size: Option<u64>,
    /// The minimum and maximum value, typed as the column, or as its values for a
    /// categorical column. `None` when the row group doesn't record both exactly, or
    /// they can't be converted to that type.
    pub bounds: Option<(Scalar, Scalar)>,
}

impl ScanColumnStats {
    /// Values the column could hold, from its integer range.
    pub fn int_domain(&self) -> Option<f64> {
        let (min, max) = self.int_range?;
        (max >= min).then(|| range_width(min, max))
    }
}

/// Values in `min..=max`. The count can exceed `i128`, e.g. for a 128-bit decimal.
pub(crate) fn range_width(min: i128, max: i128) -> f64 {
    max.checked_sub(min)
        .and_then(|width| width.checked_add(1))
        .map_or(max as f64 - min as f64 + 1.0, |width| width as f64)
}

// We don't index
#[allow(clippy::disallowed_types)]
pub type ScanColumnStatsMap = PlHashMap<PlSmallStr, ScanColumnStats>;

/// Plan-time statistics for a scan.
///
/// Derivative: takes no part in plan equality or hashing.
#[derive(Clone, Debug, Default, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "dsl-schema", derive(schemars::JsonSchema))]
pub struct ScanStats {
    #[cfg_attr(feature = "serde", serde(default))]
    pub rows: Card,
    #[cfg_attr(feature = "serde", serde(default))]
    #[cfg_attr(
        feature = "dsl-schema",
        schemars(with = "Option<Vec<(PlSmallStr, ScanColumnStats)>>")
    )]
    columns: Option<Arc<ScanColumnStatsMap>>,
}

impl ScanStats {
    pub fn unknown() -> Self {
        Self::default()
    }

    pub fn new(rows: Card) -> Self {
        ScanStats {
            rows,
            columns: None,
        }
    }

    pub fn exact_rows(rows: u64) -> Self {
        Self::new(Card::Exact(rows))
    }

    pub fn approx_rows(rows: u64) -> Self {
        Self::new(Card::approx(rows))
    }

    pub fn with_columns(mut self, columns: ScanColumnStatsMap) -> Self {
        self.columns = (!columns.is_empty()).then(|| Arc::new(columns));
        self
    }

    pub fn column(&self, name: &str) -> Option<&ScanColumnStats> {
        self.columns.as_ref()?.get(name)
    }

    /// Statistics of one scan that reads the sources of `parts` one after another. A column
    /// keeps statistics only if every part has them.
    pub(crate) fn concat(parts: &[&Self]) -> Self {
        // Exact empty inputs contribute no values, even without column metadata.
        let parts: Vec<_> = parts
            .iter()
            .copied()
            .filter(|part| part.rows != Card::Exact(0))
            .collect();
        if parts.is_empty() {
            return Self::exact_rows(0);
        }
        let rows = parts
            .iter()
            .map(|s| s.rows)
            .reduce(Card::checked_add)
            .unwrap();
        let Some(maps) = parts
            .iter()
            .map(|s| s.columns.as_deref())
            .collect::<Option<Vec<_>>>()
        else {
            return Self::new(rows);
        };

        let columns = maps[0]
            .iter()
            .filter_map(|(name, first)| {
                let mut out = first.clone();
                let mut accumulated_rows = parts[0].rows;
                let mut width_rows = accumulated_rows.value().unwrap_or(0) as f64;
                for (map, part) in maps.iter().zip(&parts).skip(1) {
                    let col = map.get(name)?;
                    let left_empty = non_null_empty(&out, accumulated_rows);
                    let right_empty = non_null_empty(col, part.rows);
                    let distinct = union_distinct(&out, col, left_empty, right_empty);
                    let part_rows = part.rows.value().unwrap_or(0) as f64;
                    out.null_count = out.null_count.checked_add(col.null_count);
                    out.avg_byte_width =
                        out.avg_byte_width.zip(col.avg_byte_width).map(|(l, r)| {
                            let total = width_rows + part_rows;
                            if total == 0.0 {
                                (l + r) / 2.0
                            } else {
                                ((l as f64 * width_rows + r as f64 * part_rows) / total) as f32
                            }
                        });
                    out.int_range =
                        union_range(out.int_range, col.int_range, left_empty, right_empty);
                    out.int_range_bounds = union_range(
                        out.int_range_bounds,
                        col.int_range_bounds,
                        left_empty,
                        right_empty,
                    );
                    out.int_range_partial = match (left_empty, right_empty) {
                        (true, false) => col.int_range_partial,
                        (false, true) => out.int_range_partial,
                        (true, true) => false,
                        (false, false) => out.int_range_partial || col.int_range_partial,
                    };
                    out.distinct = distinct;
                    // The first input's sample does not describe the concatenation.
                    out.sampled_row_groups = None;
                    accumulated_rows = accumulated_rows.checked_add(part.rows);
                    width_rows += part_rows;
                }
                Some((name.clone(), out))
            })
            .collect();
        Self::new(rows).with_columns(columns)
    }
}

fn non_null_empty(column: &ScanColumnStats, rows: Card) -> bool {
    column.distinct == Card::Exact(0)
        || matches!((rows, column.null_count), (Card::Exact(rows), Card::Exact(nulls)) if rows == nulls)
}

fn union_range(
    left: Option<(i128, i128)>,
    right: Option<(i128, i128)>,
    left_empty: bool,
    right_empty: bool,
) -> Option<(i128, i128)> {
    match (left_empty, right_empty) {
        (true, true) => None,
        (true, false) => right,
        (false, true) => left,
        (false, false) => left
            .zip(right)
            .map(|((l_min, l_max), (r_min, r_max))| (l_min.min(r_min), l_max.max(r_max))),
    }
}

fn union_distinct(
    left: &ScanColumnStats,
    right: &ScanColumnStats,
    left_empty: bool,
    right_empty: bool,
) -> Card {
    match (left_empty, right_empty) {
        (true, true) => return Card::Exact(0),
        (true, false) => return right.distinct,
        (false, true) => return left.distinct,
        (false, false) => {},
    }
    let (Card::Exact(left_count), Card::Exact(right_count)) = (left.distinct, right.distinct)
    else {
        return Card::Unknown;
    };
    let Some(((left_min, left_max), (right_min, right_max))) = left
        .int_range_bounds
        .zip(right.int_range_bounds)
        .filter(|((l_min, l_max), (r_min, r_max))| l_min <= l_max && r_min <= r_max)
    else {
        return Card::Unknown;
    };
    if left_max < right_min || right_max < left_min {
        return left_count
            .checked_add(right_count)
            .map_or(Card::Unknown, Card::Exact);
    }
    if left_count == 1
        && right_count == 1
        && left_min == left_max
        && left_min == right_min
        && right_min == right_max
    {
        return Card::Exact(1);
    }
    Card::Unknown
}

/// Row count of a leaf node.
pub fn leaf_row_count(ir: &IR) -> Card {
    match ir {
        IR::Scan { file_info, .. } => file_info.stats.rows,
        IR::DataFrameScan { df, .. } => Card::Exact(df.height() as u64),
        _ => Card::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int_domain_beyond_i128() {
        let max = 10i128.pow(38) - 1;
        let stats = ScanColumnStats {
            int_range: Some((-max, max)),
            ..Default::default()
        };
        assert_eq!(stats.int_domain(), Some(2e38));
    }

    fn scan(rows: u64, distinct: Card, bounds: Option<(i128, i128)>) -> ScanStats {
        ScanStats::exact_rows(rows).with_columns(ScanColumnStatsMap::from_iter([(
            PlSmallStr::from_static("key"),
            ScanColumnStats {
                distinct,
                null_count: Card::Exact(0),
                int_range: bounds,
                int_range_bounds: bounds,
                ..Default::default()
            },
        )]))
    }

    #[test]
    fn concat_requires_disjoint_guaranteed_bounds_for_exact_union() {
        let left = scan(100, Card::Exact(1), Some((7, 7)));
        let right = scan(50, Card::Exact(1), Some((42, 42)));
        let union = ScanStats::concat(&[&left, &right]);
        let column = union.column("key").unwrap();
        assert_eq!(column.distinct, Card::Exact(2));
        assert_eq!(column.int_range_bounds, Some((7, 42)));

        let same = ScanStats::concat(&[&left, &left]);
        assert_eq!(same.column("key").unwrap().distinct, Card::Exact(1));

        let overlapping = scan(100, Card::Exact(5), Some((0, 100)));
        let union = ScanStats::concat(&[&overlapping, &overlapping]);
        assert_eq!(union.column("key").unwrap().distinct, Card::Unknown);

        let mut estimated = right.clone();
        Arc::make_mut(estimated.columns.as_mut().unwrap())
            .get_mut("key")
            .unwrap()
            .int_range_bounds = None;
        let union = ScanStats::concat(&[&left, &estimated]);
        assert_eq!(union.column("key").unwrap().distinct, Card::Unknown);
        assert_eq!(union.column("key").unwrap().int_range, Some((7, 42)));
        assert_eq!(union.column("key").unwrap().int_range_bounds, None);
    }

    #[test]
    fn concat_empty_inputs_and_null_only_values_are_identity_elements() {
        let empty = ScanStats::exact_rows(0);
        assert_eq!(ScanStats::concat(&[]).rows, Card::Exact(0));
        assert_eq!(ScanStats::concat(&[&empty]).rows, Card::Exact(0));
        let values = scan(100, Card::Exact(1), Some((7, 7)));
        let mut nulls = scan(3, Card::Exact(0), None);
        Arc::make_mut(nulls.columns.as_mut().unwrap())
            .get_mut("key")
            .unwrap()
            .null_count = Card::Exact(3);
        let union = ScanStats::concat(&[&empty, &nulls, &values, &empty]);
        assert_eq!(union.rows, Card::Exact(103));
        let column = union.column("key").unwrap();
        assert_eq!(column.distinct, Card::Exact(1));
        assert_eq!(column.null_count, Card::Exact(3));
        assert_eq!(column.int_range_bounds, Some((7, 7)));
    }

    #[test]
    fn concat_count_overflow_does_not_saturate_to_an_exact_value() {
        let left = scan(
            u64::MAX,
            Card::Exact(u64::MAX),
            Some((0, i128::from(u64::MAX) - 1)),
        );
        let right = scan(
            1,
            Card::Exact(1),
            Some((i128::from(u64::MAX), i128::from(u64::MAX))),
        );
        let union = ScanStats::concat(&[&left, &right]);
        assert_eq!(union.rows, Card::Unknown);
        let column = union.column("key").unwrap();
        assert_eq!(column.distinct, Card::Unknown);
        assert_eq!(column.null_count, Card::Exact(0));
        assert_eq!(column.int_domain(), Some(2f64.powi(64)));
        assert_eq!(
            Card::Exact(u64::MAX).checked_add(Card::Exact(1)),
            Card::Unknown
        );
    }
}
