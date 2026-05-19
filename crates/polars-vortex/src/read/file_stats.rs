//! Vortex file-level statistics → Polars `TableStatistics` DataFrame.
//!
//! PR-3.1 leads the Polars file-stats pattern: Parquet's scan does not populate
//! `UnifiedScanArgs::table_statistics` today (it's hard-coded `None` at
//! `polars-plan/src/dsl/file_scan/mod.rs`). Vortex is the first format to
//! plumb file-level stats into Polars' skip-batch-predicate machinery so the
//! mem-engine can prune whole files BEFORE the streaming source spins up.
//!
//! ## DataFrame contract
//!
//! The shape expected by `polars-mem-engine/src/scan_predicate/functions.rs`
//! (search for `format_pl_smallstr!("{col}_min")`):
//!
//! | column      | dtype       | semantics                       |
//! | ----------- | ----------- | ------------------------------- |
//! | `len`       | `IDX_DTYPE` | total file row count            |
//! | `{col}_min` | column dtype | per-column min (null if unknown) |
//! | `{col}_max` | column dtype | per-column max (null if unknown) |
//! | `{col}_nc`  | `IDX_DTYPE` | per-column null count            |
//!
//! Per-file granularity: one row per source file (cycle 1 ships single-file
//! only; multi-file aggregation tracked at `.big-plans/vortex-integration.md`
//! Deferred entry "PR-3.1 cycle-2 multi-file stats aggregation").
//!
//! ## Soundness: Inexact stats refused
//!
//! Vortex's `StatsSet::get(Stat::Min)` returns `Option<Precision<ScalarValue>>`
//! where `Precision::Exact(v)` means the true min equals `v` and
//! `Precision::Inexact(v)` means the true min is within a Vortex-specific
//! bound around `v`. The polars-mem-engine `skip_batch_predicate` evaluator
//! treats `_min`/`_max` columns as EXACT scalars; using Inexact values would
//! produce false-positive skips (rows that should match get pruned).
//! Conservative refusal: only `Precision::Exact` is emitted; Inexact stats
//! produce null in the DataFrame so the mem-engine treats the file as
//! "unknown range" (no prune).

use polars_core::frame::DataFrame;
use polars_core::frame::column::Column;
use polars_core::prelude::{AnyValue, DataType, IDX_DTYPE};
use polars_core::scalar::Scalar;
use polars_core::schema::Schema;
use polars_error::PolarsResult;
use polars_utils::format_pl_smallstr;
use polars_utils::pl_str::PlSmallStr;
use vortex::array::scalar::{PValue, ScalarValue};
use vortex::expr::stats::{Precision, Stat};
use vortex::file::Footer;

/// Build a single-row Polars [`DataFrame`] matching the `TableStatistics`
/// contract from a Vortex [`Footer`]. Returns `None` if the footer has no
/// per-column statistics (`FileStatistics` absent).
///
/// `file_schema` is the schema discovered by [`crate::read::schema::vortex_dtype_to_schema`];
/// columns are iterated in declaration order which matches `stats_sets()[i]`
/// (Vortex stores per-field stats in struct-field declaration order; see
/// `FileStatistics::new_with_dtype` at `vortex-file/src/footer/file_statistics.rs:62`).
///
/// `row_count` is from `footer.row_count()` and populates the `len` column.
///
/// **Cycle 1 scope** (PR-3.1): handles primitive column types (Int8/16/32/64,
/// UInt8/16/32/64, Float32/64, Boolean, String). Complex dtypes (List, Struct,
/// Array, temporal extension types, Decimal) emit null in the `_min`/`_max`
/// columns but still emit a valid (typed-null) Series so the resulting
/// DataFrame schema matches mem-engine expectations. The `_nc` (null count)
/// column is always populated when present in the footer regardless of column
/// dtype.
pub fn footer_to_table_statistics(
    footer: &Footer,
    file_schema: &Schema,
) -> PolarsResult<Option<DataFrame>> {
    let Some(file_stats) = footer.statistics() else {
        return Ok(None);
    };

    let row_count = footer.row_count();
    let n_rows = 1; // single-file ⇒ single row

    // `len` column carries the file row count. Type IDX_DTYPE per mem-engine
    // schema (`scan_predicate/functions.rs:131`).
    let mut columns: Vec<Column> = Vec::with_capacity(1 + file_schema.len() * 3);
    columns.push(idx_column(
        PlSmallStr::from_static("len"),
        row_count,
        n_rows,
    ));

    // Iterate file_schema columns in declaration order; the i-th entry of
    // stats_sets corresponds to the i-th struct field per Vortex's
    // `FileStatistics::new_with_dtype` invariant.
    for (i, (col_name, col_dtype)) in file_schema.iter().enumerate() {
        if i >= file_stats.stats_sets().len() {
            // Defensive: footer's stats_sets shorter than schema. Skip
            // remaining columns rather than panic (the assertion in
            // `FileStatistics::new` makes this impossible in practice for
            // well-formed footers, but be defensive against truncated/corrupt
            // footers).
            break;
        }
        let stats_set = &file_stats.stats_sets()[i];

        // _min — only emit Exact values; Inexact and missing become null.
        let min_av = stats_set
            .get(Stat::Min)
            .and_then(precision_as_exact)
            .and_then(|sv| scalar_value_to_any_value(&sv, col_dtype));
        columns.push(scalar_to_typed_column(
            format_pl_smallstr!("{col_name}_min"),
            min_av,
            col_dtype,
        )?);

        // _max — same Exact-only discipline.
        let max_av = stats_set
            .get(Stat::Max)
            .and_then(precision_as_exact)
            .and_then(|sv| scalar_value_to_any_value(&sv, col_dtype));
        columns.push(scalar_to_typed_column(
            format_pl_smallstr!("{col_name}_max"),
            max_av,
            col_dtype,
        )?);

        // _nc — null count, always IDX_DTYPE. Exact-only too: an Inexact
        // null-count would mean "≥ this many nulls" which can't soundly feed
        // mem-engine's skip-batch evaluator (it does straight comparisons).
        let nc_value = stats_set
            .get(Stat::NullCount)
            .and_then(precision_as_exact)
            .and_then(|sv| match sv {
                ScalarValue::Primitive(pv) => pvalue_to_u64(pv),
                _ => None,
            });
        columns.push(opt_idx_column(
            format_pl_smallstr!("{col_name}_nc"),
            nc_value,
            n_rows,
        ));
    }

    Ok(Some(DataFrame::new(n_rows, columns)?))
}

/// Helper: unwrap a `Precision<ScalarValue>` into the inner value, but only
/// when `Exact`. Inexact stats are conservatively dropped (see module doc).
fn precision_as_exact(p: Precision<ScalarValue>) -> Option<ScalarValue> {
    match p {
        Precision::Exact(v) => Some(v),
        Precision::Inexact(_) => None,
    }
}

/// Convert a Vortex [`ScalarValue`] into a Polars [`AnyValue`] for the given
/// target [`DataType`]. Returns `None` for unsupported combinations (complex
/// dtypes, type mismatch).
fn scalar_value_to_any_value(sv: &ScalarValue, target_dt: &DataType) -> Option<AnyValue<'static>> {
    match (sv, target_dt) {
        (ScalarValue::Bool(b), DataType::Boolean) => Some(AnyValue::Boolean(*b)),
        (ScalarValue::Primitive(pv), DataType::Int8) => pv
            .as_i64()
            .and_then(|i| i8::try_from(i).ok().map(AnyValue::Int8)),
        (ScalarValue::Primitive(pv), DataType::Int16) => pv
            .as_i64()
            .and_then(|i| i16::try_from(i).ok().map(AnyValue::Int16)),
        (ScalarValue::Primitive(pv), DataType::Int32) => pv
            .as_i64()
            .and_then(|i| i32::try_from(i).ok().map(AnyValue::Int32)),
        (ScalarValue::Primitive(pv), DataType::Int64) => pv.as_i64().map(AnyValue::Int64),
        (ScalarValue::Primitive(pv), DataType::UInt8) => pv
            .as_u64()
            .and_then(|u| u8::try_from(u).ok().map(AnyValue::UInt8)),
        (ScalarValue::Primitive(pv), DataType::UInt16) => pv
            .as_u64()
            .and_then(|u| u16::try_from(u).ok().map(AnyValue::UInt16)),
        (ScalarValue::Primitive(pv), DataType::UInt32) => pv
            .as_u64()
            .and_then(|u| u32::try_from(u).ok().map(AnyValue::UInt32)),
        (ScalarValue::Primitive(pv), DataType::UInt64) => pv.as_u64().map(AnyValue::UInt64),
        (ScalarValue::Primitive(pv), DataType::Float32) => {
            pv.as_f64().map(|f| AnyValue::Float32(f as f32))
        },
        (ScalarValue::Primitive(pv), DataType::Float64) => pv.as_f64().map(AnyValue::Float64),
        (ScalarValue::Utf8(s), DataType::String) => {
            // Allocate owned to lift to 'static lifetime; the per-file stats
            // DataFrame outlives the borrowed Vortex Footer.
            Some(AnyValue::StringOwned(s.as_str().into()))
        },
        _ => None,
    }
}

/// Helper: PValue → u64 (saturating). Used only for null_count which is
/// non-negative.
fn pvalue_to_u64(pv: PValue) -> Option<u64> {
    match pv {
        PValue::U8(v) => Some(v as u64),
        PValue::U16(v) => Some(v as u64),
        PValue::U32(v) => Some(v as u64),
        PValue::U64(v) => Some(v),
        PValue::I8(v) if v >= 0 => Some(v as u64),
        PValue::I16(v) if v >= 0 => Some(v as u64),
        PValue::I32(v) if v >= 0 => Some(v as u64),
        PValue::I64(v) if v >= 0 => Some(v as u64),
        _ => None,
    }
}

/// Build a single-row `Column` of [`IDX_DTYPE`] holding `value`.
fn idx_column(name: PlSmallStr, value: u64, n_rows: usize) -> Column {
    let av = match IDX_DTYPE {
        DataType::UInt32 => AnyValue::UInt32(value as u32),
        DataType::UInt64 => AnyValue::UInt64(value),
        _ => unreachable!("IDX_DTYPE is always UInt32 or UInt64"),
    };
    Column::new_scalar(name, Scalar::new(IDX_DTYPE, av), n_rows)
}

/// Build a single-row IDX_DTYPE `Column` from `Option<u64>` (None → typed null).
fn opt_idx_column(name: PlSmallStr, value: Option<u64>, n_rows: usize) -> Column {
    match value {
        Some(v) => idx_column(name, v, n_rows),
        None => Column::full_null(name, n_rows, &IDX_DTYPE),
    }
}

/// Build a single-row `Column` of the given dtype from an `Option<AnyValue>`
/// (None → typed null). Used for `{col}_min` / `{col}_max`.
fn scalar_to_typed_column(
    name: PlSmallStr,
    value: Option<AnyValue<'static>>,
    dtype: &DataType,
) -> PolarsResult<Column> {
    let column = match value {
        Some(av) => Column::new_scalar(name, Scalar::new(dtype.clone(), av), 1),
        None => Column::full_null(name, 1, dtype),
    };
    Ok(column)
}

#[cfg(test)]
mod tests {
    use polars_core::prelude::*;
    use vortex::array::scalar::{PValue, ScalarValue};
    use vortex::expr::stats::Precision;

    use super::*;

    /// Smoke test: helpers compile and primitive conversion produces the
    /// expected AnyValue. Direct full-Footer construction is complex (requires
    /// building a Layout, segments, etc.) so end-to-end coverage stays at the
    /// e2e Python test layer; this layer tests the conversion plumbing.
    #[test]
    fn scalar_value_int32_exact_converts() {
        let sv = ScalarValue::Primitive(PValue::I32(42));
        let av = scalar_value_to_any_value(&sv, &DataType::Int32);
        assert_eq!(av, Some(AnyValue::Int32(42)));
    }

    /// PValue overflow into a narrower dtype refuses (sound — produces null
    /// in the DataFrame so mem-engine treats it as unknown).
    #[test]
    fn scalar_value_int64_overflow_int8_returns_none() {
        let sv = ScalarValue::Primitive(PValue::I64(i32::MAX as i64 + 1));
        let av = scalar_value_to_any_value(&sv, &DataType::Int8);
        assert_eq!(av, None);
    }

    /// Float conversion preserves value.
    #[test]
    fn scalar_value_float64_exact_converts() {
        let sv = ScalarValue::Primitive(PValue::F64(3.14));
        let av = scalar_value_to_any_value(&sv, &DataType::Float64);
        assert_eq!(av, Some(AnyValue::Float64(3.14)));
    }

    /// String conversion.
    #[test]
    fn scalar_value_string_converts() {
        let sv = ScalarValue::Utf8("hello".into());
        let av = scalar_value_to_any_value(&sv, &DataType::String);
        assert_eq!(av, Some(AnyValue::StringOwned("hello".into())));
    }

    /// Bool conversion.
    #[test]
    fn scalar_value_bool_converts() {
        let sv = ScalarValue::Bool(true);
        let av = scalar_value_to_any_value(&sv, &DataType::Boolean);
        assert_eq!(av, Some(AnyValue::Boolean(true)));
    }

    /// Type mismatch refuses (Int32 ScalarValue, Int64 target dtype) — sound,
    /// produces null. Note: deliberate refuse since callers might
    /// accidentally pass mismatched dtypes; the convertor stays strict.
    #[test]
    fn scalar_value_type_mismatch_returns_none() {
        let sv = ScalarValue::Bool(true);
        let av = scalar_value_to_any_value(&sv, &DataType::Int32);
        assert_eq!(av, None);
    }

    /// Inexact precision is refused even when the value is valid.
    #[test]
    fn precision_inexact_returns_none() {
        let p: Precision<ScalarValue> = Precision::Inexact(ScalarValue::Primitive(PValue::I32(42)));
        assert_eq!(precision_as_exact(p), None);
    }

    /// Exact precision returns the inner value.
    #[test]
    fn precision_exact_returns_value() {
        let p: Precision<ScalarValue> = Precision::Exact(ScalarValue::Primitive(PValue::I32(42)));
        assert_eq!(
            precision_as_exact(p),
            Some(ScalarValue::Primitive(PValue::I32(42)))
        );
    }

    /// pvalue_to_u64 handles non-negative signed correctly.
    #[test]
    fn pvalue_to_u64_signed_positive_converts() {
        assert_eq!(pvalue_to_u64(PValue::I64(100)), Some(100));
    }

    /// pvalue_to_u64 refuses negative signed (sound — null counts are
    /// non-negative; a negative would indicate corruption).
    #[test]
    fn pvalue_to_u64_signed_negative_returns_none() {
        assert_eq!(pvalue_to_u64(PValue::I64(-1)), None);
    }
}
