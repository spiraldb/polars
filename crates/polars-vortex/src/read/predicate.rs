//! Polars predicate → Vortex `Expression` convertor (filter pushdown).
//!
//! We translate the structured pieces of [`polars_io::predicates::ScanIOPredicate`] into a
//! Vortex `Expression` to hand to `ScanBuilder::with_filter`. What we can't translate stays
//! as a residual filter, which the multi-scan layer applies post-decode (the streaming
//! reader advertises `PARTIAL_FILTER` capability so the multi-scan layer knows to keep the
//! original predicate around).
//!
//! ## What we translate
//!
//! The high-leverage path is [`ColumnPredicates`]: the Polars optimizer already extracts
//! single-column predicates into [`SpecializedColumnPredicate`] variants (Equal / Between /
//! EqualOneOf / StartsWith / EndsWith / RegexMatch). Each maps to a Vortex expression node
//! cleanly. We collect all per-column predicates and `and`-collect them into a single
//! filter.
//!
//! Multi-column predicates, arithmetic, struct field access, and `RegexMatch` stay as
//! residual for now (PR-3 follow-ups). They're correct because the multi-scan layer
//! always applies the original `predicate.predicate` post-decode.

use polars_core::prelude::AnyValue;
use polars_io::predicates::{ScanIOPredicate, SpecializedColumnPredicate};
use polars_utils::pl_str::PlSmallStr;
use vortex::array::scalar::Scalar as VortexScalar;
use vortex::dtype::Nullability;
use vortex::expr::{
    Expression, and_collect, eq, get_item, gt_eq, like, lit, lt_eq, or_collect, root,
};

/// Convert what we can of `scan_predicate` into a single Vortex filter expression. The
/// returned expression should be passed to `ScanBuilder::with_filter`; the multi-scan
/// layer is responsible for the residual (full `predicate.predicate` is re-applied to
/// emitted morsels).
///
/// Returns `None` when nothing pushable was found.
pub fn polars_to_vortex_predicate(scan_predicate: &ScanIOPredicate) -> Option<Expression> {
    let mut per_column = Vec::new();
    for (column_name, (_phys_expr, specialized_opt)) in scan_predicate.column_predicates.predicates.iter() {
        let Some(specialized) = specialized_opt else {
            continue;
        };
        if let Some(expr) = convert_specialized(column_name, specialized) {
            per_column.push(expr);
        }
    }
    and_collect(per_column)
}

fn convert_specialized(
    column_name: &PlSmallStr,
    specialized: &SpecializedColumnPredicate,
) -> Option<Expression> {
    let col = get_item(column_name.as_str(), root());

    Some(match specialized {
        SpecializedColumnPredicate::Equal(scalar) => {
            eq(col, lit(polars_scalar_to_vortex(scalar)?))
        }
        SpecializedColumnPredicate::Between(low, high) => {
            let lo = lit(polars_scalar_to_vortex(low)?);
            let hi = lit(polars_scalar_to_vortex(high)?);
            // Closed range: low <= col <= high.
            vortex::expr::and(gt_eq(col.clone(), lo), lt_eq(col, hi))
        }
        SpecializedColumnPredicate::EqualOneOf(scalars) => {
            let terms: Vec<Expression> = scalars
                .iter()
                .filter_map(|s| Some(eq(col.clone(), lit(polars_scalar_to_vortex(s)?))))
                .collect();
            // If every scalar in the IN-list converted, push the OR; if some failed we
            // could still push the partial set + leave a residual, but for safety we
            // require all-or-nothing here (otherwise the pushed filter is *narrower*
            // than the user's actual predicate, which would drop rows incorrectly).
            if terms.len() != scalars.len() {
                return None;
            }
            or_collect(terms)?
        }
        SpecializedColumnPredicate::StartsWith(bytes) => {
            let prefix = bytes_to_like_literal(bytes)?;
            // `prefix%`
            let pattern = format!("{prefix}%");
            like(col, lit(VortexScalar::utf8(pattern, Nullability::NonNullable)))
        }
        SpecializedColumnPredicate::EndsWith(bytes) => {
            let suffix = bytes_to_like_literal(bytes)?;
            // `%suffix`
            let pattern = format!("%{suffix}");
            like(col, lit(VortexScalar::utf8(pattern, Nullability::NonNullable)))
        }
        // No native regex in Vortex's `like`; let the multi-scan residual handle it.
        SpecializedColumnPredicate::RegexMatch(_) => return None,
    })
}

/// Validate that `bytes` is valid UTF-8 and free of SQL-LIKE special characters
/// (`%`, `_`, `\`). Returns the borrowed `&str` so the caller can build a pattern.
/// Returning `None` falls back to the residual filter, which is always correct.
///
/// We refuse pushdown when the bytes contain `%` or `_` because LIKE would interpret
/// those as wildcards, *widening* the predicate. Widening is still correct (the
/// multi-scan residual filter trims the extra rows), but it defeats the perf win
/// of pushdown — so we'd rather not push than push wastefully. Backslash is the
/// LIKE escape character; same reasoning.
fn bytes_to_like_literal(bytes: &[u8]) -> Option<&str> {
    let s = std::str::from_utf8(bytes).ok()?;
    if s.contains('%') || s.contains('_') || s.contains('\\') {
        return None;
    }
    Some(s)
}

/// Convert a Polars `Scalar` into a Vortex `Scalar` for the common primitive types.
/// Returns `None` for variants we don't yet translate (extension types, nested types,
/// etc.) — the caller treats this as "not pushable" and falls back to the residual.
fn polars_scalar_to_vortex(scalar: &polars_core::scalar::Scalar) -> Option<VortexScalar> {
    let nul = Nullability::Nullable;
    Some(match scalar.value() {
        AnyValue::Null => return None, // Vortex `null` requires a known DType; skip for now.
        AnyValue::Boolean(v) => VortexScalar::bool(*v, nul),
        AnyValue::UInt8(v) => VortexScalar::primitive(*v, nul),
        AnyValue::UInt16(v) => VortexScalar::primitive(*v, nul),
        AnyValue::UInt32(v) => VortexScalar::primitive(*v, nul),
        AnyValue::UInt64(v) => VortexScalar::primitive(*v, nul),
        AnyValue::Int8(v) => VortexScalar::primitive(*v, nul),
        AnyValue::Int16(v) => VortexScalar::primitive(*v, nul),
        AnyValue::Int32(v) => VortexScalar::primitive(*v, nul),
        AnyValue::Int64(v) => VortexScalar::primitive(*v, nul),
        AnyValue::Float32(v) => VortexScalar::primitive(*v, nul),
        AnyValue::Float64(v) => VortexScalar::primitive(*v, nul),
        AnyValue::String(s) => VortexScalar::utf8(s.to_string(), nul),
        AnyValue::StringOwned(s) => VortexScalar::utf8(s.to_string(), nul),
        AnyValue::Binary(b) => VortexScalar::binary(b.to_vec(), nul),
        AnyValue::BinaryOwned(b) => VortexScalar::binary(b.clone(), nul),
        _ => return None,
    })
}
