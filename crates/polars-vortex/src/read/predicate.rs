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
//! residual for now (tracked under PR-13 — aggressive AExpr-based pushdown). They're
//! correct because the multi-scan layer always applies the original
//! `predicate.predicate` post-decode.

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
///
/// Conjuncts are emitted in column-name sorted order so the resulting Vortex
/// `Expression` is deterministic — `ColumnPredicates::predicates` is a hash map
/// whose iteration order varies, and Vortex's pruning evaluator may short-circuit
/// left-to-right, so the order matters for both reproducibility and (potentially)
/// pruning effectiveness.
pub fn polars_to_vortex_predicate(scan_predicate: &ScanIOPredicate) -> Option<Expression> {
    let mut per_column_pairs: Vec<(&PlSmallStr, &SpecializedColumnPredicate)> = scan_predicate
        .column_predicates
        .predicates
        .iter()
        .filter_map(|(name, (_, specialized_opt))| {
            specialized_opt.as_ref().map(|s| (name, s))
        })
        .collect();
    per_column_pairs.sort_by(|(a, _), (b, _)| a.cmp(b));

    let per_column: Vec<Expression> = per_column_pairs
        .into_iter()
        .filter_map(|(name, specialized)| convert_specialized(name, specialized))
        .collect();
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

/// Convert a Polars `Scalar` into a Vortex `Scalar` for the common types.
/// Returns `None` for variants we don't yet translate (extension types we don't have
/// a Vortex analogue for, nested types, etc.) — the caller treats this as "not
/// pushable" and falls back to the residual filter.
///
/// Scalars are constructed with `Nullability::Nullable` since the optimizer's
/// `SpecializedColumnPredicate` doesn't carry the column's nullability. Vortex's
/// type system unifies nullability when comparing against a `NonNullable` column,
/// so this is correct but may reduce pruning effectiveness if Vortex's pruning
/// evaluator is stricter than its comparison evaluator.
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
        // Temporal and Decimal arms live in dedicated helpers so they can be
        // feature-gated cleanly on polars-core's dtype-* features without
        // ballooning this match.
        #[cfg(feature = "dtype-date")]
        AnyValue::Date(days) => temporal::date_scalar(*days, nul)?,
        #[cfg(feature = "dtype-datetime")]
        AnyValue::Datetime(value, unit, tz) => {
            temporal::datetime_scalar(*value, *unit, tz.map(|t| t.as_ref()), nul)?
        }
        #[cfg(feature = "dtype-datetime")]
        AnyValue::DatetimeOwned(value, unit, tz) => temporal::datetime_scalar(
            *value,
            *unit,
            tz.as_ref().map(|t| t.as_ref().as_ref()),
            nul,
        )?,
        #[cfg(feature = "dtype-time")]
        AnyValue::Time(ns) => temporal::time_scalar(*ns, nul)?,
        #[cfg(feature = "dtype-decimal")]
        AnyValue::Decimal(v, precision, scale) => {
            temporal::decimal_scalar(*v, *precision, *scale, nul)?
        }
        // Duration has no Vortex extension dtype analogue; fall through to residual.
        _ => return None,
    })
}

#[cfg(any(
    feature = "dtype-date",
    feature = "dtype-datetime",
    feature = "dtype-time",
    feature = "dtype-decimal",
))]
mod temporal {
    use vortex::array::scalar::Scalar as VortexScalar;
    use vortex::dtype::Nullability;

    #[cfg(any(feature = "dtype-date", feature = "dtype-datetime", feature = "dtype-time"))]
    use vortex::array::extension::datetime::TimeUnit as VortexTimeUnit;

    #[cfg(feature = "dtype-datetime")]
    use polars_core::prelude::TimeUnit as PolarsTimeUnit;

    #[cfg(feature = "dtype-date")]
    pub(super) fn date_scalar(days: i32, nul: Nullability) -> Option<VortexScalar> {
        use vortex::array::extension::datetime::Date;
        let ext = Date::new(VortexTimeUnit::Days, nul).erased();
        let storage = VortexScalar::primitive(days, nul);
        Some(VortexScalar::extension_ref(ext, storage))
    }

    #[cfg(feature = "dtype-datetime")]
    pub(super) fn datetime_scalar(
        value: i64,
        unit: PolarsTimeUnit,
        tz: Option<&str>,
        nul: Nullability,
    ) -> Option<VortexScalar> {
        use std::sync::Arc;

        use vortex::array::extension::datetime::Timestamp;

        let vortex_unit = match unit {
            PolarsTimeUnit::Nanoseconds => VortexTimeUnit::Nanoseconds,
            PolarsTimeUnit::Microseconds => VortexTimeUnit::Microseconds,
            PolarsTimeUnit::Milliseconds => VortexTimeUnit::Milliseconds,
        };
        let tz_arc: Option<Arc<str>> = tz.map(Arc::from);
        let ext = Timestamp::new_with_tz(vortex_unit, tz_arc, nul).erased();
        let storage = VortexScalar::primitive(value, nul);
        Some(VortexScalar::extension_ref(ext, storage))
    }

    #[cfg(feature = "dtype-time")]
    pub(super) fn time_scalar(ns: i64, nul: Nullability) -> Option<VortexScalar> {
        use vortex::array::extension::datetime::Time;
        let ext = Time::new(VortexTimeUnit::Nanoseconds, nul).erased();
        let storage = VortexScalar::primitive(ns, nul);
        Some(VortexScalar::extension_ref(ext, storage))
    }

    #[cfg(feature = "dtype-decimal")]
    pub(super) fn decimal_scalar(
        value: i128,
        precision: usize,
        scale: usize,
        nul: Nullability,
    ) -> Option<VortexScalar> {
        use vortex::dtype::DecimalDType;
        use vortex::scalar::DecimalValue;
        // Vortex's `DecimalDType::new(u8, i8)` has narrower precision/scale ranges
        // than Polars' `Decimal(usize, usize)`. A bare `as`-cast would silently
        // wrap for out-of-range values, producing a Vortex scalar with completely
        // wrong precision/scale that would mis-prune the scan. `try_into` on
        // failure → return None, so the convertor falls back to the residual
        // filter (which is always correct).
        let p: u8 = precision.try_into().ok()?;
        let s: i8 = scale.try_into().ok()?;
        let dtype = DecimalDType::new(p, s);
        Some(VortexScalar::decimal(
            DecimalValue::I128(value),
            dtype,
            nul,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn like_literal_safe_strings() {
        assert_eq!(bytes_to_like_literal(b"hello"), Some("hello"));
        assert_eq!(bytes_to_like_literal(b""), Some(""));
        assert_eq!(bytes_to_like_literal(b"a.b-c@d"), Some("a.b-c@d"));
    }

    #[test]
    fn like_literal_refuses_wildcards() {
        // SQL-LIKE special chars must NOT be pushed — they'd widen the predicate.
        assert_eq!(bytes_to_like_literal(b"hello%world"), None);
        assert_eq!(bytes_to_like_literal(b"foo_bar"), None);
        assert_eq!(bytes_to_like_literal(b"a\\b"), None);
        assert_eq!(bytes_to_like_literal(b"%"), None);
        assert_eq!(bytes_to_like_literal(b"_"), None);
        assert_eq!(bytes_to_like_literal(b"\\"), None);
    }

    #[test]
    fn like_literal_refuses_invalid_utf8() {
        assert_eq!(bytes_to_like_literal(&[0xff, 0xfe]), None);
    }

    #[test]
    fn scalar_primitive_types_convert() {
        use polars_core::scalar::Scalar;
        use polars_core::prelude::DataType;

        let s = Scalar::new(DataType::Int32, AnyValue::Int32(42));
        assert!(polars_scalar_to_vortex(&s).is_some());

        let s = Scalar::new(DataType::Float64, AnyValue::Float64(3.14));
        assert!(polars_scalar_to_vortex(&s).is_some());

        let s = Scalar::new(DataType::Boolean, AnyValue::Boolean(true));
        assert!(polars_scalar_to_vortex(&s).is_some());

        let s = Scalar::new(DataType::String, AnyValue::StringOwned("hello".into()));
        assert!(polars_scalar_to_vortex(&s).is_some());
    }

    #[test]
    fn scalar_null_does_not_convert() {
        use polars_core::scalar::Scalar;
        use polars_core::prelude::DataType;

        let s = Scalar::new(DataType::Int32, AnyValue::Null);
        // We don't yet translate AnyValue::Null because Vortex's `null` literal
        // needs a known target DType. The convertor returns None, which the
        // caller treats as "not pushable".
        assert!(polars_scalar_to_vortex(&s).is_none());
    }

    /// Helper: verify a scalar is of the expected `DType::Extension(...)` shape.
    /// Returns the extension id as a string for the caller to assert on.
    #[cfg(any(
        feature = "dtype-date",
        feature = "dtype-datetime",
        feature = "dtype-time",
    ))]
    fn extension_id_of(scalar: &VortexScalar) -> String {
        use vortex::dtype::DType;
        match scalar.dtype() {
            DType::Extension(ext) => ext.id().to_string(),
            other => panic!("expected Extension dtype, got {:?}", other),
        }
    }

    #[cfg(feature = "dtype-date")]
    #[test]
    fn date_scalar_is_vortex_date_days() {
        let s = temporal::date_scalar(19_000, Nullability::Nullable).expect("date scalar");
        assert_eq!(extension_id_of(&s), "vortex.date");
    }

    #[cfg(feature = "dtype-datetime")]
    #[test]
    fn datetime_scalar_units_map_correctly() {
        use polars_core::prelude::TimeUnit;
        // Each polars TimeUnit should produce a Vortex Timestamp with the matching unit.
        for unit in [TimeUnit::Nanoseconds, TimeUnit::Microseconds, TimeUnit::Milliseconds] {
            let s = temporal::datetime_scalar(0, unit, None, Nullability::Nullable)
                .expect("datetime scalar");
            assert_eq!(extension_id_of(&s), "vortex.timestamp");
        }
    }

    #[cfg(feature = "dtype-datetime")]
    #[test]
    fn datetime_scalar_with_timezone() {
        use polars_core::prelude::TimeUnit;
        let s = temporal::datetime_scalar(
            1_700_000_000_000_000_000,
            TimeUnit::Nanoseconds,
            Some("UTC"),
            Nullability::Nullable,
        )
        .expect("datetime scalar with tz");
        assert_eq!(extension_id_of(&s), "vortex.timestamp");
    }

    #[cfg(feature = "dtype-time")]
    #[test]
    fn time_scalar_is_nanoseconds() {
        // Polars time is always nanoseconds-since-midnight.
        let s = temporal::time_scalar(60_000_000_000, Nullability::Nullable).expect("time scalar");
        assert_eq!(extension_id_of(&s), "vortex.time");
    }

    #[cfg(feature = "dtype-decimal")]
    #[test]
    fn decimal_scalar_roundtrips_precision_and_scale() {
        use vortex::dtype::DType;

        let s = temporal::decimal_scalar(12_345, 10, 2, Nullability::Nullable)
            .expect("decimal scalar");
        match s.dtype() {
            DType::Decimal(dec, _) => {
                assert_eq!(dec.precision(), 10);
                assert_eq!(dec.scale(), 2);
            }
            other => panic!("expected Decimal dtype, got {:?}", other),
        }
    }

    #[cfg(feature = "dtype-decimal")]
    #[test]
    fn decimal_scalar_rejects_overflowing_precision_and_scale() {
        // u8::MAX is 255; usize::try_into::<u8> fails for 256.
        assert!(temporal::decimal_scalar(0, 256, 0, Nullability::Nullable).is_none());
        // i8::MAX is 127; usize::try_into::<i8> fails for 128.
        assert!(temporal::decimal_scalar(0, 10, 128, Nullability::Nullable).is_none());
    }

    #[cfg(feature = "dtype-date")]
    #[test]
    fn convertor_returns_pushable_for_date_predicate() {
        use polars_core::prelude::DataType;
        use polars_core::scalar::Scalar;
        use polars_io::predicates::SpecializedColumnPredicate;

        let scalar = Scalar::new(DataType::Date, AnyValue::Date(19_000));
        let pred = SpecializedColumnPredicate::Equal(scalar);
        let expr = convert_specialized(&"d".into(), &pred);
        assert!(expr.is_some(), "Date equality should be pushable when dtype-date is on");
    }

    // ========================================================================
    // Per-variant pushdown-engagement tests: each `SpecializedColumnPredicate`
    // variant we claim to support should produce a non-None Vortex expression.
    // ========================================================================

    fn int32_scalar(v: i32) -> polars_core::scalar::Scalar {
        use polars_core::prelude::DataType;
        use polars_core::scalar::Scalar;
        Scalar::new(DataType::Int32, AnyValue::Int32(v))
    }

    #[test]
    fn equal_predicate_is_pushable() {
        use polars_io::predicates::SpecializedColumnPredicate;
        let pred = SpecializedColumnPredicate::Equal(int32_scalar(42));
        assert!(convert_specialized(&"a".into(), &pred).is_some());
    }

    #[test]
    fn between_predicate_is_pushable() {
        use polars_io::predicates::SpecializedColumnPredicate;
        let pred = SpecializedColumnPredicate::Between(int32_scalar(1), int32_scalar(10));
        assert!(convert_specialized(&"a".into(), &pred).is_some());
    }

    #[test]
    fn equal_one_of_predicate_is_pushable() {
        use polars_io::predicates::SpecializedColumnPredicate;
        let pred = SpecializedColumnPredicate::EqualOneOf(
            vec![int32_scalar(1), int32_scalar(2), int32_scalar(3)].into_boxed_slice(),
        );
        assert!(convert_specialized(&"a".into(), &pred).is_some());
    }

    #[test]
    fn starts_with_predicate_is_pushable_for_safe_bytes() {
        use polars_io::predicates::SpecializedColumnPredicate;
        let pred = SpecializedColumnPredicate::StartsWith(b"hello".to_vec().into_boxed_slice());
        assert!(convert_specialized(&"s".into(), &pred).is_some());
    }

    #[test]
    fn starts_with_predicate_refuses_unsafe_bytes() {
        // Wildcard chars trigger the safety check — return None so the residual
        // filter handles it correctly.
        use polars_io::predicates::SpecializedColumnPredicate;
        let pred =
            SpecializedColumnPredicate::StartsWith(b"hello%".to_vec().into_boxed_slice());
        assert!(convert_specialized(&"s".into(), &pred).is_none());
    }

    #[test]
    fn ends_with_predicate_is_pushable_for_safe_bytes() {
        use polars_io::predicates::SpecializedColumnPredicate;
        let pred = SpecializedColumnPredicate::EndsWith(b"world".to_vec().into_boxed_slice());
        assert!(convert_specialized(&"s".into(), &pred).is_some());
    }

    #[test]
    fn regex_match_predicate_falls_back_to_residual() {
        // RegexMatch is documented as residual-only.
        use polars_io::predicates::SpecializedColumnPredicate;
        let regex = regex::bytes::Regex::new("^foo").unwrap();
        let pred = SpecializedColumnPredicate::RegexMatch(regex);
        assert!(convert_specialized(&"s".into(), &pred).is_none());
    }

    #[test]
    fn equal_one_of_with_partial_failure_returns_none() {
        // Documented behavior: if any scalar in the IN-list fails to convert
        // (e.g., AnyValue::Null), the whole predicate falls back to residual
        // — pushing a partial set would be narrower than the user's actual
        // predicate, which would silently drop rows.
        use polars_core::prelude::DataType;
        use polars_core::scalar::Scalar;
        use polars_io::predicates::SpecializedColumnPredicate;
        let pred = SpecializedColumnPredicate::EqualOneOf(
            vec![
                int32_scalar(1),
                Scalar::new(DataType::Int32, AnyValue::Null),
                int32_scalar(3),
            ]
            .into_boxed_slice(),
        );
        assert!(convert_specialized(&"a".into(), &pred).is_none());
    }
}
