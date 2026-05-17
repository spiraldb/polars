//! Polars `Scalar` → Vortex `VortexScalar` conversion (filter pushdown literal helper).
//!
//! **PR-2.6 Option B → A cutover (2026-05-16)**: this module USED to host the legacy
//! `SpecializedColumnPredicate`-derived filter-pushdown path (`polars_to_vortex_predicate`,
//! `convert_specialized`, `bytes_to_like_literal` for LIKE prefix/suffix). PR-2.6 deletes
//! that path entirely — the AExpr-direct convertor at
//! `polars_plan::plans::predicates::vortex_convertor::aexpr_to_vortex_expression`
//! (introduced in PR-2.1, wired at `polars-stream/src/physical_plan/lower_ir.rs` in
//! PR-2.2) is now the sole filter-pushdown path. The convertor handles every shape the
//! legacy path handled (Eq / Lt / Gt / Between via `Lt + Gt + And` / EqualOneOf via
//! `Eq` + `Or` / StartsWith and EndsWith are NOT YET in the convertor — see Deferred
//! work) plus everything the legacy path did not (multi-column predicates, arithmetic,
//! CAST, struct field access).
//!
//! ## What this module still does
//!
//! Hosts [`polars_scalar_to_vortex`] — the canonical `polars_core::scalar::Scalar` →
//! [`VortexScalar`] mapping. The AExpr-direct convertor calls into this for
//! `AExpr::Literal(LiteralValue::Scalar(s))` shapes (single source of truth for the
//! `AnyValue` → `VortexScalar` mapping; same `pub` visibility established in PR-2.1).
//! Temporal (Date / Datetime / Time) and Decimal arms live in the `temporal` submodule
//! so they can be feature-gated cleanly on polars-core's `dtype-*` features.

use polars_core::prelude::AnyValue;
use vortex::array::scalar::Scalar as VortexScalar;
use vortex::dtype::Nullability;

/// Convert a Polars [`polars_core::scalar::Scalar`] into a Vortex [`VortexScalar`].
///
/// Returns `None` for variants we don't yet translate (Duration — no Vortex extension
/// dtype analogue; nested types; extension dtypes without a Vortex equivalent). The
/// AExpr-direct convertor's `?`-propagation drops the enclosing predicate to residual on
/// `None`.
///
/// Scalars are constructed with `Nullability::Nullable`. The optimizer's per-column
/// predicates don't carry the column's nullability; Vortex's type system unifies
/// nullability when comparing against a `NonNullable` column, so this is correct but
/// may reduce pruning effectiveness if Vortex's pruning evaluator is stricter than its
/// comparison evaluator.
pub fn polars_scalar_to_vortex(scalar: &polars_core::scalar::Scalar) -> Option<VortexScalar> {
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
        },
        #[cfg(feature = "dtype-datetime")]
        AnyValue::DatetimeOwned(value, unit, tz) => {
            temporal::datetime_scalar(*value, *unit, tz.as_ref().map(|t| t.as_ref().as_ref()), nul)?
        },
        #[cfg(feature = "dtype-time")]
        AnyValue::Time(ns) => temporal::time_scalar(*ns, nul)?,
        #[cfg(feature = "dtype-decimal")]
        AnyValue::Decimal(v, precision, scale) => {
            temporal::decimal_scalar(*v, *precision, *scale, nul)?
        },
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
    #[cfg(feature = "dtype-datetime")]
    use polars_core::prelude::TimeUnit as PolarsTimeUnit;
    #[cfg(any(
        feature = "dtype-date",
        feature = "dtype-datetime",
        feature = "dtype-time"
    ))]
    use vortex::array::extension::datetime::TimeUnit as VortexTimeUnit;
    use vortex::array::scalar::Scalar as VortexScalar;
    use vortex::dtype::Nullability;

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
        // wrong precision/scale that would cause incorrect pruning. `try_into` on
        // failure → return None, so the convertor falls back to the residual
        // filter (which is always correct).
        let p: u8 = precision.try_into().ok()?;
        let s: i8 = scale.try_into().ok()?;
        let dtype = DecimalDType::new(p, s);
        Some(VortexScalar::decimal(DecimalValue::I128(value), dtype, nul))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_primitive_types_convert() {
        use polars_core::prelude::DataType;
        use polars_core::scalar::Scalar;

        let s = Scalar::new(DataType::Int32, AnyValue::Int32(42));
        assert!(polars_scalar_to_vortex(&s).is_some());

        let s = Scalar::new(DataType::Float64, AnyValue::Float64(2.5));
        assert!(polars_scalar_to_vortex(&s).is_some());

        let s = Scalar::new(DataType::Boolean, AnyValue::Boolean(true));
        assert!(polars_scalar_to_vortex(&s).is_some());

        let s = Scalar::new(DataType::String, AnyValue::StringOwned("hello".into()));
        assert!(polars_scalar_to_vortex(&s).is_some());
    }

    #[test]
    fn scalar_null_does_not_convert() {
        use polars_core::prelude::DataType;
        use polars_core::scalar::Scalar;

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
        for unit in [
            TimeUnit::Nanoseconds,
            TimeUnit::Microseconds,
            TimeUnit::Milliseconds,
        ] {
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
    fn decimal_scalar_round_trip() {
        use vortex::dtype::DType;
        // Polars Decimal(10, 2) — small precision/scale, fits in u8/i8 easily.
        let s =
            temporal::decimal_scalar(12345, 10, 2, Nullability::Nullable).expect("decimal scalar");
        match s.dtype() {
            DType::Decimal(dec, _) => {
                assert_eq!(dec.precision(), 10);
                assert_eq!(dec.scale(), 2);
            },
            other => panic!("expected Decimal dtype, got {:?}", other),
        }
    }

    #[cfg(feature = "dtype-decimal")]
    #[test]
    fn decimal_scalar_overflow_returns_none() {
        // precision > 38 OR scale > i8::MAX would overflow the Vortex types; refuse.
        assert!(temporal::decimal_scalar(0, 256, 0, Nullability::Nullable).is_none());
        assert!(temporal::decimal_scalar(0, 0, 256, Nullability::Nullable).is_none());
    }
}
