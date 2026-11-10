//! Semantics-preserving static predicates for the native reader.
use polars_core::prelude::{AnyValue, DataType, Schema};
use polars_core::scalar::Scalar;
use polars_io::predicates::{
    RuntimeRange, RuntimeRangeHint, ScanIOPredicate, SpecializedColumnPredicate,
};
use polars_utils::pl_str::PlSmallStr;
use vortex::dtype::Nullability;
use vortex::expr::{
    Expression, and, and_collect, eq, get_item, gt_eq, is_null, like, lit, lt_eq, not, or,
    or_collect, root,
};
use vortex::scalar::Scalar as VortexScalar;

pub fn polars_to_vortex_predicate(
    predicate: &ScanIOPredicate,
    logical: &Schema,
    physical: &Schema,
) -> Option<Expression> {
    let staged = predicate.staged.as_ref()?;
    // Physical expressions are opaque here. Unknown residuals can contain errors.
    if !predicate.filters_rows || staged.rest.is_some() {
        return None;
    }
    // Polars applies projection casts before its residual predicate. Do not hide cast errors.
    if logical
        .iter()
        .any(|(name, dtype)| physical.get(name) != Some(dtype))
    {
        return None;
    }
    let mut columns: Vec<_> = staged.column_predicates.iter().collect();
    columns.sort_unstable_by(|a, b| a.0.cmp(b.0));
    let expressions: Option<Vec<_>> = columns
        .into_iter()
        .map(|(name, p)| {
            if !p.dynamic.is_empty() || p.predicate.is_none() {
                return None;
            }
            let dtype = logical.get(name)?;
            if physical.get(name)? != dtype || !safe_dtype(dtype) {
                return None;
            }
            convert_specialized(name, p.specialized.as_ref()?, dtype)
        })
        .collect();
    and_collect(expressions?)
}
/// Translate conjunctive range hints after the planner proves the whole residual safe.
/// The caller must keep the complete Polars predicate and its producer-side recheck.
/// A snapshot is valid only for a trusted, immutable join producer.
pub fn runtime_ranges_to_vortex_predicate(
    hints: &[RuntimeRangeHint],
    logical: &Schema,
    physical: &Schema,
) -> Option<Expression> {
    // Early filtering must not hide a projection cast failure on another column.
    if logical
        .iter()
        .any(|(name, dtype)| physical.get(name) != Some(dtype))
    {
        return None;
    }
    and_collect(
        hints
            .iter()
            .filter_map(|hint| runtime_range_expression(hint, logical, physical)),
    )
}

fn runtime_range_expression(
    hint: &RuntimeRangeHint,
    logical: &Schema,
    physical: &Schema,
) -> Option<Expression> {
    if hint.constant.is_some()
        || hint.order_keeping_expr.is_some()
        || !hint.source.is_immutable_join_filter()
        || hint.source.read_order().is_some()
    {
        return None;
    }
    let dtype = logical.get(&hint.column)?;
    if physical.get(&hint.column) != Some(dtype)
        || !matches!(
            dtype,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
        )
    {
        return None;
    }
    let range = hint.source.runtime_range();
    if !hint.source.can_bypass() {
        return None;
    }
    match range {
        RuntimeRange::Pending | RuntimeRange::Disabled => None,
        RuntimeRange::Empty => Some(lit(false)),
        RuntimeRange::Range {
            lo,
            hi,
            nulls_match,
        } => {
            // Bounds must already be in the physical column's exact dtype.
            let bound = |value: &Scalar| {
                if value.dtype() != dtype || value.value().dtype() != *dtype || value.is_null() {
                    return None;
                }
                polars_scalar_to_vortex_for_dtype(value, dtype).map(lit)
            };
            if let (Some(lo), Some(hi)) = (&lo, &hi) {
                let lower = lo.value().extract::<i128>()?;
                let upper = hi.value().extract::<i128>()?;
                if lower > upper {
                    return None;
                }
            }
            let column = get_item(hint.column.as_str(), root());
            let mut expressions = Vec::with_capacity(2);
            if let Some(lo) = lo {
                expressions.push(gt_eq(column.clone(), bound(&lo)?));
            }
            if let Some(hi) = hi {
                expressions.push(lt_eq(column.clone(), bound(&hi)?));
            }
            match and_collect(expressions) {
                Some(expression) if nulls_match => Some(or(is_null(column), expression)),
                Some(expression) => Some(expression),
                None if nulls_match => None,
                None => Some(not(is_null(column))),
            }
        },
    }
}

fn safe_dtype(dtype: &DataType) -> bool {
    match dtype {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::String => true,
        #[cfg(feature = "dtype-date")]
        DataType::Date => true,
        #[cfg(feature = "dtype-decimal")]
        DataType::Decimal(_, _) => true,
        _ => false,
    }
}
fn convert_specialized(
    name: &PlSmallStr,
    p: &SpecializedColumnPredicate,
    dtype: &DataType,
) -> Option<Expression> {
    let col = get_item(name.as_str(), root());
    let scalar = |s: &Scalar| polars_scalar_to_vortex_for_dtype(s, dtype).map(lit);
    Some(match p {
        SpecializedColumnPredicate::Equal(s) if s.dtype() == dtype && s.is_null() => is_null(col),
        SpecializedColumnPredicate::Equal(s) => eq(col, scalar(s)?),
        SpecializedColumnPredicate::Between(lo, hi) => {
            and(gt_eq(col.clone(), scalar(lo)?), lt_eq(col, scalar(hi)?))
        },
        SpecializedColumnPredicate::EqualOneOf(values) => {
            if !matches!(
                dtype,
                DataType::Boolean
                    | DataType::String
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
            ) || values.len() > 64
            {
                return None;
            }
            let terms: Option<Vec<_>> = values
                .iter()
                .map(|s| {
                    if s.dtype() != dtype {
                        return None;
                    }
                    if s.is_null() {
                        Some(is_null(col.clone()))
                    } else {
                        Some(eq(col.clone(), scalar(s)?))
                    }
                })
                .collect();
            or_collect(terms?)?
        },
        SpecializedColumnPredicate::StartsWith(bytes) if dtype == &DataType::String => like(
            col,
            lit(VortexScalar::utf8(
                format!("{}%", bytes_to_like_literal(bytes)?),
                Nullability::NonNullable,
            )),
        ),
        SpecializedColumnPredicate::EndsWith(bytes) if dtype == &DataType::String => like(
            col,
            lit(VortexScalar::utf8(
                format!("%{}", bytes_to_like_literal(bytes)?),
                Nullability::NonNullable,
            )),
        ),
        _ => return None,
    })
}
pub fn bytes_to_like_literal(bytes: &[u8]) -> Option<&str> {
    let text = std::str::from_utf8(bytes).ok()?;
    if text.contains(['%', '_', '\\']) {
        None
    } else {
        Some(text)
    }
}
pub fn polars_scalar_to_vortex(scalar: &Scalar) -> Option<VortexScalar> {
    let n = Nullability::Nullable;
    Some(match scalar.value() {
        AnyValue::Boolean(v) => VortexScalar::bool(*v, n),
        AnyValue::Int8(v) => VortexScalar::primitive(*v, n),
        AnyValue::Int16(v) => VortexScalar::primitive(*v, n),
        AnyValue::Int32(v) => VortexScalar::primitive(*v, n),
        AnyValue::Int64(v) => VortexScalar::primitive(*v, n),
        AnyValue::UInt8(v) => VortexScalar::primitive(*v, n),
        AnyValue::UInt16(v) => VortexScalar::primitive(*v, n),
        AnyValue::UInt32(v) => VortexScalar::primitive(*v, n),
        AnyValue::UInt64(v) => VortexScalar::primitive(*v, n),
        #[cfg(feature = "dtype-date")]
        AnyValue::Date(v) => VortexScalar::extension::<vortex::array::extension::datetime::Date>(
            vortex::array::extension::datetime::TimeUnit::Days,
            VortexScalar::primitive(*v, n),
        ),
        #[cfg(feature = "dtype-decimal")]
        AnyValue::Decimal(value, precision, scale) => decimal_scalar(*value, *precision, *scale)?,
        AnyValue::String(v) => VortexScalar::utf8(v.to_string(), n),
        AnyValue::StringOwned(v) => VortexScalar::utf8(v.to_string(), n),
        _ => return None,
    })
}

/// Convert a resolved scalar only when its value is exact in the requested dtype.
pub fn polars_scalar_to_vortex_for_dtype(
    scalar: &Scalar,
    dtype: &DataType,
) -> Option<VortexScalar> {
    #[cfg(feature = "dtype-decimal")]
    if let DataType::Decimal(precision, scale) = dtype {
        let AnyValue::Decimal(value, _, old_scale) = scalar.value() else {
            return None;
        };
        let value = if scale >= old_scale {
            value.checked_mul(10i128.checked_pow(u32::try_from(scale - old_scale).ok()?)?)?
        } else {
            let divisor = 10i128.checked_pow(u32::try_from(old_scale - scale).ok()?)?;
            if value % divisor != 0 {
                return None;
            }
            value / divisor
        };
        return decimal_scalar(value, *precision, *scale);
    }
    (scalar.dtype() == dtype)
        .then(|| polars_scalar_to_vortex(scalar))
        .flatten()
}

#[cfg(feature = "dtype-decimal")]
fn decimal_scalar(value: i128, precision: usize, scale: usize) -> Option<VortexScalar> {
    if !(1..=38).contains(&precision)
        || value.checked_abs()? >= 10i128.checked_pow(u32::try_from(precision).ok()?)?
    {
        return None;
    }
    let dtype = vortex::dtype::DecimalDType::try_new(
        u8::try_from(precision).ok()?,
        i8::try_from(scale).ok()?,
    )
    .ok()?;
    VortexScalar::try_decimal(value.into(), dtype, Nullability::NonNullable).ok()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use polars_core::prelude::*;
    use polars_error::{PolarsResult, polars_err};
    use polars_io::predicates::{ColumnPredicate, PhysicalIoExpr, StagedScanIOPredicate};

    use super::*;

    struct EqualZero;
    impl PhysicalIoExpr for EqualZero {
        fn evaluate_io(&self, df: &DataFrame) -> PolarsResult<Series> {
            Ok(df.column("x")?.i64()?.equal(0i64).into_series())
        }
    }
    struct Fails;
    impl PhysicalIoExpr for Fails {
        fn evaluate_io(&self, _: &DataFrame) -> PolarsResult<Series> {
            Err(polars_err!(ComputeError: "residual failed"))
        }
    }
    fn predicate() -> ScanIOPredicate {
        ScanIOPredicate {
            predicate: Arc::new(EqualZero),
            staged: Some(StagedScanIOPredicate {
                column_predicates: Arc::new(
                    [(
                        "x".into(),
                        ColumnPredicate {
                            predicate: Some(Arc::new(EqualZero)),
                            specialized: Some(SpecializedColumnPredicate::Equal(Scalar::from(
                                0i64,
                            ))),
                            dynamic: Vec::new(),
                        },
                    )]
                    .into_iter()
                    .collect(),
                ),
                rest: None,
            }),
            filters_rows: true,
            live_columns: Arc::new(["x".into()].into_iter().collect()),
            skip_batch_predicate: None,
            runtime_ranges: Vec::new(),
            hive_predicate: None,
            hive_predicate_is_full_predicate: false,
        }
    }

    #[test]
    fn null_specializations_retain_matching_null_rows() {
        use vortex::array::IntoArray;
        use vortex::array::arrays::{PrimitiveArray, StructArray};
        let input = StructArray::from_fields(&[(
            "x",
            PrimitiveArray::from_option_iter([None, Some(0i64), Some(1i64)]).into_array(),
        )])
        .unwrap()
        .into_array();
        for (specialized, expected) in [
            (
                SpecializedColumnPredicate::Equal(Scalar::null(DataType::Int64)),
                vec![Some(true), Some(false), Some(false)],
            ),
            (
                SpecializedColumnPredicate::EqualOneOf(
                    vec![Scalar::null(DataType::Int64), Scalar::from(1i64)].into(),
                ),
                vec![Some(true), Some(false), Some(true)],
            ),
        ] {
            let expr = convert_specialized(&"x".into(), &specialized, &DataType::Int64).unwrap();
            let result = input.clone().apply(&expr).unwrap();
            let frame = crate::read::array_bridge::vortex_array_to_dataframe(
                StructArray::from_fields(&[("result", result)])
                    .unwrap()
                    .into_array(),
            )
            .unwrap();
            let actual: Vec<_> = frame
                .column("result")
                .unwrap()
                .bool()
                .unwrap()
                .iter()
                .collect();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn opaque_residual_error_prevents_early_row_removal() {
        let schema = Schema::from_iter([("x".into(), DataType::Int64)]);
        let mut p = predicate();
        assert!(polars_to_vortex_predicate(&p, &schema, &schema).is_some());
        p.predicate = Arc::new(Fails);
        p.staged.as_mut().unwrap().rest = Some(Arc::new(Fails));
        let df = df!("x" => [1i64]).unwrap();
        assert!(p.predicate.evaluate_io(&df).is_err());
        assert!(polars_to_vortex_predicate(&p, &schema, &schema).is_none());
    }

    #[test]
    fn projection_casts_and_unknown_column_terms_prevent_pushdown() {
        let physical = Schema::from_iter([
            ("x".into(), DataType::Int64),
            ("y".into(), DataType::String),
        ]);
        let logical =
            Schema::from_iter([("x".into(), DataType::Int64), ("y".into(), DataType::Int64)]);
        let mut p = predicate();
        assert!(polars_to_vortex_predicate(&p, &logical, &physical).is_none());
        p.staged.as_mut().unwrap().column_predicates = Arc::new(
            [(
                "x".into(),
                ColumnPredicate {
                    predicate: Some(Arc::new(EqualZero)),
                    specialized: None,
                    dynamic: Vec::new(),
                },
            )]
            .into_iter()
            .collect(),
        );
        assert!(polars_to_vortex_predicate(&p, &physical, &physical).is_none());
    }
    #[cfg(feature = "dtype-decimal")]
    #[test]
    fn decimal_literal_rescale_requires_exact_value_and_precision() {
        let scalar = |value, precision, scale| {
            Scalar::new(
                DataType::Decimal(precision, scale),
                AnyValue::Decimal(value, precision, scale),
            )
        };
        let target = DataType::Decimal(6, 2);
        assert_eq!(
            polars_scalar_to_vortex_for_dtype(&scalar(12300, 8, 4), &target),
            polars_scalar_to_vortex(&scalar(123, 6, 2)),
        );
        assert_eq!(
            polars_scalar_to_vortex_for_dtype(&scalar(-123, 6, 2), &DataType::Decimal(8, 4)),
            polars_scalar_to_vortex(&scalar(-12300, 8, 4)),
        );
        assert!(polars_scalar_to_vortex_for_dtype(&scalar(12301, 8, 4), &target).is_none());
        assert!(polars_scalar_to_vortex_for_dtype(&scalar(1_000_000, 7, 2), &target).is_none());
        assert!(
            polars_scalar_to_vortex_for_dtype(
                &scalar(i128::MAX, 38, 0),
                &DataType::Decimal(38, 38)
            )
            .is_none()
        );
        assert!(
            polars_scalar_to_vortex_for_dtype(&Scalar::null(target.clone()), &target).is_none()
        );
    }
    struct RangeSource {
        range: RuntimeRange,
        trusted: bool,
        bypass: bool,
        ordered: bool,
    }
    impl polars_io::predicates::DynamicPredicateSource for RangeSource {
        fn runtime_range(&self) -> RuntimeRange {
            self.range.clone()
        }
        fn filters_rows(&self) -> bool {
            false
        }
        fn can_bypass(&self) -> bool {
            self.bypass
        }
        fn is_immutable_join_filter(&self) -> bool {
            self.trusted
        }
        fn read_order(&self) -> Option<polars_io::predicates::ReadOrder> {
            self.ordered
                .then_some(polars_io::predicates::ReadOrder::LowFirst)
        }
    }
    fn range_hint(range: RuntimeRange) -> RuntimeRangeHint {
        RuntimeRangeHint {
            column: "x".into(),
            order_keeping_expr: None,
            constant: None,
            source: Arc::new(RangeSource {
                range,
                trusted: true,
                bypass: true,
                ordered: false,
            }),
        }
    }
    fn range_mask(expression: Expression) -> Vec<Option<bool>> {
        use vortex::array::IntoArray;
        use vortex::array::arrays::{PrimitiveArray, StructArray};
        let input = StructArray::from_fields(&[(
            "x",
            PrimitiveArray::from_option_iter([
                None,
                Some(-1i64),
                Some(0),
                Some(1),
                Some(2),
                Some(3),
            ])
            .into_array(),
        )])
        .unwrap()
        .into_array();
        let result = input.apply(&expression).unwrap();
        let frame = crate::read::array_bridge::vortex_array_to_dataframe(
            StructArray::from_fields(&[("result", result)])
                .unwrap()
                .into_array(),
        )
        .unwrap();
        frame
            .column("result")
            .unwrap()
            .bool()
            .unwrap()
            .iter()
            .collect()
    }
    #[test]
    fn immutable_runtime_range_execution_preserves_bounds_and_nulls() {
        let schema = Schema::from_iter([("x".into(), DataType::Int64)]);
        for nulls_match in [false, true] {
            let hint = range_hint(RuntimeRange::Range {
                lo: Some(Scalar::from(0i64)),
                hi: Some(Scalar::from(2i64)),
                nulls_match,
            });
            let expression = runtime_ranges_to_vortex_predicate(&[hint], &schema, &schema).unwrap();
            let null_value = if nulls_match { Some(true) } else { None };
            assert_eq!(
                range_mask(expression),
                vec![
                    null_value,
                    Some(false),
                    Some(true),
                    Some(true),
                    Some(true),
                    Some(false)
                ]
            );
        }
        let empty = range_hint(RuntimeRange::Empty);
        assert_eq!(
            range_mask(runtime_ranges_to_vortex_predicate(&[empty], &schema, &schema).unwrap()),
            vec![Some(false); 6]
        );
        for state in [RuntimeRange::Pending, RuntimeRange::Disabled] {
            assert!(
                runtime_ranges_to_vortex_predicate(&[range_hint(state)], &schema, &schema)
                    .is_none()
            );
        }
        let unbounded = range_hint(RuntimeRange::Range {
            lo: None,
            hi: None,
            nulls_match: false,
        });
        assert_eq!(
            range_mask(runtime_ranges_to_vortex_predicate(&[unbounded], &schema, &schema).unwrap()),
            vec![
                Some(false),
                Some(true),
                Some(true),
                Some(true),
                Some(true),
                Some(true)
            ]
        );
        assert!(
            runtime_ranges_to_vortex_predicate(
                &[range_hint(RuntimeRange::Range {
                    lo: None,
                    hi: None,
                    nulls_match: true
                })],
                &schema,
                &schema
            )
            .is_none()
        );
    }
    #[test]
    fn runtime_range_rejects_unproven_producers_and_bounds() {
        let schema = Schema::from_iter([("x".into(), DataType::Int64)]);
        for (trusted, bypass, ordered) in [
            (false, true, false),
            (true, false, false),
            (true, true, true),
        ] {
            let mut hint = range_hint(RuntimeRange::Empty);
            hint.source = Arc::new(RangeSource {
                range: RuntimeRange::Empty,
                trusted,
                bypass,
                ordered,
            });
            assert!(runtime_ranges_to_vortex_predicate(&[hint], &schema, &schema).is_none());
        }
        for (lo, hi) in [
            (Scalar::from(0u64), Scalar::from(2u64)),
            (Scalar::null(DataType::Int64), Scalar::from(2i64)),
            (Scalar::from(3i64), Scalar::from(2i64)),
        ] {
            let hint = range_hint(RuntimeRange::Range {
                lo: Some(lo),
                hi: Some(hi),
                nulls_match: false,
            });
            assert!(runtime_ranges_to_vortex_predicate(&[hint], &schema, &schema).is_none());
        }
        let malformed = range_hint(RuntimeRange::Range {
            lo: Some(Scalar::new(DataType::Int64, AnyValue::UInt64(0))),
            hi: None,
            nulls_match: false,
        });
        assert!(runtime_ranges_to_vortex_predicate(&[malformed], &schema, &schema).is_none());
        let other = Schema::from_iter([("x".into(), DataType::UInt64)]);
        assert!(
            runtime_ranges_to_vortex_predicate(&[range_hint(RuntimeRange::Empty)], &schema, &other)
                .is_none()
        );
        let mut constant = range_hint(RuntimeRange::Empty);
        constant.constant = Some(Scalar::from(0i64));
        assert!(runtime_ranges_to_vortex_predicate(&[constant], &schema, &schema).is_none());
        struct Identity;
        impl polars_io::predicates::ColumnIoExpr for Identity {
            fn evaluate_io(&self, df: &DataFrame) -> PolarsResult<Column> {
                df.column("x").cloned()
            }
        }
        let mut computed = range_hint(RuntimeRange::Empty);
        computed.order_keeping_expr = Some(Arc::new(Identity));
        assert!(runtime_ranges_to_vortex_predicate(&[computed], &schema, &schema).is_none());
    }
    #[test]
    fn unsigned_runtime_range_keeps_u64_extremes() {
        use vortex::array::IntoArray;
        use vortex::array::arrays::{PrimitiveArray, StructArray};
        let schema = Schema::from_iter([("x".into(), DataType::UInt64)]);
        let hint = range_hint(RuntimeRange::Range {
            lo: Some(Scalar::from(u64::MAX - 1)),
            hi: Some(Scalar::from(u64::MAX)),
            nulls_match: false,
        });
        let expression = runtime_ranges_to_vortex_predicate(&[hint], &schema, &schema).unwrap();
        let input = StructArray::from_fields(&[(
            "x",
            PrimitiveArray::from_option_iter([
                Some(0u64),
                Some(u64::MAX - 1),
                Some(u64::MAX),
                None,
            ])
            .into_array(),
        )])
        .unwrap()
        .into_array();
        let result = input.apply(&expression).unwrap();
        let frame = crate::read::array_bridge::vortex_array_to_dataframe(
            StructArray::from_fields(&[("result", result)])
                .unwrap()
                .into_array(),
        )
        .unwrap();
        assert_eq!(
            frame
                .column("result")
                .unwrap()
                .bool()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(false), Some(true), Some(true), None]
        );
    }
}
