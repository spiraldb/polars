//! Semantics-preserving static predicates for the native reader.
use polars_core::prelude::{AnyValue, DataType, Schema};
use polars_core::scalar::Scalar;
use polars_io::predicates::{
    RuntimeRange, RuntimeRangeHint, ScanIOPredicate, SpecializedColumnPredicate,
};
use polars_utils::pl_str::PlSmallStr;
use vortex::array::expr::stats::{Precision, Stat};
use vortex::dtype::{DType, FieldPath, Nullability, PType};
use vortex::expr::{
    Expression, and, and_collect, eq, get_item, gt_eq, is_null, like, lit, lt_eq, not, or,
    or_collect, root,
};
use vortex::file::Footer;
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
    runtime_ranges_with_footer(hints, logical, physical, None).row_filter
}

/// Apply per-file cost admission using only this file's already loaded footer.
/// Bounds that cover at least 99% of an exact integer domain can be omitted.
/// The complete Polars predicate and join recheck must remain active.
#[cfg(test)]
fn runtime_ranges_to_vortex_predicate_with_footer(
    hints: &[RuntimeRangeHint],
    logical: &Schema,
    physical: &Schema,
    footer: &Footer,
) -> Option<Expression> {
    runtime_ranges_to_vortex_filters(hints, logical, physical, footer).row_filter
}

/// Separate row evaluation from conservative split pruning.
#[derive(Default)]
pub struct RuntimeVortexFilters {
    pub row_filter: Option<Expression>,
    pub prune_filter: Option<Expression>,
}

pub fn runtime_ranges_to_vortex_filters(
    hints: &[RuntimeRangeHint],
    logical: &Schema,
    physical: &Schema,
    footer: &Footer,
) -> RuntimeVortexFilters {
    runtime_ranges_with_footer(hints, logical, physical, Some(footer))
}

fn runtime_ranges_with_footer(
    hints: &[RuntimeRangeHint],
    logical: &Schema,
    physical: &Schema,
    footer: Option<&Footer>,
) -> RuntimeVortexFilters {
    if hints.is_empty()
        || logical
            .iter()
            .any(|(name, dtype)| physical.get(name) != Some(dtype))
    {
        return RuntimeVortexFilters::default();
    }
    let mut pruning = Vec::new();
    let row_filter = and_collect(hints.iter().filter_map(|hint| {
        runtime_range_expression(hint, logical, physical, footer, &mut pruning)
    }));
    RuntimeVortexFilters {
        row_filter,
        prune_filter: and_collect(pruning),
    }
}

fn runtime_range_expression(
    hint: &RuntimeRangeHint,
    logical: &Schema,
    physical: &Schema,
    footer: Option<&Footer>,
    pruning: &mut Vec<Expression>,
) -> Option<Expression> {
    if hint.constant.is_some()
        || hint.order_keeping_expr.is_some()
        || !hint.source.is_immutable_join_filter()
        || hint.source.read_order().is_some()
    {
        return None;
    }
    let dtype = logical.get(&hint.column)?;
    if physical.get(&hint.column) != Some(dtype) {
        return None;
    }
    let ptype = integer_ptype(dtype)?;
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
            // Validate every bound before either conversion or cost admission.
            let integer = |value: &Scalar| {
                if value.dtype() != dtype || value.value().dtype() != *dtype || value.is_null() {
                    return None;
                }
                value.value().extract::<i128>()
            };
            let lower = match &lo {
                Some(value) => Some(integer(value)?),
                None => None,
            };
            let upper = match &hi {
                Some(value) => Some(integer(value)?),
                None => None,
            };
            if lower.zip(upper).is_some_and(|(lo, hi)| lo > hi) {
                return None;
            }
            let column = get_item(hint.column.as_str(), root());
            let bound = |value: &Scalar| polars_scalar_to_vortex_for_dtype(value, dtype).map(lit);
            let mut expressions = Vec::with_capacity(2);
            if let Some(lo) = &lo {
                expressions.push(gt_eq(column.clone(), bound(lo)?));
            }
            if let Some(hi) = &hi {
                expressions.push(lt_eq(column.clone(), bound(hi)?));
            }
            let expression = match and_collect(expressions) {
                Some(expression) if nulls_match => Some(or(is_null(column.clone()), expression)),
                Some(expression) => Some(expression),
                None if nulls_match => None,
                None => Some(not(is_null(column.clone()))),
            };
            let domain = footer.and_then(|footer| file_integer_domain(footer, &hint.column, ptype));
            if let Some((domain, reason)) = domain.and_then(|domain| {
                omit_integer_bounds(&domain, lower, upper).map(|reason| (domain, reason))
            }) {
                if reason == RangeOmission::NearFull {
                    if let Some(expression) = expression {
                        pruning.push(expression);
                    }
                }
                if polars_core::config::verbose() {
                    let route = match reason {
                        RangeOmission::Contained => "omit contained runtime bounds",
                        RangeOmission::NearFull => {
                            "runtime range for pruning: >=99% integer-domain cost heuristic"
                        },
                    };
                    eprintln!("[vortex]: {route} on '{}'", hint.column);
                }
                return if nulls_match || domain.nulls_absent {
                    None
                } else {
                    Some(not(is_null(column)))
                };
            }
            if footer.is_some() && polars_core::config::verbose() {
                eprintln!(
                    "[vortex]: retain runtime bounds for row evaluation on '{}'",
                    hint.column
                );
            }
            expression
        },
    }
}

#[derive(Clone, Copy, Debug)]
struct FileIntegerDomain {
    min: i128,
    max: i128,
    exact: bool,
    nulls_absent: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RangeOmission {
    Contained,
    NearFull,
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

fn file_integer_domain(
    footer: &Footer,
    column: &PlSmallStr,
    ptype: PType,
) -> Option<FileIntegerDomain> {
    let DType::Struct(fields, Nullability::NonNullable) = footer.dtype() else {
        return None;
    };
    let field = fields.field(column.as_str())?;
    if !matches!(&field, DType::Primitive(actual, _) if *actual == ptype) {
        return None;
    }
    let (aggregates, stat_dtype) = footer
        .statistics()?
        .get_by_path(&FieldPath::from_name(column.as_str()))?;
    if stat_dtype != &field {
        return None;
    }
    let bound = |stat: Stat| {
        let (scalar, exact) = match aggregates.get(&stat.aggregate_fn()?) {
            Precision::Exact(value) => (value, true),
            Precision::Inexact(value) => (value, false),
            Precision::Absent => return None,
        };
        if !matches!(scalar.dtype(), DType::Primitive(actual, _) if *actual == ptype) {
            return None;
        }
        let value = match ptype {
            PType::I8 | PType::I16 | PType::I32 | PType::I64 => {
                i128::from(i64::try_from(&scalar).ok()?)
            },
            PType::U8 | PType::U16 | PType::U32 | PType::U64 => {
                i128::from(u64::try_from(&scalar).ok()?)
            },
            _ => return None,
        };
        Some((value, exact))
    };
    let (min, min_exact) = bound(Stat::Min)?;
    let (max, max_exact) = bound(Stat::Max)?;
    if min > max || footer.row_count() == 0 {
        return None;
    }
    let nonnullable = matches!(field, DType::Primitive(_, Nullability::NonNullable));
    let nulls = match aggregates.get(&Stat::NullCount.aggregate_fn()?) {
        Precision::Exact(value) => {
            if !matches!(value.dtype(), DType::Primitive(PType::U64, _)) {
                return None;
            }
            let count = u64::try_from(&value).ok()?;
            if count > footer.row_count() || (nonnullable && count != 0) {
                return None;
            }
            Some(count)
        },
        Precision::Inexact(value) => {
            if !matches!(value.dtype(), DType::Primitive(PType::U64, _)) {
                return None;
            }
            u64::try_from(&value).ok()?;
            None
        },
        Precision::Absent => None,
    };
    // Non-null extrema contradict a claim that every row is null.
    if nulls == Some(footer.row_count()) {
        return None;
    }
    Some(FileIntegerDomain {
        min,
        max,
        exact: min_exact && max_exact,
        nulls_absent: nonnullable || nulls == Some(0),
    })
}

fn omit_integer_bounds(
    domain: &FileIntegerDomain,
    lower: Option<i128>,
    upper: Option<i128>,
) -> Option<RangeOmission> {
    let lower = lower.unwrap_or(domain.min);
    let upper = upper.unwrap_or(domain.max);
    if lower <= domain.min && upper >= domain.max {
        return Some(RangeOmission::Contained);
    }
    if !domain.exact {
        return None;
    }
    let lo = lower.max(domain.min);
    let hi = upper.min(domain.max);
    if lo > hi {
        return None;
    }
    // Each supported dtype has at most 2^64 values. Products fit within u128.
    let width = u128::try_from(domain.max.checked_sub(domain.min)?)
        .ok()?
        .checked_add(1)?;
    let overlap = u128::try_from(hi.checked_sub(lo)?).ok()?.checked_add(1)?;
    (100 * overlap >= 99 * width).then_some(RangeOmission::NearFull)
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
    fn metadata_footer(
        field: DType,
        stats_dtype: DType,
        min: Precision<VortexScalar>,
        max: Precision<VortexScalar>,
        nulls: Precision<VortexScalar>,
    ) -> Footer {
        use vortex::dtype::StructFields;
        use vortex::file::FileStatistics;
        use vortex::layout::layouts::file_stats::{AggregateStat, AggregateStats};
        use vortex::layout::layouts::flat::FlatLayout;
        use vortex::layout::segments::SegmentId;
        use vortex::session::registry::ReadContext;
        let ctx = ReadContext::new([]);
        let dtype = DType::Struct(
            [("x", field)].into_iter().collect::<StructFields>(),
            Nullability::NonNullable,
        );
        let stats = AggregateStats::new(
            [(Stat::Min, min), (Stat::Max, max), (Stat::NullCount, nulls)]
                .into_iter()
                .map(|(stat, value)| AggregateStat::from_value(stat.aggregate_fn().unwrap(), value))
                .collect(),
        );
        Footer::new(
            FlatLayout::new(100, dtype, SegmentId::from(0), ctx.clone()).into_layout(),
            Arc::from([]),
            Some(FileStatistics::new(
                Arc::from([stats]),
                Arc::from([stats_dtype]),
                Arc::from([FieldPath::from_name("x")]),
            )),
            ctx,
        )
    }
    fn i64_footer(
        min: Precision<VortexScalar>,
        max: Precision<VortexScalar>,
        nulls: Precision<VortexScalar>,
        nullable: bool,
    ) -> Footer {
        let dtype = DType::Primitive(
            PType::I64,
            if nullable {
                Nullability::Nullable
            } else {
                Nullability::NonNullable
            },
        );
        metadata_footer(dtype.clone(), dtype, min, max, nulls)
    }
    fn exact_i64(value: i64) -> Precision<VortexScalar> {
        Precision::Exact(VortexScalar::primitive(value, Nullability::Nullable))
    }
    fn exact_nulls(value: u64) -> Precision<VortexScalar> {
        Precision::Exact(VortexScalar::primitive(value, Nullability::NonNullable))
    }
    #[test]
    fn integer_domain_overlap_uses_inclusive_exact_endpoints() {
        let domain = FileIntegerDomain {
            min: 0,
            max: 99,
            exact: true,
            nulls_absent: true,
        };
        for (lo, hi, expected) in [
            (Some(0), Some(99), Some(RangeOmission::Contained)),
            (Some(1), None, Some(RangeOmission::NearFull)),
            (None, Some(98), Some(RangeOmission::NearFull)),
            (Some(2), None, None),
            (None, Some(97), None),
            (Some(50), Some(99), None),
            (Some(100), Some(200), None),
        ] {
            assert_eq!(omit_integer_bounds(&domain, lo, hi), expected);
        }
        let inexact = FileIntegerDomain {
            exact: false,
            ..domain
        };
        assert_eq!(
            omit_integer_bounds(&inexact, Some(0), Some(99)),
            Some(RangeOmission::Contained)
        );
        assert_eq!(omit_integer_bounds(&inexact, Some(1), None), None);
        for (min, max) in [
            (i128::from(i64::MIN), i128::from(i64::MAX)),
            (0, i128::from(u64::MAX)),
        ] {
            let full = FileIntegerDomain { min, max, ..domain };
            assert_eq!(
                omit_integer_bounds(&full, Some(min), Some(max)),
                Some(RangeOmission::Contained)
            );
            assert_eq!(
                omit_integer_bounds(&full, Some(min + 1), None),
                Some(RangeOmission::NearFull)
            );
            assert_eq!(
                omit_integer_bounds(&full, Some(min + (max - min) / 2 + 1), None),
                None
            );
        }
    }
    #[test]
    fn footer_range_omission_preserves_null_rejection_and_other_hints() {
        let schema = Schema::from_iter([("x".into(), DataType::Int64)]);
        let broad = |nulls_match| {
            range_hint(RuntimeRange::Range {
                lo: Some(Scalar::from(1i64)),
                hi: None,
                nulls_match,
            })
        };
        for (nullable, nulls, omitted) in [
            (true, exact_nulls(0), true),
            (false, Precision::Absent, true),
            (true, Precision::Absent, false),
            (true, Precision::Inexact(VortexScalar::from(0u64)), false),
            (true, exact_nulls(1), false),
        ] {
            let footer = i64_footer(exact_i64(0), exact_i64(99), nulls, nullable);
            let result = runtime_ranges_to_vortex_predicate_with_footer(
                &[broad(false)],
                &schema,
                &schema,
                &footer,
            );
            assert_eq!(result.is_none(), omitted);
            if let Some(expr) = result {
                assert_eq!(
                    range_mask(expr),
                    vec![
                        Some(false),
                        Some(true),
                        Some(true),
                        Some(true),
                        Some(true),
                        Some(true)
                    ]
                );
            }
            assert!(
                runtime_ranges_to_vortex_predicate_with_footer(
                    &[broad(true)],
                    &schema,
                    &schema,
                    &footer
                )
                .is_none()
            );
        }
        let footer = i64_footer(exact_i64(0), exact_i64(99), exact_nulls(0), true);
        for range in [
            RuntimeRange::Empty,
            RuntimeRange::Range {
                lo: Some(Scalar::from(0i64)),
                hi: Some(Scalar::from(49i64)),
                nulls_match: false,
            },
            RuntimeRange::Range {
                lo: Some(Scalar::from(100i64)),
                hi: None,
                nulls_match: false,
            },
        ] {
            let old =
                runtime_ranges_to_vortex_predicate(&[range_hint(range.clone())], &schema, &schema)
                    .unwrap();
            let new = runtime_ranges_to_vortex_predicate_with_footer(
                &[broad(false), range_hint(range)],
                &schema,
                &schema,
                &footer,
            )
            .unwrap();
            assert_eq!(range_mask(new), range_mask(old));
        }
    }
    #[test]
    fn footer_metadata_precision_and_malformed_values_fall_back() {
        let schema = Schema::from_iter([("x".into(), DataType::Int64)]);
        let hint = || {
            range_hint(RuntimeRange::Range {
                lo: Some(Scalar::from(1i64)),
                hi: None,
                nulls_match: false,
            })
        };
        let nullable = DType::Primitive(PType::I64, Nullability::Nullable);
        let wrong = DType::Primitive(PType::U64, Nullability::Nullable);
        let cases = [
            i64_footer(Precision::Absent, exact_i64(99), exact_nulls(0), true),
            i64_footer(
                exact_i64(0),
                Precision::Inexact(VortexScalar::from(99i64)),
                exact_nulls(0),
                true,
            ),
            i64_footer(exact_i64(99), exact_i64(0), exact_nulls(0), true),
            i64_footer(
                Precision::Exact(VortexScalar::from(0u64)),
                exact_i64(99),
                exact_nulls(0),
                true,
            ),
            i64_footer(exact_i64(0), exact_i64(99), exact_nulls(101), true),
            i64_footer(exact_i64(0), exact_i64(99), exact_nulls(100), true),
            i64_footer(exact_i64(0), exact_i64(99), exact_nulls(1), false),
            metadata_footer(nullable, wrong, exact_i64(0), exact_i64(99), exact_nulls(0)),
        ];
        let expected =
            range_mask(runtime_ranges_to_vortex_predicate(&[hint()], &schema, &schema).unwrap());
        for footer in cases {
            let actual = runtime_ranges_to_vortex_predicate_with_footer(
                &[hint()],
                &schema,
                &schema,
                &footer,
            )
            .unwrap();
            assert_eq!(range_mask(actual), expected);
        }
        let footer = i64_footer(
            exact_i64(0),
            Precision::Inexact(VortexScalar::from(99i64)),
            exact_nulls(0),
            true,
        );
        let contained = range_hint(RuntimeRange::Range {
            lo: Some(Scalar::from(0i64)),
            hi: Some(Scalar::from(99i64)),
            nulls_match: false,
        });
        assert!(
            runtime_ranges_to_vortex_predicate_with_footer(&[contained], &schema, &schema, &footer)
                .is_none()
        );
    }
    #[test]
    fn footer_cost_admission_matches_all_eight_integer_types() {
        macro_rules! check {
            ($native:ty, $polars:ident, $ptype:ident) => {{
                let dtype = DType::Primitive(PType::$ptype, Nullability::Nullable);
                let footer = metadata_footer(
                    dtype.clone(),
                    dtype,
                    Precision::Exact(VortexScalar::primitive(0 as $native, Nullability::Nullable)),
                    Precision::Exact(VortexScalar::primitive(
                        99 as $native,
                        Nullability::Nullable,
                    )),
                    exact_nulls(0),
                );
                let schema = Schema::from_iter([("x".into(), DataType::$polars)]);
                let hint = range_hint(RuntimeRange::Range {
                    lo: Some(Scalar::new(
                        DataType::$polars,
                        AnyValue::$polars(1 as $native),
                    )),
                    hi: None,
                    nulls_match: false,
                });
                assert!(
                    runtime_ranges_to_vortex_predicate(&[hint.clone()], &schema, &schema).is_some()
                );
                assert!(
                    runtime_ranges_to_vortex_predicate_with_footer(
                        &[hint],
                        &schema,
                        &schema,
                        &footer
                    )
                    .is_none()
                );
            }};
        }
        check!(i8, Int8, I8);
        check!(i16, Int16, I16);
        check!(i32, Int32, I32);
        check!(i64, Int64, I64);
        check!(u8, UInt8, U8);
        check!(u16, UInt16, U16);
        check!(u32, UInt32, U32);
        check!(u64, UInt64, U64);
    }
    #[test]
    fn near_full_ranges_keep_split_pruning_and_separate_null_rejection() {
        let schema = Schema::from_iter([("x".into(), DataType::Int64)]);
        let broad = |nulls_match| {
            range_hint(RuntimeRange::Range {
                lo: Some(Scalar::from(1i64)),
                hi: None,
                nulls_match,
            })
        };
        for nulls_match in [false, true] {
            let footer = i64_footer(exact_i64(0), exact_i64(99), exact_nulls(1), true);
            let filters =
                runtime_ranges_to_vortex_filters(&[broad(nulls_match)], &schema, &schema, &footer);
            let original =
                runtime_ranges_to_vortex_predicate(&[broad(nulls_match)], &schema, &schema)
                    .unwrap();
            assert_eq!(
                range_mask(filters.prune_filter.unwrap()),
                range_mask(original)
            );
            if nulls_match {
                assert!(filters.row_filter.is_none());
            } else {
                assert_eq!(
                    range_mask(filters.row_filter.unwrap()),
                    vec![
                        Some(false),
                        Some(true),
                        Some(true),
                        Some(true),
                        Some(true),
                        Some(true)
                    ]
                );
            }
        }
        let footer = i64_footer(exact_i64(0), exact_i64(99), exact_nulls(0), true);
        let filters = runtime_ranges_to_vortex_filters(&[broad(false)], &schema, &schema, &footer);
        assert!(filters.row_filter.is_none());
        assert!(filters.prune_filter.is_some());
        let contained = range_hint(RuntimeRange::Range {
            lo: Some(Scalar::from(0i64)),
            hi: Some(Scalar::from(99i64)),
            nulls_match: false,
        });
        let filters = runtime_ranges_to_vortex_filters(&[contained], &schema, &schema, &footer);
        assert!(filters.row_filter.is_none());
        assert!(filters.prune_filter.is_none());
        let selective = range_hint(RuntimeRange::Range {
            lo: Some(Scalar::from(50i64)),
            hi: None,
            nulls_match: false,
        });
        let filters = runtime_ranges_to_vortex_filters(
            &[broad(false), selective.clone()],
            &schema,
            &schema,
            &footer,
        );
        assert!(filters.prune_filter.is_some());
        assert_eq!(
            range_mask(filters.row_filter.unwrap()),
            range_mask(runtime_ranges_to_vortex_predicate(&[selective], &schema, &schema).unwrap())
        );
    }
    #[test]
    fn prune_only_route_requires_all_existing_hint_proofs() {
        let schema = Schema::from_iter([("x".into(), DataType::Int64)]);
        let footer = i64_footer(exact_i64(0), exact_i64(99), exact_nulls(0), true);
        for range in [
            RuntimeRange::Empty,
            RuntimeRange::Range {
                lo: Some(Scalar::from(100i64)),
                hi: None,
                nulls_match: false,
            },
        ] {
            let filters =
                runtime_ranges_to_vortex_filters(&[range_hint(range)], &schema, &schema, &footer);
            assert!(filters.row_filter.is_some());
            assert!(filters.prune_filter.is_none());
        }
        for range in [RuntimeRange::Pending, RuntimeRange::Disabled] {
            let filters =
                runtime_ranges_to_vortex_filters(&[range_hint(range)], &schema, &schema, &footer);
            assert!(filters.row_filter.is_none());
            assert!(filters.prune_filter.is_none());
        }
        let mut hint = range_hint(RuntimeRange::Range {
            lo: Some(Scalar::from(1i64)),
            hi: None,
            nulls_match: false,
        });
        hint.source = Arc::new(RangeSource {
            range: hint.source.runtime_range(),
            trusted: false,
            bypass: true,
            ordered: false,
        });
        let filters = runtime_ranges_to_vortex_filters(&[hint], &schema, &schema, &footer);
        assert!(filters.row_filter.is_none());
        assert!(filters.prune_filter.is_none());
        let missing = i64_footer(Precision::Absent, exact_i64(99), exact_nulls(0), true);
        let hint = range_hint(RuntimeRange::Range {
            lo: Some(Scalar::from(1i64)),
            hi: None,
            nulls_match: false,
        });
        let filters = runtime_ranges_to_vortex_filters(&[hint], &schema, &schema, &missing);
        assert!(filters.row_filter.is_some());
        assert!(filters.prune_filter.is_none());
    }
}
