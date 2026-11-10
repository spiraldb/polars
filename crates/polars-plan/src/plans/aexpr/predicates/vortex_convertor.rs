//! Conservative conversion of resolved row predicates into Vortex expressions.

use polars_core::prelude::{AnyValue, DataType};
use polars_core::schema::Schema;
use polars_utils::arena::{Arena, Node};
use polars_vortex::read::predicate::{polars_scalar_to_vortex, polars_scalar_to_vortex_for_dtype};
use polars_vortex::vortex::array::scalar::Scalar as VortexScalar;
use polars_vortex::vortex::dtype::Nullability;
use polars_vortex::vortex::expr::{
    Expression, and, and_collect, eq, fill_null, get_item, gt, gt_eq, is_null, lit, lt, lt_eq, not,
    not_eq, or, or_collect, root,
};

use crate::plans::{
    AExpr, IRBooleanFunction, IRFunctionExpr, LiteralValue, MintermIter, Operator,
    aexpr_to_leaf_names_iter,
};

/// Return a necessary condition for the original predicate.
/// The caller must retain the complete Polars predicate as a residual filter.
pub fn aexpr_to_vortex_expression(
    node: Node,
    arena: &Arena<AExpr>,
    schema: &Schema,
) -> Option<Expression> {
    and_collect(file_minterms(node, arena, schema))
}

/// Select complete Boolean conjuncts that reference physical columns.
/// Unsupported children of OR and NOT remain together in the residual filter.
pub fn file_minterms(node: Node, arena: &Arena<AExpr>, schema: &Schema) -> Vec<Expression> {
    // Early filtering can hide a data-dependent error in an unsupported residual.
    if !early_filter_safe(node, arena, schema, 128) {
        return Vec::new();
    }
    MintermIter::new(node, arena)
        .filter(|term| {
            aexpr_to_leaf_names_iter(*term, arena).all(|name| schema.contains(name.as_str()))
        })
        .filter_map(|term| {
            let (expr, dtype) = convert(term, arena, schema, 128)?;
            (dtype == DataType::Boolean).then_some(expr)
        })
        .collect()
}

// A residual must remain rowwise and total after early row removal. Shape-changing
// expressions and opaque UDFs need their original input, even when they do not fail.
fn early_filter_safe(node: Node, arena: &Arena<AExpr>, schema: &Schema, depth: usize) -> bool {
    if depth == 0 || arena.get(node).is_fallible_top_level(arena) {
        return false;
    }
    let child = |node| early_filter_safe(node, arena, schema, depth - 1);
    match arena.get(node) {
        AExpr::Column(_) | AExpr::Literal(LiteralValue::Scalar(_)) => true,
        AExpr::BinaryExpr { left, op, right } => {
            if !child(*left) || !child(*right) {
                return false;
            }
            if convert(node, arena, schema, depth).is_some() {
                return true;
            }
            // Unsupported residuals still need an operator/type proof. For example,
            // List ordering fails in Polars even though its AST nodes look rowwise.
            let context = crate::plans::aexpr::ToFieldContext::new(arena, schema);
            let Ok(left_dtype) = arena.get(*left).to_dtype(&context) else {
                return false;
            };
            let Ok(right_dtype) = arena.get(*right).to_dtype(&context) else {
                return false;
            };
            if left_dtype != right_dtype {
                return false;
            }
            match op {
                Operator::And | Operator::Or | Operator::LogicalAnd | Operator::LogicalOr => {
                    left_dtype == DataType::Boolean
                },
                Operator::Eq
                | Operator::NotEq
                | Operator::EqValidity
                | Operator::NotEqValidity
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq => matches!(
                    left_dtype,
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
                        | DataType::Float32
                        | DataType::Float64
                ),
                _ => false,
            }
        },
        #[cfg(feature = "is_in")]
        AExpr::Function {
            input,
            function: IRFunctionExpr::Boolean(IRBooleanFunction::IsIn { needle_cast, .. }),
            ..
        } if input.len() == 2 && needle_cast.is_none() => {
            // Conversion proves the fixed collection, element dtype and null rules.
            child(input[0].node()) && convert(node, arena, schema, depth).is_some()
        },
        #[cfg(feature = "is_between")]
        AExpr::Function {
            input,
            function: IRFunctionExpr::Boolean(IRBooleanFunction::IsBetween { .. }),
            ..
        } if input.len() == 3 => {
            input.iter().all(|expr| child(expr.node()))
                && convert(node, arena, schema, depth).is_some()
        },
        AExpr::Function {
            input,
            function: IRFunctionExpr::Boolean(function),
            ..
        } if input.len() == 1 => {
            child(input[0].node())
                && match function {
                    IRBooleanFunction::IsNull | IRBooleanFunction::IsNotNull => true,
                    IRBooleanFunction::Not => {
                        input[0].dtype(schema, arena).ok() == Some(&DataType::Boolean)
                    },
                    _ => false,
                }
        },
        #[cfg(feature = "strings")]
        AExpr::Function {
            input,
            function: IRFunctionExpr::StringExpr(function),
            ..
        } if input.len() == 1 => {
            matches!(
                function,
                crate::plans::IRStringFunction::LenBytes | crate::plans::IRStringFunction::LenChars
            ) && input[0].dtype(schema, arena).ok() == Some(&DataType::String)
                && child(input[0].node())
        },
        #[cfg(feature = "dtype-struct")]
        AExpr::Function {
            input,
            function: IRFunctionExpr::StructExpr(crate::plans::IRStructFunction::FieldByName(_)),
            ..
        } if input.len() == 1 => {
            child(input[0].node()) && convert(node, arena, schema, depth).is_some()
        },
        _ => false,
    }
}

fn comparable(dtype: &DataType) -> bool {
    match dtype {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Boolean
        | DataType::String => true,
        #[cfg(feature = "dtype-date")]
        DataType::Date => true,
        #[cfg(feature = "dtype-decimal")]
        DataType::Decimal(_, _) => true,
        _ => false,
    }
}

fn literal_in_dtype(node: Node, arena: &Arena<AExpr>, dtype: &DataType) -> Option<Expression> {
    let AExpr::Literal(LiteralValue::Scalar(scalar)) = arena.get(node) else {
        return None;
    };
    polars_scalar_to_vortex_for_dtype(scalar, dtype).map(lit)
}

/// Convert a complete subtree. Partial conversion occurs only in `file_minterms`.
fn convert(
    node: Node,
    arena: &Arena<AExpr>,
    schema: &Schema,
    depth: usize,
) -> Option<(Expression, DataType)> {
    if depth == 0 {
        return None;
    }
    let child = |node| convert(node, arena, schema, depth - 1);
    Some(match arena.get(node) {
        AExpr::Column(name) => (get_item(name.as_str(), root()), schema.get(name)?.clone()),
        AExpr::Literal(LiteralValue::Scalar(scalar)) => {
            let dtype = scalar.dtype().clone();
            if !comparable(&dtype) {
                return None;
            }
            let value = if scalar.is_null() {
                // Ordinary comparisons propagate null, while null checks remain exact.
                let dtype = match &dtype {
                    DataType::Boolean => {
                        polars_vortex::vortex::dtype::DType::Bool(Nullability::Nullable)
                    },
                    DataType::String => {
                        polars_vortex::vortex::dtype::DType::Utf8(Nullability::Nullable)
                    },
                    _ => return None,
                };
                VortexScalar::null(dtype)
            } else {
                scalar_value(scalar.value())?
            };
            (lit(value), dtype)
        },
        AExpr::BinaryExpr { left, op, right } => {
            let (mut left_expr, mut left_dtype) = child(*left)?;
            let (mut right_expr, mut right_dtype) = child(*right)?;
            if left_dtype != right_dtype {
                #[cfg(feature = "dtype-decimal")]
                if matches!(left_dtype, DataType::Decimal(_, _)) {
                    if let Some(value) = literal_in_dtype(*right, arena, &left_dtype) {
                        right_expr = value;
                        right_dtype = left_dtype.clone();
                    }
                }
                #[cfg(feature = "dtype-decimal")]
                if left_dtype != right_dtype && matches!(right_dtype, DataType::Decimal(_, _)) {
                    if let Some(value) = literal_in_dtype(*left, arena, &right_dtype) {
                        left_expr = value;
                        left_dtype = right_dtype.clone();
                    }
                }
                if left_dtype != right_dtype {
                    return None;
                }
            }
            let (left, right) = (left_expr, right_expr);
            let expr = match op {
                Operator::And | Operator::LogicalAnd if left_dtype == DataType::Boolean => {
                    and(left, right)
                },
                Operator::Or | Operator::LogicalOr if left_dtype == DataType::Boolean => {
                    or(left, right)
                },
                Operator::Eq if comparable(&left_dtype) => eq(left, right),
                Operator::NotEq if comparable(&left_dtype) => not_eq(left, right),
                Operator::Lt if comparable(&left_dtype) => lt(left, right),
                Operator::LtEq if comparable(&left_dtype) => lt_eq(left, right),
                Operator::Gt if comparable(&left_dtype) => gt(left, right),
                Operator::GtEq if comparable(&left_dtype) => gt_eq(left, right),
                _ => return None,
            };
            (expr, DataType::Boolean)
        },
        #[cfg(feature = "is_between")]
        AExpr::Function {
            input,
            function: IRFunctionExpr::Boolean(IRBooleanFunction::IsBetween { closed }),
            ..
        } if input.len() == 3 => {
            use polars_defs::expr::ClosedInterval;
            let (value, dtype) = child(input[0].node())?;
            if !comparable(&dtype) {
                return None;
            }
            let lower = literal_in_dtype(input[1].node(), arena, &dtype)?;
            let upper = literal_in_dtype(input[2].node(), arena, &dtype)?;
            let lower = if matches!(closed, ClosedInterval::Both | ClosedInterval::Left) {
                gt_eq(value.clone(), lower)
            } else {
                gt(value.clone(), lower)
            };
            let upper = if matches!(closed, ClosedInterval::Both | ClosedInterval::Right) {
                lt_eq(value, upper)
            } else {
                lt(value, upper)
            };
            (and(lower, upper), DataType::Boolean)
        },
        #[cfg(feature = "is_in")]
        AExpr::Function {
            input,
            function:
                IRFunctionExpr::Boolean(IRBooleanFunction::IsIn {
                    nulls_equal,
                    needle_cast,
                }),
            ..
        } if input.len() == 2 && needle_cast.is_none() => {
            let (value, dtype) = child(input[0].node())?;
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
            ) {
                return None;
            }
            let (values, had_nulls) =
                super::try_extract_is_in_haystack(input[1].node(), arena, schema, &dtype, 64)?;
            let terms: Option<Vec<_>> = values
                .iter()
                .map(|av| {
                    let scalar = polars_core::scalar::Scalar::new(dtype.clone(), av.into_static());
                    polars_scalar_to_vortex_for_dtype(&scalar, &dtype)
                        .map(|scalar| eq(value.clone(), lit(scalar)))
                })
                .collect();
            let terms = terms?;
            // Empty membership with null propagation needs a conditional expression.
            // Keep that case in Polars rather than erase nulls under NOT.
            if terms.is_empty() && !*nulls_equal {
                return None;
            }
            let mut membership = or_collect(terms).unwrap_or_else(|| lit(false));
            if *nulls_equal {
                membership = if had_nulls {
                    or(membership, is_null(value))
                } else {
                    fill_null(membership, lit(false))
                };
            }
            (membership, DataType::Boolean)
        },
        AExpr::Function {
            input,
            function: IRFunctionExpr::Boolean(function),
            ..
        } if input.len() == 1 => {
            let (value, dtype) = child(input[0].node())?;
            let expr = match function {
                IRBooleanFunction::IsNull => is_null(value),
                IRBooleanFunction::IsNotNull => not(is_null(value)),
                IRBooleanFunction::Not if dtype == DataType::Boolean => not(value),
                _ => return None,
            };
            (expr, DataType::Boolean)
        },
        #[cfg(feature = "dtype-struct")]
        AExpr::Function {
            input,
            function: IRFunctionExpr::StructExpr(crate::plans::IRStructFunction::FieldByName(name)),
            ..
        } if input.len() == 1 => {
            let (value, dtype) = child(input[0].node())?;
            let DataType::Struct(fields) = dtype else {
                return None;
            };
            let field = fields.iter().find(|field| field.name() == name)?;
            (get_item(name.as_str(), value), field.dtype().clone())
        },
        _ => return None,
    })
}

fn scalar_value(value: &AnyValue<'_>) -> Option<VortexScalar> {
    let nullability = Nullability::NonNullable;
    Some(match value {
        AnyValue::Boolean(value) => VortexScalar::bool(*value, nullability),
        AnyValue::Int8(value) => VortexScalar::primitive(*value, nullability),
        AnyValue::Int16(value) => VortexScalar::primitive(*value, nullability),
        AnyValue::Int32(value) => VortexScalar::primitive(*value, nullability),
        AnyValue::Int64(value) => VortexScalar::primitive(*value, nullability),
        AnyValue::UInt8(value) => VortexScalar::primitive(*value, nullability),
        AnyValue::UInt16(value) => VortexScalar::primitive(*value, nullability),
        AnyValue::UInt32(value) => VortexScalar::primitive(*value, nullability),
        AnyValue::UInt64(value) => VortexScalar::primitive(*value, nullability),
        AnyValue::String(value) => VortexScalar::utf8(*value, nullability),
        AnyValue::StringOwned(value) => VortexScalar::utf8(value.as_str(), nullability),
        _ => {
            let scalar =
                polars_core::scalar::Scalar::new(value.dtype(), value.clone().into_static());
            polars_scalar_to_vortex(&scalar)?
        },
    })
}

#[cfg(test)]
mod tests {
    use polars_core::scalar::Scalar;
    use polars_vortex::read::array_bridge::vortex_array_to_dataframe;
    use polars_vortex::vortex::array::IntoArray;
    use polars_vortex::vortex::array::arrays::{BoolArray, PrimitiveArray, StructArray};

    use super::*;
    use crate::plans::aexpr::builder::AExprBuilder;

    #[test]
    fn nullable_boolean_or_under_not_preserves_kleene_semantics() {
        let mut arena = Arena::default();
        let a = arena.add(AExpr::Column("a".into()));
        let b = arena.add(AExpr::Column("b".into()));
        let either = arena.add(AExpr::BinaryExpr {
            left: a,
            op: Operator::LogicalOr,
            right: b,
        });
        let neither = AExprBuilder::new_from_node(either).not(&mut arena).node();
        let schema = Schema::from_iter([
            ("a".into(), DataType::Boolean),
            ("b".into(), DataType::Boolean),
        ]);
        let expr = aexpr_to_vortex_expression(neither, &arena, &schema).unwrap();
        let input = StructArray::from_fields(&[
            (
                "a",
                BoolArray::from_iter([Some(true), None, Some(false), Some(false)]).into_array(),
            ),
            (
                "b",
                BoolArray::from_iter([None, Some(false), Some(false), Some(true)]).into_array(),
            ),
        ])
        .unwrap()
        .into_array();
        let result = input.apply(&expr).unwrap();
        let frame = vortex_array_to_dataframe(
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
        assert_eq!(actual, vec![Some(false), None, Some(true), Some(false)]);
    }

    #[test]
    fn integer_comparison_preserves_extreme_values_and_nulls() {
        let mut arena = Arena::default();
        let column = arena.add(AExpr::Column("x".into()));
        let zero = arena.add(AExpr::Literal(LiteralValue::Scalar(Scalar::from(0i64))));
        let less = arena.add(AExpr::BinaryExpr {
            left: column,
            op: Operator::Lt,
            right: zero,
        });
        let schema = Schema::from_iter([("x".into(), DataType::Int64)]);
        let expr = aexpr_to_vortex_expression(less, &arena, &schema).unwrap();
        let values = PrimitiveArray::from_option_iter([
            Some(i64::MIN),
            Some(-1i64),
            Some(0),
            Some(i64::MAX),
            None,
        ])
        .into_array();
        let input = StructArray::from_fields(&[("x", values)])
            .unwrap()
            .into_array();
        let result = input.apply(&expr).unwrap();
        let frame = vortex_array_to_dataframe(
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
        assert_eq!(
            actual,
            vec![Some(true), Some(true), Some(false), Some(false), None]
        );
        let unsigned = arena.add(AExpr::Literal(LiteralValue::Scalar(Scalar::from(0u64))));
        let mixed = arena.add(AExpr::BinaryExpr {
            left: column,
            op: Operator::Lt,
            right: unsigned,
        });
        assert!(aexpr_to_vortex_expression(mixed, &arena, &schema).is_none());
    }

    #[test]
    fn unsupported_or_branch_is_never_extracted() {
        let mut arena = Arena::default();
        let flag = arena.add(AExpr::Column("flag".into()));
        let float = arena.add(AExpr::Column("x".into()));
        let zero = arena.add(AExpr::Literal(LiteralValue::Scalar(Scalar::from(0.0f64))));
        let float_eq = arena.add(AExpr::BinaryExpr {
            left: float,
            op: Operator::Eq,
            right: zero,
        });
        let disjunction = arena.add(AExpr::BinaryExpr {
            left: flag,
            op: Operator::LogicalOr,
            right: float_eq,
        });
        let conjunction = arena.add(AExpr::BinaryExpr {
            left: flag,
            op: Operator::LogicalAnd,
            right: float_eq,
        });
        let schema = Schema::from_iter([
            ("flag".into(), DataType::Boolean),
            ("x".into(), DataType::Float64),
        ]);
        assert!(aexpr_to_vortex_expression(disjunction, &arena, &schema).is_none());
        let expr = aexpr_to_vortex_expression(conjunction, &arena, &schema).unwrap();
        // A row that only fails the residual float condition must survive pushdown.
        let input =
            StructArray::from_fields(&[("flag", BoolArray::from_iter([true, false]).into_array())])
                .unwrap()
                .into_array();
        let result = input.apply(&expr).unwrap();
        let frame = vortex_array_to_dataframe(
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
        assert_eq!(actual, vec![Some(true), Some(false)]);
    }
    #[cfg(feature = "is_in")]
    #[test]
    fn literal_membership_preserves_null_conventions_under_not() {
        use polars_core::prelude::{NamedFrom, Series};
        use polars_vortex::vortex::array::arrays::VarBinViewArray;

        use crate::plans::ExprIR;

        for nulls_equal in [false, true] {
            for has_null in [false, true] {
                let mut arena = Arena::default();
                let column = arena.add(AExpr::Column("text".into()));
                let values = if has_null {
                    Series::new("".into(), [Some("MAIL"), None])
                } else {
                    Series::new("".into(), [Some("MAIL")])
                };
                let haystack = arena.add(AExpr::Literal(
                    Scalar::new(
                        DataType::List(Box::new(DataType::String)),
                        AnyValue::List(values),
                    )
                    .into(),
                ));
                let membership = arena.add(AExpr::Function {
                    input: vec![
                        ExprIR::from_node(column, &arena),
                        ExprIR::from_node(haystack, &arena),
                    ],
                    function: IRFunctionExpr::Boolean(IRBooleanFunction::IsIn {
                        nulls_equal,
                        needle_cast: None,
                    }),
                    options: crate::prelude::FunctionOptions::elementwise(),
                });
                let negated = AExprBuilder::new_from_node(membership)
                    .not(&mut arena)
                    .node();
                let schema = Schema::from_iter([("text".into(), DataType::String)]);
                let expression = aexpr_to_vortex_expression(negated, &arena, &schema).unwrap();
                let input = StructArray::from_fields(&[(
                    "text",
                    VarBinViewArray::from_iter_nullable_str([Some("MAIL"), Some("SHIP"), None])
                        .into_array(),
                )])
                .unwrap()
                .into_array();
                let result = input.clone().apply(&expression).unwrap();
                let frame = vortex_array_to_dataframe(
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
                let null_result = if nulls_equal { Some(!has_null) } else { None };
                assert_eq!(actual, vec![Some(false), Some(true), null_result]);
            }
        }
    }

    #[cfg(feature = "dtype-date")]
    #[test]
    fn date_literal_executes_days_and_rejects_millisecond_storage() {
        use polars_vortex::vortex::array::arrays::TemporalArray;
        use polars_vortex::vortex::array::extension::datetime::TimeUnit;
        let mut arena = Arena::default();
        let column = arena.add(AExpr::Column("day".into()));
        let zero = arena.add(AExpr::Literal(
            Scalar::new(DataType::Date, AnyValue::Date(0)).into(),
        ));
        let less = arena.add(AExpr::BinaryExpr {
            left: column,
            op: Operator::Lt,
            right: zero,
        });
        let schema = Schema::from_iter([("day".into(), DataType::Date)]);
        let expression = aexpr_to_vortex_expression(less, &arena, &schema).unwrap();
        let input = StructArray::from_fields(&[(
            "day",
            TemporalArray::new_date(
                PrimitiveArray::from_option_iter([Some(-1i32), Some(0), Some(i32::MAX), None])
                    .into_array(),
                TimeUnit::Days,
            )
            .into_array(),
        )])
        .unwrap()
        .into_array();
        let result = input.clone().apply(&expression).unwrap();
        let frame = vortex_array_to_dataframe(
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
            vec![Some(true), Some(false), Some(false), None]
        );
        let millis = StructArray::from_fields(&[(
            "day",
            TemporalArray::new_date(
                PrimitiveArray::from_iter([1i64]).into_array(),
                TimeUnit::Milliseconds,
            )
            .into_array(),
        )])
        .unwrap()
        .into_array();
        assert!(expression.bind(millis.dtype()).is_err());
        let columns = arena.add(AExpr::BinaryExpr {
            left: column,
            op: Operator::Eq,
            right: column,
        });
        let column_expression = aexpr_to_vortex_expression(columns, &arena, &schema).unwrap();
        assert!(column_expression.bind(input.dtype()).is_ok());
    }
    #[cfg(feature = "dtype-decimal")]
    #[test]
    fn decimal_literal_exact_rescale_executes_and_rejects_loss() {
        use polars_vortex::vortex::array::arrays::DecimalArray;
        use polars_vortex::vortex::dtype::DecimalDType;
        let mut arena = Arena::default();
        let column = arena.add(AExpr::Column("amount".into()));
        let literal = arena.add(AExpr::Literal(
            Scalar::new(DataType::Decimal(8, 4), AnyValue::Decimal(12300, 8, 4)).into(),
        ));
        let equal = arena.add(AExpr::BinaryExpr {
            left: column,
            op: Operator::Eq,
            right: literal,
        });
        let schema = Schema::from_iter([("amount".into(), DataType::Decimal(6, 2))]);
        let expression = aexpr_to_vortex_expression(equal, &arena, &schema).unwrap();
        let input = StructArray::from_fields(&[(
            "amount",
            DecimalArray::from_option_iter(
                [Some(122i32), Some(123), Some(124), None],
                DecimalDType::new(6, 2),
            )
            .into_array(),
        )])
        .unwrap()
        .into_array();
        let result = input.clone().apply(&expression).unwrap();
        let frame = vortex_array_to_dataframe(
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
            vec![Some(false), Some(true), Some(false), None]
        );
        let inexact = arena.add(AExpr::Literal(
            Scalar::new(DataType::Decimal(8, 4), AnyValue::Decimal(12301, 8, 4)).into(),
        ));
        let rejected = arena.add(AExpr::BinaryExpr {
            left: column,
            op: Operator::Eq,
            right: inexact,
        });
        assert!(aexpr_to_vortex_expression(rejected, &arena, &schema).is_none());
    }

    #[cfg(feature = "is_in")]
    #[test]
    fn membership_expansion_has_a_fixed_limit() {
        use polars_core::prelude::{NamedFrom, Series};

        use crate::plans::ExprIR;
        for length in [64, 65] {
            let mut arena = Arena::default();
            let column = arena.add(AExpr::Column("key".into()));
            let values = Series::new("".into(), (0..length).collect::<Vec<i64>>());
            let haystack = arena.add(AExpr::Literal(
                Scalar::new(
                    DataType::List(Box::new(DataType::Int64)),
                    AnyValue::List(values),
                )
                .into(),
            ));
            let membership = arena.add(AExpr::Function {
                input: vec![
                    ExprIR::from_node(column, &arena),
                    ExprIR::from_node(haystack, &arena),
                ],
                function: IRFunctionExpr::Boolean(IRBooleanFunction::IsIn {
                    nulls_equal: false,
                    needle_cast: None,
                }),
                options: crate::prelude::FunctionOptions::elementwise(),
            });
            let schema = Schema::from_iter([("key".into(), DataType::Int64)]);
            assert_eq!(
                aexpr_to_vortex_expression(membership, &arena, &schema).is_some(),
                length == 64
            );
        }
    }
    #[test]
    fn shape_dependent_residuals_disable_all_early_filtering() {
        use polars_core::prelude::{NamedFrom, Series};

        use crate::prelude::SpecialEq;
        let mut arena = Arena::default();
        let column = arena.add(AExpr::Column("x".into()));
        let zero = arena.add(AExpr::Literal(Scalar::from(0i64).into()));
        let gate = arena.add(AExpr::BinaryExpr {
            left: column,
            op: Operator::Gt,
            right: zero,
        });
        let length = arena.add(AExpr::Len);
        let vector = arena.add(AExpr::Literal(LiteralValue::Series(SpecialEq::new(
            Series::new("".into(), [1i64, 2, 3]),
        ))));
        let index = arena.add(AExpr::Literal(Scalar::from(99i64).into()));
        let gather = arena.add(AExpr::Gather {
            expr: column,
            idx: index,
            returns_scalar: true,
            null_on_oob: false,
        });
        let schema = Schema::from_iter([("x".into(), DataType::Int64)]);
        for residual in [length, vector, gather] {
            let predicate = arena.add(AExpr::BinaryExpr {
                left: residual,
                op: Operator::Gt,
                right: zero,
            });
            let conjunction = arena.add(AExpr::BinaryExpr {
                left: gate,
                op: Operator::LogicalAnd,
                right: predicate,
            });
            assert!(aexpr_to_vortex_expression(conjunction, &arena, &schema).is_none());
        }
    }
    #[test]
    fn unsupported_list_ordering_disables_other_native_conjuncts() {
        let mut arena = Arena::default();
        let a = arena.add(AExpr::Column("a".into()));
        let b = arena.add(AExpr::Column("b".into()));
        let key = arena.add(AExpr::Column("k".into()));
        let zero = arena.add(AExpr::Literal(Scalar::from(0i64).into()));
        let invalid = arena.add(AExpr::BinaryExpr {
            left: a,
            op: Operator::Gt,
            right: b,
        });
        let gate = arena.add(AExpr::BinaryExpr {
            left: key,
            op: Operator::Lt,
            right: zero,
        });
        let predicate = arena.add(AExpr::BinaryExpr {
            left: invalid,
            op: Operator::LogicalAnd,
            right: gate,
        });
        let schema = Schema::from_iter([
            ("a".into(), DataType::List(Box::new(DataType::Int64))),
            ("b".into(), DataType::List(Box::new(DataType::Int64))),
            ("k".into(), DataType::Int64),
        ]);
        assert!(aexpr_to_vortex_expression(gate, &arena, &schema).is_some());
        assert!(aexpr_to_vortex_expression(predicate, &arena, &schema).is_none());
    }
}
