//! Conservative conversion of resolved row predicates into Vortex expressions.

use polars_core::prelude::{AnyValue, DataType};
use polars_core::schema::Schema;
use polars_utils::arena::{Arena, Node};
use polars_vortex::vortex::array::scalar::Scalar as VortexScalar;
use polars_vortex::vortex::dtype::Nullability;
use polars_vortex::vortex::expr::{
    Expression, and, and_collect, eq, get_item, gt, gt_eq, is_null, lit, lt, lt_eq, not, not_eq,
    or, root,
};

use crate::plans::iterator::ArenaExprIter;
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
    if arena
        .iter(node)
        .any(|(_, expr)| expr.is_fallible_top_level(arena))
    {
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

fn comparable(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Boolean
            | DataType::String
    )
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
            let (left, left_dtype) = child(*left)?;
            let (right, right_dtype) = child(*right)?;
            if left_dtype != right_dtype {
                return None;
            }
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
        _ => return None,
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
}
