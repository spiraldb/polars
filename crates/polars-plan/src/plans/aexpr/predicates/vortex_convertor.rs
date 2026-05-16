//! AExpr-direct convertor for Polars predicates → Vortex `Expression` (PR-13 path).
//!
//! Translates Polars [`AExpr`] trees into Vortex [`Expression`] trees for filter pushdown,
//! walking the `Arena<AExpr>` directly instead of going through the pre-optimized
//! [`polars_io::predicates::SpecializedColumnPredicate`] shapes that
//! [`polars_vortex::read::predicate::polars_to_vortex_predicate`] consumes. This lets us
//! handle predicates the optimizer doesn't pre-extract — multi-column comparisons,
//! arithmetic in predicates, CAST in predicates, struct field access in predicates,
//! temporal extracts — which today fall through as residual.
//!
//! ## Why this lives in `polars-plan` (not `polars-vortex`)
//!
//! The original PR-2.1 plan row listed `crates/polars-vortex/src/read/aexpr_predicate.rs`
//! as the target location, but `polars-vortex` cannot depend on `polars-plan`: the workspace
//! `polars-plan/Cargo.toml` declares an optional `polars-vortex = { workspace = true, optional
//! = true }` dep gated on the `vortex` feature (mirrored at lines 28 + 80 of that file), so
//! the dependency arrow points polars-plan → polars-vortex. The convertor consumes [`AExpr`],
//! [`LiteralValue`], [`Operator`], and [`IRBooleanFunction`] — all polars-plan types — so it
//! has to live here. (The scalar-conversion helper
//! [`polars_vortex::read::predicate::polars_scalar_to_vortex`] is shared via a public
//! re-export so the AnyValue→VortexScalar mapping has one canonical source of truth across
//! both the SpecializedColumnPredicate path and this AExpr-direct path.)
//!
//! ## What this module covers (PR-2.1 / PR-13.1 foundation)
//!
//! The 14 shapes below — the "kernel" the rest of PR-13 extends.
//!
//! | Shape | AExpr matcher | Vortex builder |
//! |---|---|---|
//! | column reference | `AExpr::Column(name)` | `get_item(name, root())` |
//! | scalar literal | `AExpr::Literal(LiteralValue::Scalar(s))` | `lit(s)` |
//! | equality | `AExpr::BinaryExpr { op: Eq, .. }` | `eq` |
//! | inequality | `AExpr::BinaryExpr { op: NotEq, .. }` | `not_eq` |
//! | `<` | `AExpr::BinaryExpr { op: Lt, .. }` | `lt` |
//! | `<=` | `AExpr::BinaryExpr { op: LtEq, .. }` | `lt_eq` |
//! | `>` | `AExpr::BinaryExpr { op: Gt, .. }` | `gt` |
//! | `>=` | `AExpr::BinaryExpr { op: GtEq, .. }` | `gt_eq` |
//! | logical AND | `AExpr::BinaryExpr { op: And \| LogicalAnd, .. }` | `and` |
//! | logical OR | `AExpr::BinaryExpr { op: Or \| LogicalOr, .. }` | `or` |
//! | `is_null` | `AExpr::Function { Boolean(IsNull), .. }` | `is_null` |
//! | `is_not_null` | `AExpr::Function { Boolean(IsNotNull), .. }` | `is_not_null` |
//! | `not` | `AExpr::Function { Boolean(Not), .. }` | `not` |
//!
//! ## What this module does NOT cover yet (PR-13.2-.5 follow-ups)
//!
//! Arithmetic in predicates (`Plus`/`Minus`/`Multiply`/divides/`Modulus`) → PR-2.2.
//! CAST → PR-2.3. Struct field access (`StructField`) → PR-2.4. Temporal extracts
//! (`AExpr::Function { IRFunctionExpr::TemporalExpr(..), .. }`) → PR-2.5. Anything else
//! (`Sort`, `Gather`, `Filter`, `Agg`, `Ternary`, `AnonymousFunction`, `Over`, `Rolling`,
//! etc.) returns `None` and falls through as residual; the multi-scan layer re-applies the
//! full predicate post-decode so dropping coverage is always SOUND, just suboptimal.
//!
//! ## Wiring
//!
//! No call site yet. PR-2.2 will wire the convertor at
//! `crates/polars-stream/src/physical_plan/to_graph.rs:843` (inside the `FileScanIR::Vortex`
//! branch of `lower_node`), where the [`AExpr`] arena is live alongside the predicate
//! `ExprIR`. The resulting `Expression` is attached to the Vortex `VortexReaderBuilder` via
//! a Vortex-specific side channel (parallel to how `FileScanIR::Vortex::metadata` and the
//! new (PR-2.0) `segment_cache` already thread).

use polars_utils::arena::{Arena, Node};
use polars_vortex::vortex::expr::{
    Expression, and, eq, get_item, gt, gt_eq, is_not_null, is_null, lit, lt, lt_eq, not, not_eq,
    or, root,
};

use crate::dsl::Operator;
use crate::plans::aexpr::function_expr::{IRBooleanFunction, IRFunctionExpr};
use crate::plans::lit::LiteralValue;
use crate::plans::AExpr;

/// Convert a Polars AExpr predicate tree into a Vortex [`Expression`] for pushdown.
///
/// Returns `Some(expr)` when every node in the tree maps to a Vortex expression; returns
/// `None` if ANY node is unsupported (Vortex can't filter on what it can't represent, so
/// partial pushdown would be incorrect — *narrower* than the user's predicate). The
/// multi-scan layer always re-applies the full predicate post-decode, so `None` is the
/// safe fallback.
///
/// # Parameters
///
/// - `root_node` — the AExpr root to convert. The convertor walks the tree rooted here.
/// - `arena` — the [`AExpr`] arena. Borrowed immutably (no nodes added).
///
/// # Returns
///
/// `Some(Expression)` on full pushdown; `None` if any shape can't be translated. Always
/// SAFE — the caller treats `None` as "not pushable" and lets the residual filter run.
///
/// # ⚠️ Bitwise-vs-logical operator caveat — addressed at the call site in PR-2.2
///
/// Polars' [`Operator::And`] / [`Operator::Or`] and [`IRBooleanFunction::Not`] are
/// **bitwise-OR-logical**: they work on integer columns as bitwise ops AND on bool columns
/// as logical ops. The annotation `// Also bitwise negate` at
/// `crates/polars-plan/src/plans/aexpr/function_expr/boolean.rs:49` is explicit about this.
///
/// Vortex's [`and`], [`or`], and [`not`] are **boolean-only**.
///
/// The convertor maps all three unconditionally to Vortex's boolean variants. For the
/// typical predicate root (a boolean tree consumed by `WHERE`), this is correct. But for
/// embedded integer-bitwise sub-trees — e.g., `(col_int & 1) > 0` — the convertor would
/// emit `gt(and(col_int, lit(1)), lit(0))`, which is semantically wrong (Vortex's `and`
/// is undefined on integer arrays).
///
/// **TODO (PR-2.2 wire-up)**: When wiring at `to_graph.rs:843`, the call site has the
/// `output_schema` available. Either (a) skip-the-pushdown when any operand of And/Or/Not
/// is non-bool — this is what [`super::column_expr`] does at lines 245-247 — or (b) thread
/// a `&Schema` into this convertor and guard inside the match arms. Option (a) at the call
/// site is cheaper because the convertor stays schema-free. Tracked: PR-2.1 cycle-1
/// should-fix items (both fresh + correctness lenses).
pub fn aexpr_to_vortex_expression(
    root_node: Node,
    arena: &Arena<AExpr>,
) -> Option<Expression> {
    match arena.get(root_node) {
        // --- leaves ---
        AExpr::Column(name) => Some(get_item(name.as_str(), root())),
        AExpr::Literal(lv) => convert_literal(lv),

        // --- comparisons + boolean combinators (BinaryExpr) ---
        AExpr::BinaryExpr { left, op, right } => {
            let lhs = aexpr_to_vortex_expression(*left, arena)?;
            let rhs = aexpr_to_vortex_expression(*right, arena)?;
            Some(match op {
                Operator::Eq => eq(lhs, rhs),
                Operator::NotEq => not_eq(lhs, rhs),
                Operator::Lt => lt(lhs, rhs),
                Operator::LtEq => lt_eq(lhs, rhs),
                Operator::Gt => gt(lhs, rhs),
                Operator::GtEq => gt_eq(lhs, rhs),
                Operator::And | Operator::LogicalAnd => and(lhs, rhs),
                Operator::Or | Operator::LogicalOr => or(lhs, rhs),
                // EqValidity / NotEqValidity — null-aware equality variants Vortex doesn't
                // have a direct equivalent for; fall through to residual.
                Operator::EqValidity | Operator::NotEqValidity => return None,
                // Arithmetic (Plus/Minus/Multiply/RustDivide/TrueDivide/FloorDivide/Modulus)
                // → PR-2.2 / PR-13.2.
                Operator::Plus
                | Operator::Minus
                | Operator::Multiply
                | Operator::RustDivide
                | Operator::TrueDivide
                | Operator::FloorDivide
                | Operator::Modulus => return None,
                // Bitwise Xor — no Vortex equivalent in the predicate context; residual.
                Operator::Xor => return None,
            })
        },

        // --- unary boolean functions (IsNull / IsNotNull / Not) ---
        AExpr::Function {
            input,
            function: IRFunctionExpr::Boolean(boolean_fn),
            ..
        } => {
            // All three of IsNull / IsNotNull / Not are unary — one Node input.
            // `input` is `Vec<ExprIR>`; we take the first and unwrap its node.
            let arg_node = input.first().map(|expr_ir| expr_ir.node())?;
            let arg = aexpr_to_vortex_expression(arg_node, arena)?;
            match boolean_fn {
                IRBooleanFunction::IsNull => Some(is_null(arg)),
                IRBooleanFunction::IsNotNull => Some(is_not_null(arg)),
                IRBooleanFunction::Not => Some(not(arg)),
                // Other IRBooleanFunction variants (IsIn, IsBetween, AllHorizontal, etc.)
                // are not in the PR-2.1 foundation scope; PR-2.2..PR-2.5 may add some.
                _ => None,
            }
        },

        // --- unsupported shapes (residual) ---
        // Cast → PR-2.3. StructField → PR-2.4. Other Function variants (temporal etc.)
        // → PR-2.5. Sort/Gather/Filter/Agg/Ternary/AnonymousFunction/Over/Rolling etc. all
        // fall through to residual unconditionally.
        _ => None,
    }
}

/// Convert a Polars [`LiteralValue`] to a Vortex literal [`Expression`].
///
/// Only [`LiteralValue::Scalar`] is handled in the PR-2.1 foundation; [`LiteralValue::Dyn`],
/// [`LiteralValue::Series`], and [`LiteralValue::Range`] fall through to `None`. `Dyn`
/// requires type-inference context (target dtype) which the convertor doesn't have access
/// to without a schema; `Series` and `Range` aren't sensible predicates anyway.
fn convert_literal(lv: &LiteralValue) -> Option<Expression> {
    match lv {
        LiteralValue::Scalar(scalar) => {
            Some(lit(polars_vortex::read::predicate::polars_scalar_to_vortex(
                scalar,
            )?))
        },
        // Dyn requires materialization against a target dtype; not in foundation scope.
        // Series / Range aren't valid predicate literals.
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests for each of the 14 PR-2.1 / PR-13.1 shapes.
    //!
    //! Each test builds a small AExpr tree directly in an `Arena<AExpr>` (no DSL involved),
    //! calls [`aexpr_to_vortex_expression`], and asserts the conversion returns `Some` (the
    //! Vortex expression's structure is opaque to the test — we trust the builder helpers).
    //! Unsupported shapes additionally assert `None`.
    use polars_core::prelude::{AnyValue, DataType};
    use polars_core::scalar::Scalar;
    use polars_utils::arena::Arena;
    use polars_utils::pl_str::PlSmallStr;

    use polars_core::chunked_array::cast::CastOptions;

    use super::*;
    use crate::plans::lit::DynLiteralValue;
    use crate::plans::{ExprIR, OutputName};
    use crate::prelude::FunctionOptions;

    /// Helper: add an `AExpr::Column(name)` to the arena and return its node id.
    fn col(arena: &mut Arena<AExpr>, name: &str) -> Node {
        arena.add(AExpr::Column(PlSmallStr::from(name)))
    }

    /// Helper: add an `AExpr::Literal(Scalar(<i32>))` literal.
    fn lit_i32(arena: &mut Arena<AExpr>, value: i32) -> Node {
        let scalar = Scalar::new(DataType::Int32, AnyValue::Int32(value));
        arena.add(AExpr::Literal(LiteralValue::Scalar(scalar)))
    }

    /// Helper: build a `BinaryExpr` with the given operator.
    fn binop(arena: &mut Arena<AExpr>, left: Node, op: Operator, right: Node) -> Node {
        arena.add(AExpr::BinaryExpr { left, op, right })
    }

    /// Helper: build an `AExpr::Function` with a Boolean function variant.
    fn boolean_fn(
        arena: &mut Arena<AExpr>,
        bf: IRBooleanFunction,
        arg: Node,
    ) -> Node {
        // ExprIR::new takes a node + OutputName; for predicate-arena tests we use a dummy
        // empty alias (the OutputName isn't consulted by the convertor).
        let expr_ir = ExprIR::new(arg, OutputName::Alias(PlSmallStr::EMPTY));
        arena.add(AExpr::Function {
            input: vec![expr_ir],
            function: IRFunctionExpr::Boolean(bf),
            options: FunctionOptions::default(),
        })
    }

    // === Shape coverage tests (14 shapes) ===

    #[test]
    fn shape_column() {
        let mut arena = Arena::new();
        let n = col(&mut arena, "a");
        assert!(aexpr_to_vortex_expression(n, &arena).is_some());
    }

    #[test]
    fn shape_literal_scalar() {
        let mut arena = Arena::new();
        let n = lit_i32(&mut arena, 42);
        assert!(aexpr_to_vortex_expression(n, &arena).is_some());
    }

    #[test]
    fn shape_eq() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 42);
        let n = binop(&mut arena, c, Operator::Eq, l);
        assert!(aexpr_to_vortex_expression(n, &arena).is_some());
    }

    #[test]
    fn shape_not_eq() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 42);
        let n = binop(&mut arena, c, Operator::NotEq, l);
        assert!(aexpr_to_vortex_expression(n, &arena).is_some());
    }

    #[test]
    fn shape_lt() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 42);
        let n = binop(&mut arena, c, Operator::Lt, l);
        assert!(aexpr_to_vortex_expression(n, &arena).is_some());
    }

    #[test]
    fn shape_lt_eq() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 42);
        let n = binop(&mut arena, c, Operator::LtEq, l);
        assert!(aexpr_to_vortex_expression(n, &arena).is_some());
    }

    #[test]
    fn shape_gt() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 42);
        let n = binop(&mut arena, c, Operator::Gt, l);
        assert!(aexpr_to_vortex_expression(n, &arena).is_some());
    }

    #[test]
    fn shape_gt_eq() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 42);
        let n = binop(&mut arena, c, Operator::GtEq, l);
        assert!(aexpr_to_vortex_expression(n, &arena).is_some());
    }

    #[test]
    fn shape_and() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 42);
        let eq_node = binop(&mut arena, c, Operator::Eq, l);
        let c2 = col(&mut arena, "b");
        let l2 = lit_i32(&mut arena, 7);
        let lt_node = binop(&mut arena, c2, Operator::Lt, l2);
        let n = binop(&mut arena, eq_node, Operator::And, lt_node);
        assert!(aexpr_to_vortex_expression(n, &arena).is_some());
    }

    #[test]
    fn shape_or() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 42);
        let eq_node = binop(&mut arena, c, Operator::Eq, l);
        let c2 = col(&mut arena, "b");
        let l2 = lit_i32(&mut arena, 7);
        let lt_node = binop(&mut arena, c2, Operator::Lt, l2);
        let n = binop(&mut arena, eq_node, Operator::Or, lt_node);
        assert!(aexpr_to_vortex_expression(n, &arena).is_some());
    }

    #[test]
    fn shape_logical_and() {
        // LogicalAnd is the short-circuit form Polars uses internally for boolean
        // simplification; it should map to vortex::expr::and same as Operator::And.
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 42);
        let left = binop(&mut arena, c, Operator::Eq, l);
        let c2 = col(&mut arena, "b");
        let l2 = lit_i32(&mut arena, 7);
        let right = binop(&mut arena, c2, Operator::Lt, l2);
        let n = binop(&mut arena, left, Operator::LogicalAnd, right);
        assert!(aexpr_to_vortex_expression(n, &arena).is_some());
    }

    #[test]
    fn shape_logical_or() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 42);
        let left = binop(&mut arena, c, Operator::Eq, l);
        let c2 = col(&mut arena, "b");
        let l2 = lit_i32(&mut arena, 7);
        let right = binop(&mut arena, c2, Operator::Lt, l2);
        let n = binop(&mut arena, left, Operator::LogicalOr, right);
        assert!(aexpr_to_vortex_expression(n, &arena).is_some());
    }

    #[test]
    fn shape_is_null() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let n = boolean_fn(&mut arena, IRBooleanFunction::IsNull, c);
        assert!(aexpr_to_vortex_expression(n, &arena).is_some());
    }

    #[test]
    fn shape_is_not_null() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let n = boolean_fn(&mut arena, IRBooleanFunction::IsNotNull, c);
        assert!(aexpr_to_vortex_expression(n, &arena).is_some());
    }

    #[test]
    fn shape_not() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 42);
        let eq_node = binop(&mut arena, c, Operator::Eq, l);
        let n = boolean_fn(&mut arena, IRBooleanFunction::Not, eq_node);
        assert!(aexpr_to_vortex_expression(n, &arena).is_some());
    }

    // === Unsupported-shapes-return-None coverage ===

    #[test]
    fn unsupported_arithmetic_returns_none() {
        // `col + 1` is PR-2.2's scope, not PR-2.1. Foundation must return None.
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 1);
        let n = binop(&mut arena, c, Operator::Plus, l);
        assert!(aexpr_to_vortex_expression(n, &arena).is_none());
    }

    #[test]
    fn unsupported_xor_returns_none() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 1);
        let n = binop(&mut arena, c, Operator::Xor, l);
        assert!(aexpr_to_vortex_expression(n, &arena).is_none());
    }

    #[test]
    fn unsupported_eq_validity_returns_none() {
        // EqValidity is the null-aware equality variant Vortex doesn't have a direct
        // equivalent for; the convertor explicitly returns None at the match arm.
        // (cycle-1 fresh-lens F-002.)
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 1);
        let n = binop(&mut arena, c, Operator::EqValidity, l);
        assert!(aexpr_to_vortex_expression(n, &arena).is_none());
    }

    #[test]
    fn unsupported_not_eq_validity_returns_none() {
        // NotEqValidity — paired with EqValidity. (cycle-1 fresh-lens F-002.)
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 1);
        let n = binop(&mut arena, c, Operator::NotEqValidity, l);
        assert!(aexpr_to_vortex_expression(n, &arena).is_none());
    }

    #[test]
    fn unsupported_cast_returns_none() {
        // `col.cast(Int64)` is PR-2.3's scope.
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let n = arena.add(AExpr::Cast {
            expr: c,
            dtype: DataType::Int64,
            options: CastOptions::Strict,
        });
        assert!(aexpr_to_vortex_expression(n, &arena).is_none());
    }

    #[test]
    fn unsupported_literal_dyn_returns_none() {
        // Dyn literals need a target dtype for materialization; foundation falls through.
        let mut arena = Arena::new();
        let n = arena.add(AExpr::Literal(LiteralValue::Dyn(DynLiteralValue::Int(42))));
        assert!(aexpr_to_vortex_expression(n, &arena).is_none());
    }

    /// Sanity: a deeply nested predicate `(a == 1) AND ((b > 2) OR is_null(c))`
    /// passes through end-to-end.
    #[test]
    fn integration_nested_predicate() {
        let mut arena = Arena::new();
        let a = col(&mut arena, "a");
        let one = lit_i32(&mut arena, 1);
        let a_eq_1 = binop(&mut arena, a, Operator::Eq, one);

        let b = col(&mut arena, "b");
        let two = lit_i32(&mut arena, 2);
        let b_gt_2 = binop(&mut arena, b, Operator::Gt, two);

        let c = col(&mut arena, "c");
        let c_is_null = boolean_fn(&mut arena, IRBooleanFunction::IsNull, c);

        let inner_or = binop(&mut arena, b_gt_2, Operator::Or, c_is_null);
        let root_node = binop(&mut arena, a_eq_1, Operator::And, inner_or);

        assert!(aexpr_to_vortex_expression(root_node, &arena).is_some());
    }

    /// Sanity: any single unsupported sub-shape poisons the whole tree.
    #[test]
    fn integration_unsupported_subexpr_returns_none() {
        // `(a + 1) == 5` — arithmetic in predicates is PR-2.2; whole tree should be None.
        let mut arena = Arena::new();
        let a = col(&mut arena, "a");
        let one = lit_i32(&mut arena, 1);
        let a_plus_1 = binop(&mut arena, a, Operator::Plus, one);
        let five = lit_i32(&mut arena, 5);
        let n = binop(&mut arena, a_plus_1, Operator::Eq, five);
        assert!(aexpr_to_vortex_expression(n, &arena).is_none());
    }
}
