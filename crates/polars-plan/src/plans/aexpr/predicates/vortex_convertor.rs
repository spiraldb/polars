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
//! ## What this module covers (PR-2.1 foundation + PR-2.2 extensions)
//!
//! The 14 shapes below — the "kernel" the rest of PR-13 extends. (13 from PR-2.1's
//! foundation + 1 from PR-2.2: `addition (numeric)`.)
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
//! | logical AND | `AExpr::BinaryExpr { op: And \| LogicalAnd, .. }` | `and` (schema-gated for `And`) |
//! | logical OR | `AExpr::BinaryExpr { op: Or \| LogicalOr, .. }` | `or` (schema-gated for `Or`) |
//! | addition (numeric) | `AExpr::BinaryExpr { op: Plus, .. }` (PR-2.2) | `checked_add` (schema-gated to numeric) |
//! | `is_null` | `AExpr::Function { Boolean(IsNull), .. }` | `is_null` |
//! | `is_not_null` | `AExpr::Function { Boolean(IsNotNull), .. }` | `is_not_null` |
//! | `not` | `AExpr::Function { Boolean(Not), .. }` | `not` (schema-gated) |
//!
//! ## What this module does NOT cover yet (PR-2.3-.5 follow-ups)
//!
//! Remaining arithmetic (`Minus`/`Multiply`/divides/`Modulus`) → still residual; PR-2.2
//! ships `Plus` only because `checked_add` is the only arithmetic builder publicly exposed
//! in `vortex::expr::*` at the pinned SHA. CAST → PR-2.3. Struct field access
//! (`StructField`) → PR-2.4. Temporal extracts (`AExpr::Function {
//! IRFunctionExpr::TemporalExpr(..), .. }`) → PR-2.5. Anything else (`Sort`, `Gather`,
//! `Filter`, `Agg`, `Ternary`, `AnonymousFunction`, `Over`, `Rolling`, etc.) returns `None`
//! and falls through as residual; the multi-scan layer re-applies the full predicate
//! post-decode so dropping coverage is always SOUND, just suboptimal.
//!
//! ## Wiring
//!
//! Wired at `crates/polars-stream/src/physical_plan/lower_ir.rs:780-791` (inside the
//! `FileScanIR::Vortex` branch of `lower_ir`), where the [`AExpr`] arena is live alongside
//! the predicate `ExprIR`. The resulting `Expression` is attached to the Vortex
//! `VortexReaderBuilder.aexpr_filter` field via a Vortex-specific side channel (parallel
//! to how `FileScanIR::Vortex::metadata` and the (PR-2.0) `segment_cache` thread).
//! `VortexFileReader::begin_read` prefers `aexpr_filter` over the legacy
//! `polars_to_vortex_predicate` path; PR-2.6 will delete the legacy path once the
//! convertor is a strict superset of `SpecializedColumnPredicate` coverage.

use polars_core::prelude::DataType;
use polars_core::schema::Schema;
use polars_utils::arena::{Arena, Node};
use polars_vortex::vortex::dtype::{DType, Nullability, PType};
use polars_vortex::vortex::expr::{
    Expression, and, cast, checked_add, eq, get_item, gt, gt_eq, is_not_null, is_null, lit, lt,
    lt_eq, not, not_eq, or, root,
};

use crate::dsl::Operator;
use crate::plans::AExpr;
use crate::plans::aexpr::function_expr::{IRBooleanFunction, IRFunctionExpr};
use crate::plans::lit::LiteralValue;

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
/// - `schema` — the resolved [`Schema`] of the columns the AExpr references. Used by the
///   And/Or/Not bitwise-vs-logical gate (which refuses pushdown on integer operands) and
///   by the Plus arm's numeric gate (which refuses pushdown on non-numeric operands). When
///   `None`, the convertor conservatively refuses And/Or/Not AND Plus pushdown. Production
///   wire-up (`physical_plan::lower_ir`) always supplies `Some`; `None` is exposed only
///   to keep ad-hoc unit-test construction ergonomic.
///
/// # Returns
///
/// `Some(Expression)` on full pushdown; `None` if any shape can't be translated. Always
/// SAFE — the caller treats `None` as "not pushable" and lets the residual filter run.
///
/// # Bitwise-vs-logical operator caveat (addressed via schema gate)
///
/// Polars' [`Operator::And`] / [`Operator::Or`] and [`IRBooleanFunction::Not`] are
/// **bitwise-OR-logical**: they work on integer columns as bitwise ops AND on bool columns
/// as logical ops. (Operator::And/Or dispatch through `aexpr/schema.rs::get_arithmetic_field`
/// which returns the operand dtype unchanged; for `Not`, see
/// `crates/polars-plan/src/plans/aexpr/function_expr/boolean.rs:49 // Also bitwise negate`.)
/// Vortex's [`and`], [`or`], and [`not`] are **boolean-only**.
///
/// PR-2.2 mitigates by threading `schema` and gating each of And/Or/Not on
/// `operand_is_bool` (mirroring [`super::column_expr`]'s `dtype.is_bool()` guard at lines
/// 245-247). For `LogicalAnd` / `LogicalOr` we skip the gate because the IR-level "logical"
/// form is by construction boolean-typed.
///
/// # Arithmetic semantic caveat (PR-2.2 / PR-13.2)
///
/// Plus is mapped to Vortex's `checked_add` — the only arithmetic builder publicly exposed
/// in `vortex::expr::*` at the pinned SHA. Vortex's `checked_add` is **fallible on
/// overflow** (`vortex-array/src/expr/analysis/fallible.rs:36
/// checked_add_defaults_to_fallible`): an integer overflow during scan errors out at
/// scan-time rather than silently wrapping (Polars' `+` operator wraps). The convertor
/// only emits `checked_add` when both operands are numeric (the `operand_is_numeric` gate);
/// String/Bool/Date/Datetime/Time/Duration/Struct/List Plus operations refuse pushdown
/// (without the gate, Vortex's `Binary::coerce_args` would `vortex_bail!` at scan-time,
/// violating the always-SAFE-fallback contract). For numeric Plus near boundary values,
/// the user observes
/// a scan-time error instead of Polars' wrapping behavior. This is a known semantic
/// divergence; see Deferred work (`Vortex wrapping_add public API`).
pub fn aexpr_to_vortex_expression(
    root_node: Node,
    arena: &Arena<AExpr>,
    schema: Option<&Schema>,
) -> Option<Expression> {
    match arena.get(root_node) {
        // --- leaves ---
        AExpr::Column(name) => Some(get_item(name.as_str(), root())),
        AExpr::Literal(lv) => convert_literal(lv),

        // --- comparisons + boolean combinators (BinaryExpr) ---
        AExpr::BinaryExpr { left, op, right } => {
            // Bitwise-vs-logical schema gate (cycle-1 should-fix from PR-2.1): Polars
            // `And/Or` are bitwise-or-logical (aexpr/schema.rs:127-149 dispatches through
            // `get_arithmetic_field` so output dtype follows operand dtype). Vortex's
            // `and`/`or` are boolean-only. Refuse pushdown when either operand is not
            // boolean. `LogicalAnd`/`LogicalOr` are skipped — the IR-level "logical" form
            // is by construction boolean-typed.
            if matches!(op, Operator::And | Operator::Or) {
                let Some(s) = schema else { return None };
                if !operand_is_bool(*left, arena, s) || !operand_is_bool(*right, arena, s) {
                    return None;
                }
            }
            // Plus numeric gate (PR-2.2 cycle-1 must-fix): Vortex's `checked_add` is only
            // valid on numeric primitives with matching dtypes
            // (`vortex-array/src/scalar_fn/fns/binary/mod.rs:104-128`). Polars allows Plus
            // on String (concat), Bool, Date+Duration, etc. — emitting `checked_add` on
            // those would `vortex_bail!` at scan-time, violating the always-SAFE-fallback
            // contract. Refuse when either operand is non-numeric or when the schema is
            // unavailable (conservative).
            if matches!(op, Operator::Plus) {
                let Some(s) = schema else { return None };
                if !operand_is_numeric(*left, arena, s) || !operand_is_numeric(*right, arena, s) {
                    return None;
                }
            }
            let lhs = aexpr_to_vortex_expression(*left, arena, schema)?;
            let rhs = aexpr_to_vortex_expression(*right, arena, schema)?;
            Some(match op {
                Operator::Eq => eq(lhs, rhs),
                Operator::NotEq => not_eq(lhs, rhs),
                Operator::Lt => lt(lhs, rhs),
                Operator::LtEq => lt_eq(lhs, rhs),
                Operator::Gt => gt(lhs, rhs),
                Operator::GtEq => gt_eq(lhs, rhs),
                Operator::And | Operator::LogicalAnd => and(lhs, rhs),
                Operator::Or | Operator::LogicalOr => or(lhs, rhs),
                // Plus → Vortex `checked_add` (PR-2.2 / PR-13.2). The only Vortex
                // arithmetic builder publicly exposed in `vortex::expr::*` is
                // `checked_add`; Sub/Mul/Div remain residual until upstream exposes
                // the corresponding `checked_sub`/etc. helpers (or until polars-vortex
                // adopts the raw `Binary.try_new_expr(Operator::Sub, ...)` form).
                Operator::Plus => checked_add(lhs, rhs),
                // EqValidity / NotEqValidity — null-aware equality variants Vortex doesn't
                // have a direct equivalent for; fall through to residual.
                Operator::EqValidity | Operator::NotEqValidity => return None,
                // Other arithmetic (Minus/Multiply/RustDivide/TrueDivide/FloorDivide/
                // Modulus) → still residual; PR-2.2 ships Plus only, per the plan's
                // PR-13.2 acceptance test (`col + 1 == 5`).
                Operator::Minus
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
            // Schema gate for `Not` — Polars `IRBooleanFunction::Not` is bitwise-or-
            // logical (boolean.rs:49 `// Also bitwise negate`); Vortex `not` is
            // boolean-only. Polars's own `column_expr.rs:245-247` performs the same
            // `dtype.is_bool()` guard. IsNull/IsNotNull accept any dtype and produce
            // boolean output, so no gate.
            if matches!(boolean_fn, IRBooleanFunction::Not) {
                let Some(s) = schema else { return None };
                if !operand_is_bool(arg_node, arena, s) {
                    return None;
                }
            }
            let arg = aexpr_to_vortex_expression(arg_node, arena, schema)?;
            match boolean_fn {
                IRBooleanFunction::IsNull => Some(is_null(arg)),
                IRBooleanFunction::IsNotNull => Some(is_not_null(arg)),
                IRBooleanFunction::Not => Some(not(arg)),
                // Other IRBooleanFunction variants (IsIn, IsBetween, AllHorizontal, etc.)
                // are not in the PR-2.1 foundation scope; PR-2.2..PR-2.5 may add some.
                _ => None,
            }
        },

        // --- CAST (PR-2.3 / PR-13.3) ---
        // `col.cast(Int64) > 100` against an Int32 column pushes down as
        // `gt(cast(get_item("col", root()), DType::Primitive(I64, Nullable)), lit(100i64))`.
        // The target dtype is materialized via `polars_dtype_to_vortex_dtype`; unsupported
        // targets (Decimal — scale/precision interactions; Object — opaque; Categorical /
        // Enum — string-encoded; Date/Time/Datetime/Duration — Extension types beyond
        // Vortex's PType/Bool/Utf8 set) fall through to residual via `?`-propagation.
        AExpr::Cast {
            expr: inner,
            dtype,
            options: _,
        } => {
            let target = polars_dtype_to_vortex_dtype(dtype)?;
            let child = aexpr_to_vortex_expression(*inner, arena, schema)?;
            Some(cast(child, target))
        },

        // --- unsupported shapes (residual) ---
        // StructField → PR-2.4. Other Function variants (temporal etc.) → PR-2.5.
        // Sort/Gather/Filter/Agg/Ternary/AnonymousFunction/Over/Rolling etc. all fall
        // through to residual unconditionally.
        _ => None,
    }
}

/// Convert a Polars [`DataType`] to a Vortex [`DType`] for the CAST arm. Returns `None`
/// for dtypes Vortex doesn't natively represent as a `Primitive`/`Bool`/`Utf8` (Decimal,
/// Object, Categorical/Enum, temporal Extension types). Nullability defaults to
/// `Nullable` because Polars's runtime allows null in any column unless statically proven
/// otherwise; the runtime nullable Vortex dtype is a strict superset and CAST to a
/// nullable type is always safe.
///
/// **Scope**: PR-2.3 covers only primitive numeric + Bool + Utf8 CAST targets. Decimal
/// is deliberately refused (Vortex `DType::Decimal` requires `DecimalDType(precision,
/// scale)` and polars-vortex hasn't validated CAST-via-`vortex::expr::cast` interactions
/// at the Vortex layer). Other targets are PR-2.4/.5 scope or permanent residuals.
fn polars_dtype_to_vortex_dtype(dt: &DataType) -> Option<DType> {
    use DataType::*;
    let nullable = Nullability::Nullable;
    Some(match dt {
        Boolean => DType::Bool(nullable),
        Int8 => DType::Primitive(PType::I8, nullable),
        Int16 => DType::Primitive(PType::I16, nullable),
        Int32 => DType::Primitive(PType::I32, nullable),
        Int64 => DType::Primitive(PType::I64, nullable),
        UInt8 => DType::Primitive(PType::U8, nullable),
        UInt16 => DType::Primitive(PType::U16, nullable),
        UInt32 => DType::Primitive(PType::U32, nullable),
        UInt64 => DType::Primitive(PType::U64, nullable),
        Float32 => DType::Primitive(PType::F32, nullable),
        Float64 => DType::Primitive(PType::F64, nullable),
        String => DType::Utf8(nullable),
        // Decimal — Vortex `DType::Decimal(DecimalDType(precision, scale), nullable)`
        // requires usize ↔ u8 narrow + validation per the project BAN against `as` casts
        // on Vortex Decimal precision/scale. Deferred to a future PR.
        // Object/Categorical/Enum/Date/Datetime/Time/Duration/Binary/List/Struct/Array/
        // Null/Unknown — not in PR-2.3 scope.
        _ => return None,
    })
}

/// Schema-aware operand type check: determines whether `node`'s resolved dtype is
/// `Boolean`. Used by the And/Or/Not schema gate to refuse bitwise-on-integer pushdown.
///
/// Conservatively returns `false` when the dtype can't be resolved (unknown column,
/// nested expression we can't trivially type-check). The caller's None-fallback then
/// drops the And/Or/Not arm to residual — always SOUND.
fn operand_is_bool(node: Node, arena: &Arena<AExpr>, schema: &Schema) -> bool {
    match arena.get(node) {
        AExpr::Column(name) => matches!(schema.get(name), Some(DataType::Boolean)),
        AExpr::Literal(LiteralValue::Scalar(s)) => matches!(s.dtype(), DataType::Boolean),
        AExpr::BinaryExpr { left, op, right } => match op {
            // Comparisons unconditionally produce Boolean.
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::EqValidity
            | Operator::NotEqValidity
            // LogicalAnd/LogicalOr are the IR-level "logical" form; by construction
            // their inputs are boolean-typed.
            | Operator::LogicalAnd
            | Operator::LogicalOr => true,
            // And/Or follow operand dtype: bool-input → bool-output (logical), int-input
            // → int-output (bitwise). Recurse on inputs to determine.
            Operator::And | Operator::Or => {
                operand_is_bool(*left, arena, schema) && operand_is_bool(*right, arena, schema)
            },
            // Arithmetic and bitwise Xor produce non-bool output.
            _ => false,
        },
        // Enumerate the IRBooleanFunction variants we know produce Boolean array output.
        // `Not` is bitwise-or-logical (output dtype = input dtype), so we recurse on its
        // arg. Other variants (Any/All/IsEmpty produce scalar bool, not array bool; the
        // outer convertor returns None for those anyway via the `_ => None` arm in the
        // Function match) are conservatively treated as non-bool here — the convertor's
        // own arm-level None fallback is the second line of defense.
        AExpr::Function {
            input,
            function: IRFunctionExpr::Boolean(bf),
            ..
        } => match bf {
            IRBooleanFunction::IsNull | IRBooleanFunction::IsNotNull => true,
            IRBooleanFunction::Not => input
                .first()
                .map(|expr_ir| operand_is_bool(expr_ir.node(), arena, schema))
                .unwrap_or(false),
            _ => false,
        },
        _ => false,
    }
}

/// Schema-aware operand type check: determines whether `node`'s resolved dtype is a
/// numeric primitive compatible with Vortex's `checked_add`. Used by the Plus gate to
/// refuse pushdown on String / Bool / Date / Struct / List operands that Vortex's
/// `Binary::coerce_args` would `vortex_bail!` on at scan-time.
///
/// Conservatively returns `false` when the dtype can't be resolved (unknown column,
/// nested expression). The caller's None-fallback drops the Plus arm to residual.
fn operand_is_numeric(node: Node, arena: &Arena<AExpr>, schema: &Schema) -> bool {
    match arena.get(node) {
        AExpr::Column(name) => schema.get(name).is_some_and(is_vortex_numeric_dtype),
        AExpr::Literal(LiteralValue::Scalar(s)) => is_vortex_numeric_dtype(s.dtype()),
        // Recursive `Plus` produces numeric output if both operands are numeric; this
        // lets `(col + 1) + 1` push down (operands at each level are numeric).
        AExpr::BinaryExpr {
            left,
            op: Operator::Plus,
            right,
        } => operand_is_numeric(*left, arena, schema) && operand_is_numeric(*right, arena, schema),
        _ => false,
    }
}

/// Is `dt` one of the Vortex-primitive dtypes that `checked_add` accepts?
/// Mirrors Vortex's `is_primitive() && eq_ignore_nullability` precondition (numeric
/// integer + float). Decimals are deliberately NOT numeric here — Vortex's `Decimal` is
/// primitive but `checked_add` on Decimals has scale/precision interactions polars-vortex
/// hasn't validated; refuse for safety.
fn is_vortex_numeric_dtype(dt: &DataType) -> bool {
    use DataType::*;
    // List mirrors Vortex's `PType` ceiling (I8/I16/I32/I64/F16/F32/F64 + unsigned)
    // AND `polars_vortex::read::predicate::polars_scalar_to_vortex`'s supported literal
    // arms. `Int128` and `UInt128` are intentionally excluded: both exist in polars-core
    // (`DataType::Int128`/`UInt128`) but neither is a Vortex primitive AND
    // `polars_scalar_to_vortex` has no Int128/UInt128 arms, so a literal-of-that-type
    // would fall through `?`-propagation anyway. Listing them here would be misleading
    // (suggesting support that isn't wired).
    matches!(
        dt,
        Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Float32 | Float64
    )
}

/// Convert a Polars [`LiteralValue`] to a Vortex literal [`Expression`].
///
/// Only [`LiteralValue::Scalar`] is handled in the PR-2.1 foundation; [`LiteralValue::Dyn`],
/// [`LiteralValue::Series`], and [`LiteralValue::Range`] fall through to `None`. `Dyn`
/// requires type-inference context (target dtype) which the convertor doesn't have access
/// to without a schema; `Series` and `Range` aren't sensible predicates anyway.
fn convert_literal(lv: &LiteralValue) -> Option<Expression> {
    match lv {
        LiteralValue::Scalar(scalar) => Some(lit(
            polars_vortex::read::predicate::polars_scalar_to_vortex(scalar)?,
        )),
        // Dyn requires materialization against a target dtype; not in foundation scope.
        // Series / Range aren't valid predicate literals.
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests for the PR-2.1 / PR-13.1 foundation shapes + PR-2.2 / PR-13.2 Plus +
    //! the schema-gate refusal paths (And/Or/Not bitwise-on-int + Plus on non-numeric).
    //!
    //! Each test builds a small AExpr tree directly in an `Arena<AExpr>` (no DSL involved),
    //! calls [`aexpr_to_vortex_expression`], and asserts the conversion returns `Some`. For
    //! most shapes the Vortex expression's structure is opaque to the test — we trust the
    //! builder helpers and assert `.is_some()` / `.is_none()` only. The cycle-1 must-fix
    //! escalation around tautological tests is addressed for the load-bearing Plus shape
    //! via `shape_plus_arithmetic_structural`, which inspects `Display::fmt`'s SQL-form
    //! output — a paste-swap bug (`Plus → checked_mul`) would be caught there.
    //! Unsupported shapes additionally assert `None`.
    use polars_core::chunked_array::cast::CastOptions;
    use polars_core::prelude::{AnyValue, DataType};
    use polars_core::scalar::Scalar;
    use polars_utils::arena::Arena;
    use polars_utils::pl_str::PlSmallStr;

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
    fn boolean_fn(arena: &mut Arena<AExpr>, bf: IRBooleanFunction, arg: Node) -> Node {
        // ExprIR::new takes a node + OutputName; for predicate-arena tests we use a dummy
        // empty alias (the OutputName isn't consulted by the convertor).
        let expr_ir = ExprIR::new(arg, OutputName::Alias(PlSmallStr::EMPTY));
        arena.add(AExpr::Function {
            input: vec![expr_ir],
            function: IRFunctionExpr::Boolean(bf),
            options: FunctionOptions::default(),
        })
    }

    // === Shape coverage tests (15 shapes: 14 foundation + Plus) ===

    #[test]
    fn shape_column() {
        let mut arena = Arena::new();
        let n = col(&mut arena, "a");
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_some());
    }

    #[test]
    fn shape_literal_scalar() {
        let mut arena = Arena::new();
        let n = lit_i32(&mut arena, 42);
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_some());
    }

    #[test]
    fn shape_eq() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 42);
        let n = binop(&mut arena, c, Operator::Eq, l);
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_some());
    }

    #[test]
    fn shape_not_eq() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 42);
        let n = binop(&mut arena, c, Operator::NotEq, l);
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_some());
    }

    #[test]
    fn shape_lt() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 42);
        let n = binop(&mut arena, c, Operator::Lt, l);
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_some());
    }

    #[test]
    fn shape_lt_eq() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 42);
        let n = binop(&mut arena, c, Operator::LtEq, l);
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_some());
    }

    #[test]
    fn shape_gt() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 42);
        let n = binop(&mut arena, c, Operator::Gt, l);
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_some());
    }

    #[test]
    fn shape_gt_eq() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 42);
        let n = binop(&mut arena, c, Operator::GtEq, l);
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_some());
    }

    /// Helper: build a small schema with Int32 columns `a` and `b` for And/Or tests.
    /// The And/Or schema gate refuses pushdown when an operand is non-bool, but
    /// comparison-shape operands (eq/lt/etc.) produce Boolean output and pass the gate.
    fn schema_a_b_int32() -> Schema {
        let mut s = Schema::default();
        s.with_column(PlSmallStr::from("a"), DataType::Int32);
        s.with_column(PlSmallStr::from("b"), DataType::Int32);
        s
    }

    /// Helper: schema with `a: Boolean` for the Not test.
    fn schema_a_bool() -> Schema {
        let mut s = Schema::default();
        s.with_column(PlSmallStr::from("a"), DataType::Boolean);
        s
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
        let schema = schema_a_b_int32();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_some());
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
        let schema = schema_a_b_int32();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_some());
    }

    /// And without a schema → conservative refuse (schema gate's None branch).
    #[test]
    fn shape_and_without_schema_returns_none() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 42);
        let eq_node = binop(&mut arena, c, Operator::Eq, l);
        let c2 = col(&mut arena, "b");
        let l2 = lit_i32(&mut arena, 7);
        let lt_node = binop(&mut arena, c2, Operator::Lt, l2);
        let n = binop(&mut arena, eq_node, Operator::And, lt_node);
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_none());
    }

    /// And on integer-bitwise operands → schema gate refuses (PR-2.1 cycle-1 should-fix).
    #[test]
    fn shape_and_bitwise_int_returns_none() {
        // `col_a & col_b` where both are Int32 — Polars `Operator::And` is bitwise here,
        // not logical. The gate must refuse to avoid emitting a Vortex `and(int, int)`.
        let mut arena = Arena::new();
        let a = col(&mut arena, "a");
        let b = col(&mut arena, "b");
        let n = binop(&mut arena, a, Operator::And, b);
        let schema = schema_a_b_int32();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_none());
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
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_some());
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
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_some());
    }

    #[test]
    fn shape_is_null() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let n = boolean_fn(&mut arena, IRBooleanFunction::IsNull, c);
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_some());
    }

    #[test]
    fn shape_is_not_null() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let n = boolean_fn(&mut arena, IRBooleanFunction::IsNotNull, c);
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_some());
    }

    #[test]
    fn shape_not() {
        // Wraps `eq(col_a, 42)` which is a Boolean comparison, so the schema gate passes
        // (the comparison output is Boolean). No schema needed because the inner Eq
        // doesn't fire the gate (only And/Or do).
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 42);
        let eq_node = binop(&mut arena, c, Operator::Eq, l);
        let n = boolean_fn(&mut arena, IRBooleanFunction::Not, eq_node);
        // Not needs a schema to gate; passing schema with no-op type info works because
        // `operand_is_bool` sees the inner BinaryExpr is a comparison → Boolean output.
        let schema = schema_a_b_int32();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_some());
    }

    /// Not without schema → conservative refuse.
    #[test]
    fn shape_not_without_schema_returns_none() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 42);
        let eq_node = binop(&mut arena, c, Operator::Eq, l);
        let n = boolean_fn(&mut arena, IRBooleanFunction::Not, eq_node);
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_none());
    }

    /// Not on an integer column → schema gate refuses (Polars `Not` is bitwise on ints).
    #[test]
    fn shape_not_bitwise_int_returns_none() {
        let mut arena = Arena::new();
        let a = col(&mut arena, "a");
        let n = boolean_fn(&mut arena, IRBooleanFunction::Not, a);
        let schema = schema_a_b_int32();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_none());
    }

    /// Not on a Boolean column → schema gate passes.
    #[test]
    fn shape_not_bool_column_passes() {
        let mut arena = Arena::new();
        let a = col(&mut arena, "a");
        let n = boolean_fn(&mut arena, IRBooleanFunction::Not, a);
        let schema = schema_a_bool();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_some());
    }

    /// Plus arithmetic — PR-2.2 ships this via `vortex::expr::checked_add`. Numeric
    /// operands required (the cycle-1 must-fix gate).
    #[test]
    fn shape_plus_arithmetic() {
        // `(col_a + 1) == 5` — typical PR-13.2 acceptance shape per the plan.
        let mut arena = Arena::new();
        let a = col(&mut arena, "a");
        let one = lit_i32(&mut arena, 1);
        let a_plus_1 = binop(&mut arena, a, Operator::Plus, one);
        let five = lit_i32(&mut arena, 5);
        let n = binop(&mut arena, a_plus_1, Operator::Eq, five);
        // Plus numeric gate fires; supply Int32 schema for `a`.
        let schema = schema_a_b_int32();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_some());
    }

    // === Unsupported-shapes-return-None coverage ===

    /// PR-2.2 ships Plus → checked_add when operands are numeric and schema is provided.
    /// Pre-PR-2.2 this test asserted None; now it must assert Some.
    #[test]
    fn shape_plus_ships_in_pr_2_2() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 1);
        let n = binop(&mut arena, c, Operator::Plus, l);
        let schema = schema_a_b_int32();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_some());
    }

    /// Plus without schema → conservative refuse (Plus gate's None branch).
    #[test]
    fn shape_plus_without_schema_returns_none() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 1);
        let n = binop(&mut arena, c, Operator::Plus, l);
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_none());
    }

    /// Plus on a Boolean column → numeric gate refuses (Vortex `checked_add` would
    /// `vortex_bail!` at scan-time).
    #[test]
    fn shape_plus_bool_column_returns_none() {
        let mut arena = Arena::new();
        let a = col(&mut arena, "a");
        let one = lit_i32(&mut arena, 1);
        let n = binop(&mut arena, a, Operator::Plus, one);
        let schema = schema_a_bool(); // `a` is Boolean here
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_none());
    }

    /// Plus on a String column → numeric gate refuses (Polars allows Plus-as-concat;
    /// Vortex `checked_add` does not).
    #[test]
    fn shape_plus_string_column_returns_none() {
        let mut arena = Arena::new();
        let a = col(&mut arena, "a");
        let b = col(&mut arena, "b");
        let n = binop(&mut arena, a, Operator::Plus, b);
        let mut schema = Schema::default();
        schema.with_column(PlSmallStr::from("a"), DataType::String);
        schema.with_column(PlSmallStr::from("b"), DataType::String);
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_none());
    }

    /// Plus on Float64 columns → numeric gate passes (Float is in the numeric set).
    #[test]
    fn shape_plus_float_column_passes() {
        let mut arena = Arena::new();
        let a = col(&mut arena, "a");
        let b = col(&mut arena, "b");
        let n = binop(&mut arena, a, Operator::Plus, b);
        let mut schema = Schema::default();
        schema.with_column(PlSmallStr::from("a"), DataType::Float64);
        schema.with_column(PlSmallStr::from("b"), DataType::Float64);
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_some());
    }

    /// Nested Plus `(a + b) + c` — `operand_is_numeric` recurses through the inner Plus
    /// (lines 308-312 in vortex_convertor) so all three Int32 columns pass the gate.
    #[test]
    fn shape_plus_nested_numeric_passes() {
        let mut arena = Arena::new();
        let a = col(&mut arena, "a");
        let b = col(&mut arena, "b");
        let a_plus_b = binop(&mut arena, a, Operator::Plus, b);
        let c = col(&mut arena, "c");
        let n = binop(&mut arena, a_plus_b, Operator::Plus, c);
        let mut schema = Schema::default();
        schema.with_column(PlSmallStr::from("a"), DataType::Int32);
        schema.with_column(PlSmallStr::from("b"), DataType::Int32);
        schema.with_column(PlSmallStr::from("c"), DataType::Int32);
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_some());
    }

    /// Plus with a non-Plus BinaryExpr operand `(a * b) + c` — the inner Multiply
    /// is not yet supported by the convertor (returns None at the outer level via the
    /// Plus arm's `?`-propagation on `lhs`), but the gate ALSO refuses because
    /// `operand_is_numeric` only recurses on the inner `Plus` arm (lines 308-312); any
    /// other BinaryExpr op returns false. Both layers refuse: the gate is the first
    /// line of defense, the unsupported Multiply arm is the second.
    #[test]
    fn shape_plus_with_multiply_operand_returns_none() {
        let mut arena = Arena::new();
        let a = col(&mut arena, "a");
        let b = col(&mut arena, "b");
        let a_times_b = binop(&mut arena, a, Operator::Multiply, b);
        let c = col(&mut arena, "c");
        let n = binop(&mut arena, a_times_b, Operator::Plus, c);
        let mut schema = Schema::default();
        schema.with_column(PlSmallStr::from("a"), DataType::Int32);
        schema.with_column(PlSmallStr::from("b"), DataType::Int32);
        schema.with_column(PlSmallStr::from("c"), DataType::Int32);
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_none());
    }

    /// Structural assertion (cycle-1 must-fix from gauntlet — addresses the
    /// tautological-test concern carried forward from PR-2.1 cycle-1, for this PR's most
    /// load-bearing new shape). Verifies the Plus → `checked_add` mapping actually
    /// produces the expected Vortex `Expression` shape, not just `.is_some()`. A
    /// paste-swap bug (e.g., `Operator::Plus => checked_mul(...)`) would be caught here.
    #[test]
    fn shape_plus_arithmetic_structural() {
        let mut arena = Arena::new();
        let a = col(&mut arena, "a");
        let one = lit_i32(&mut arena, 1);
        let a_plus_1 = binop(&mut arena, a, Operator::Plus, one);
        let five = lit_i32(&mut arena, 5);
        let n = binop(&mut arena, a_plus_1, Operator::Eq, five);
        let schema = schema_a_b_int32();
        let expr = aexpr_to_vortex_expression(n, &arena, Some(&schema)).expect("Some");
        // Vortex's SQL-form Display produces a stable string. The exact format may
        // evolve across Vortex releases; assert only the recognizable structural
        // anchors (operator names + literal values + the column reference) rather
        // than the full string.
        let s = format!("{}", expr);
        assert!(
            s.contains("checked_add") || s.contains("+"),
            "expected checked_add or '+' in {s}"
        );
        assert!(s.contains("a"), "expected column 'a' in {s}");
        assert!(s.contains("1"), "expected literal 1 in {s}");
        assert!(s.contains("5"), "expected literal 5 in {s}");
        // Sanity: the outer-Eq structure should be visible.
        assert!(
            s.contains("=") || s.contains("eq"),
            "expected eq operator in {s}"
        );
    }

    /// Minus / Multiply / Divide remain residual until upstream Vortex exposes
    /// `checked_sub`/`checked_mul`/`checked_div` as public builders. (As of vortex
    /// 0.70.0 only `checked_add` is exposed.)
    #[test]
    fn unsupported_minus_returns_none() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 1);
        let n = binop(&mut arena, c, Operator::Minus, l);
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_none());
    }

    #[test]
    fn unsupported_multiply_returns_none() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 2);
        let n = binop(&mut arena, c, Operator::Multiply, l);
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_none());
    }

    #[test]
    fn unsupported_xor_returns_none() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 1);
        let n = binop(&mut arena, c, Operator::Xor, l);
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_none());
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
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_none());
    }

    #[test]
    fn unsupported_not_eq_validity_returns_none() {
        // NotEqValidity — paired with EqValidity. (cycle-1 fresh-lens F-002.)
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 1);
        let n = binop(&mut arena, c, Operator::NotEqValidity, l);
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_none());
    }

    // === PR-2.3 / PR-13.3: CAST in predicates ===

    /// CAST to a supported primitive (Int64) ships in PR-2.3 — was pre-PR-2.3 None,
    /// now Some.
    #[test]
    fn shape_cast_to_int64_ships_in_pr_2_3() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let n = arena.add(AExpr::Cast {
            expr: c,
            dtype: DataType::Int64,
            options: CastOptions::Strict,
        });
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_some());
    }

    /// CAST to Float64 — supported.
    #[test]
    fn shape_cast_to_float64() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let n = arena.add(AExpr::Cast {
            expr: c,
            dtype: DataType::Float64,
            options: CastOptions::Strict,
        });
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_some());
    }

    /// CAST to Boolean — supported.
    #[test]
    fn shape_cast_to_bool() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let n = arena.add(AExpr::Cast {
            expr: c,
            dtype: DataType::Boolean,
            options: CastOptions::Strict,
        });
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_some());
    }

    /// CAST to String — supported.
    #[test]
    fn shape_cast_to_string() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let n = arena.add(AExpr::Cast {
            expr: c,
            dtype: DataType::String,
            options: CastOptions::Strict,
        });
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_some());
    }

    /// CAST to Decimal — refused (Vortex Decimal scale/precision interactions are NOT
    /// validated at the polars-vortex layer; tracked in the function doc as deferred).
    #[cfg(feature = "dtype-decimal")]
    #[test]
    fn shape_cast_to_decimal_returns_none() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let n = arena.add(AExpr::Cast {
            expr: c,
            dtype: DataType::Decimal(10, 2),
            options: CastOptions::Strict,
        });
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_none());
    }

    /// CAST nested in a comparison — `col.cast(Int64) > 100` per the plan's PR-13.3
    /// acceptance test.
    #[test]
    fn shape_cast_then_compare() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let cast_node = arena.add(AExpr::Cast {
            expr: c,
            dtype: DataType::Int64,
            options: CastOptions::Strict,
        });
        let l = lit_i32(&mut arena, 100);
        let n = binop(&mut arena, cast_node, Operator::Gt, l);
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_some());
    }

    #[test]
    fn unsupported_literal_dyn_returns_none() {
        // Dyn literals need a target dtype for materialization; foundation falls through.
        let mut arena = Arena::new();
        let n = arena.add(AExpr::Literal(LiteralValue::Dyn(DynLiteralValue::Int(42))));
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_none());
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

        // Both Or and And fire the schema gate; provide schema with a/b/c Int32.
        let mut schema = Schema::default();
        schema.with_column(PlSmallStr::from("a"), DataType::Int32);
        schema.with_column(PlSmallStr::from("b"), DataType::Int32);
        schema.with_column(PlSmallStr::from("c"), DataType::Int32);
        assert!(aexpr_to_vortex_expression(root_node, &arena, Some(&schema)).is_some());
    }

    /// Sanity: a single still-unsupported sub-shape poisons the whole tree.
    /// (Updated for PR-2.2: Plus is now supported, so use Minus to exercise the
    /// poison path.)
    #[test]
    fn integration_unsupported_subexpr_returns_none() {
        let mut arena = Arena::new();
        let a = col(&mut arena, "a");
        let one = lit_i32(&mut arena, 1);
        let a_minus_1 = binop(&mut arena, a, Operator::Minus, one);
        let five = lit_i32(&mut arena, 5);
        let n = binop(&mut arena, a_minus_1, Operator::Eq, five);
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_none());
    }
}
