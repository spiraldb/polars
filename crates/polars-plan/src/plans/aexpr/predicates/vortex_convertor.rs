//! AExpr-direct convertor for Polars predicates → Vortex `Expression` (PR-13 path).
//!
//! Translates Polars [`AExpr`] trees into Vortex [`Expression`] trees for filter pushdown,
//! walking the `Arena<AExpr>` directly. As of PR-2.6 (Option B → A cutover) this is the
//! SOLE filter-pushdown path for Vortex scans; the legacy
//! `polars_vortex::read::predicate::polars_to_vortex_predicate` (which consumed
//! pre-extracted [`polars_io::predicates::SpecializedColumnPredicate`] shapes) was
//! deleted in PR-2.6. The AExpr-direct path handles everything the legacy path handled
//! plus shapes the optimizer doesn't pre-extract — multi-column comparisons, arithmetic
//! in predicates, CAST in predicates, struct field access in predicates. Temporal
//! extracts remain residual until upstream Vortex exposes the relevant builders (see
//! plan Deferred work).
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
//! ## What this module covers (PR-2.1 foundation + PR-2.2 + PR-2.3 + PR-2.4 extensions)
//!
//! The 16 shapes below — the "kernel" the rest of PR-13 extends. (13 from PR-2.1's
//! foundation + 1 from PR-2.2: `addition (numeric)` + 1 from PR-2.3: `cast (same-kind
//! Primitive/Bool/Utf8 target)` + 1 from PR-2.4: `struct field access`.)
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
//! | cast (same-kind) | `AExpr::Cast { dtype, options: Strict, .. }` (PR-2.3) | `cast` (kind-gated: Primitive↔Primitive, Bool↔Bool, Utf8↔Utf8) |
//! | struct field access | `AExpr::Function { StructExpr(FieldByName(n)), .. }` (PR-2.4) | `get_item(n, inner)` (schema-gated to require the field) |
//! | `is_null` | `AExpr::Function { Boolean(IsNull), .. }` | `is_null` |
//! | `is_not_null` | `AExpr::Function { Boolean(IsNotNull), .. }` | `is_not_null` |
//! | `not` | `AExpr::Function { Boolean(Not), .. }` | `not` (schema-gated) |
//!
//! ## What this module does NOT cover yet (PR-2.5 follow-ups)
//!
//! Remaining arithmetic (`Minus`/`Multiply`/divides/`Modulus`) → still residual; PR-2.2
//! ships `Plus` only because `checked_add` is the only arithmetic builder publicly exposed
//! in `vortex::expr::*` at the pinned SHA. Cross-kind CAST (Primitive↔Bool/Utf8) and
//! `NonStrict`/`Overflowing` CAST options → still residual (PR-2.3 cycle-1 must-fix gates;
//! Vortex's per-array `CastKernel` is strictly within-kind and fail-on-overflow). Other
//! struct functions (`RenameFields`/`PrefixFields`/etc.) → not in predicate scope. Temporal
//! extracts (`AExpr::Function { IRFunctionExpr::TemporalExpr(..), .. }`) → PR-2.5. Anything
//! else (`Sort`, `Gather`, `Filter`, `Agg`, `Ternary`, `AnonymousFunction`, `Over`,
//! `Rolling`, etc.) returns `None` and falls through as residual; the multi-scan layer
//! re-applies the full predicate post-decode so dropping coverage is always SOUND, just
//! suboptimal.
//!
//! ## Wiring
//!
//! Wired inside the `FileScanIR::Vortex` match arm of
//! `crates/polars-stream/src/physical_plan/lower_ir.rs::lower_ir` (search for
//! `FileScanIR::Vortex` — the line range shifts across cleanup PRs), where the [`AExpr`]
//! arena is live alongside the predicate `ExprIR`. The resulting `Expression` is attached
//! to the Vortex `VortexReaderBuilder.aexpr_filter` field via a Vortex-specific side
//! channel (parallel to how `FileScanIR::Vortex::metadata` and the (PR-2.0) `segment_cache`
//! thread). `VortexFileReader::begin_read` uses `aexpr_filter` directly. PR-2.6 deleted
//! the legacy `polars_to_vortex_predicate` (`SpecializedColumnPredicate`-derived) path;
//! the AExpr-direct convertor is now the sole filter-pushdown path.

use polars_core::chunked_array::cast::CastOptions;
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
#[cfg(feature = "dtype-struct")]
use crate::plans::aexpr::function_expr::IRStructFunction;
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
/// - `schema` — the resolved [`Schema`] of the columns the AExpr references. Used by
///   three gates: (a) the And/Or/Not bitwise-vs-logical gate (refuses pushdown on
///   integer operands); (b) the Plus arm's numeric gate (refuses pushdown on non-numeric
///   operands); (c) the CAST arm's source-kind gate (refuses cross-kind casts that
///   Vortex's per-array `CastKernel` rejects). When `None`, the convertor conservatively
///   refuses And/Or/Not AND Plus AND CAST pushdown. Production wire-up
///   (`physical_plan::lower_ir`) always supplies `Some`; `None` is exposed only to keep
///   ad-hoc unit-test construction ergonomic.
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
///
/// # CAST semantic caveat (PR-2.3 / PR-13.3)
///
/// Polars `AExpr::Cast` is mapped to Vortex's `cast` builder under **two gates** to
/// preserve the always-SAFE-fallback contract:
///
/// 1. **`CastOptions::Strict` only.** Polars `NonStrict` (overflow→null) and
///    `Overflowing` (wrap) diverge from Vortex's `Primitive::CastKernel` fail-on-overflow
///    semantics (`vortex-array/src/arrays/primitive/compute/cast.rs:85-91`
///    `vortex_bail!`s on values exceeding target range). Pushing non-Strict down would
///    convert Polars' silent-or-null behavior into a hard scan-time `ComputeError`.
///    Refuse for any non-Strict option.
///
/// 2. **Same-kind source/target only**, via `cast_kind_compatible`. Vortex's per-array
///    `CastKernel` impls are strictly within-kind: `Primitive::cast` returns `Ok(None)`
///    for non-Primitive targets (`primitive/compute/cast.rs:62-64`); `Bool::cast` for
///    non-Bool targets (`bool/compute/cast.rs:41-43`); `VarBinView::cast` for
///    non-Utf8/Binary (`varbinview/compute/cast.rs:60-62`). Cross-kind casts cause
///    `cast/mod.rs:120` to `vortex_bail!("No CastKernel ...")` at scan-time. Refuse
///    when source and target are in different kinds (Primitive ↔ Primitive, Bool ↔ Bool,
///    Utf8 ↔ Utf8 only).
///
/// Together: cross-kind CAST (Int → String, Bool → Int, etc.) and non-Strict CAST fall
/// through to residual via `?`-propagation. Within-kind Strict CAST near boundary values
/// (e.g., `cast(Int64, Int8)` on overflow) WILL still scan-time-error — consistent with
/// Polars Strict semantics.
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
                let s = schema?;
                if !operand_is_bool(*left, arena, s) || !operand_is_bool(*right, arena, s) {
                    return None;
                }
            }
            // Plus numeric + pairwise-equal-PType gate (PR-2.2 cycle-1 must-fix + PR-2.4
            // proactive fix for the cycle-2-surfaced sibling bug class):
            //
            // Vortex's `checked_add` requires `lhs.is_primitive() && lhs.eq_ignore_nullability(rhs)`
            // (`vortex-array/src/scalar_fn/fns/binary/mod.rs:115-127` — `return_dtype`
            // `vortex_bail!`s with "incompatible types for arithmetic operation" otherwise).
            // Polars allows Plus on String (concat), Bool, Date+Duration, etc. — emitting
            // `checked_add` on those (or on cross-PType operands like Int8+Int64) would
            // bail at scan-time, violating the always-SAFE-fallback contract.
            //
            // In typical Polars usage, the TYPE_COERCION optimizer rule inserts a Cast
            // BEFORE the Plus to align operand dtypes — so the Cast arm fires first and
            // the outer Plus sees same-PType operands. When TYPE_COERCION is disabled (or
            // an AExpr bypasses the optimizer), we still need to refuse pushdown. Two
            // gates: (a) both operands are numeric (per `is_vortex_numeric_dtype`),
            // (b) both operands resolve to the SAME numeric DataType. `resolve_inner_dtype`
            // handles Column / Literal / Cast / comparisons; unresolvable shapes fall
            // through to None → conservative refuse.
            if matches!(op, Operator::Plus) {
                let s = schema?;
                if !operand_is_numeric(*left, arena, s) || !operand_is_numeric(*right, arena, s) {
                    return None;
                }
                // Pairwise-equal-PType gate (PR-2.4 proactive fix; addresses PR-2.3
                // cycle-2 H4 self-reinforcement finding).
                let lhs_dt = resolve_inner_dtype(*left, arena, s)?;
                let rhs_dt = resolve_inner_dtype(*right, arena, s)?;
                if lhs_dt != rhs_dt {
                    return None;
                }
            }
            // Comparison pairwise-equal-PType gate (PR-2.4 cycle-2 should-fix
            // F-COMPARE-CROSS-PTYPE-001 — same bug class as the Plus gate above;
            // surfaced by the cycle-1 fresh reviewer applying H4 to find the sibling).
            //
            // Vortex's `Binary::return_dtype` (`vortex-array/src/scalar_fn/fns/binary/mod.rs:130-136`)
            // `vortex_bail!`s with "Cannot compare different DTypes" for comparison ops
            // when `!lhs.eq_ignore_nullability(rhs) && !lhs.is_extension() &&
            // !rhs.is_extension()`. Extension types (Date, Datetime, Time, Duration) are
            // exempt — Vortex permits Date<->Datetime comparison and similar. For
            // non-extension cross-PType operands (Int32 vs Int64, etc.) we refuse
            // pushdown to preserve the always-SAFE-fallback contract. TYPE_COERCION
            // normally inserts a Cast that aligns dtypes; this gate covers the
            // type_coercion-off path. Note: extension-type Polars dtypes aren't in
            // `is_vortex_numeric_dtype` and don't currently route through this gate's
            // dtype-resolution shapes anyway, so we don't need to special-case extension
            // here.
            if matches!(
                op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
            ) {
                let s = schema?;
                let lhs_dt = resolve_inner_dtype(*left, arena, s)?;
                let rhs_dt = resolve_inner_dtype(*right, arena, s)?;
                if lhs_dt != rhs_dt {
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
                let s = schema?;
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

        // --- Struct field access (PR-2.4 / PR-13.4) ---
        // `col.struct.field("inner") == "x"` against a struct column pushes down as
        // `eq(get_item("inner", get_item("col", root())), lit("x"))`. The Polars AExpr
        // shape is `Function { StructExpr(FieldByName(name)), input: [struct_col_expr] }`;
        // mirrors vortex-duckdb's `TableFilterClass::StructExtract` precedent at
        // `vortex-duckdb/src/convert/table_filter.rs:71-73`.
        //
        // Schema gate (cycle-2 process lesson from PR-2.3): Vortex's `GetItem.return_dtype`
        // (`vortex-array/src/scalar_fn/fns/get_item.rs:94-96`) `vortex_err!`s at scan-time
        // if the requested field name isn't in the struct's fields — same hostile-input
        // class as the cycle-1 CAST cross-kind bug. Refuse pushdown when the schema is
        // unavailable OR when the resolved inner dtype isn't a `Struct(fields)` containing
        // the requested field.
        #[cfg(feature = "dtype-struct")]
        AExpr::Function {
            input,
            function: IRFunctionExpr::StructExpr(IRStructFunction::FieldByName(name)),
            ..
        } => {
            let arg_node = input.first().map(|expr_ir| expr_ir.node())?;
            // Schema-membership gate.
            let s = schema?;
            let inner_dtype = resolve_inner_dtype(arg_node, arena, s)?;
            if !struct_field_exists(&inner_dtype, name) {
                return None;
            }
            let inner = aexpr_to_vortex_expression(arg_node, arena, schema)?;
            Some(get_item(name.as_str(), inner))
        },

        // --- CAST (PR-2.3 / PR-13.3) ---
        // `col.cast(Int64) > 100` against an Int32 column pushes down as
        // `gt(cast(get_item("col", root()), DType::Primitive(I64, Nullable)), lit(100i64))`.
        //
        // Two gates protect the convertor's always-SAFE-fallback contract (PR-2.3
        // cycle-1 must-fix from gauntlet; same bug class as PR-2.2 cycle-1 M2 Plus):
        //
        // 1. `CastOptions::Strict` only. Polars `NonStrict` (overflow→null) and
        //    `Overflowing` (wrap) silently diverge from Vortex's fail-on-overflow
        //    `checked_add`-style cast semantics — pushing those down would convert
        //    Polars's silent-or-null behavior into a scan-time `ComputeError`.
        //    The user's query semantics must dominate; refuse pushdown so the
        //    legacy path / post-decode reapply handles non-Strict.
        // 2. Source-dtype-kind compatibility check via `cast_kind_compatible`.
        //    Vortex's per-array `CastKernel` impls (verified in
        //    `vortex-array/src/arrays/{primitive,bool,varbinview}/compute/cast.rs`)
        //    are **strictly within-kind**: Primitive↔Primitive only, Bool↔Bool
        //    only, Utf8↔Utf8 only (also Binary↔Binary). Cross-kind casts return
        //    `Ok(None)` from the kernel, which `cast/mod.rs:120` then
        //    `vortex_bail!`s on with "No CastKernel". The convertor refuses
        //    cross-kind so the legacy path / post-decode reapply handles them.
        //
        // The `?`-propagation on both gates yields None for any unsupported
        // shape — always SAFE.
        AExpr::Cast {
            expr: inner,
            dtype: target_pl,
            options,
        } => {
            if !options.is_strict() {
                return None;
            }
            let target = polars_dtype_to_vortex_dtype(target_pl)?;
            // Source-dtype-kind gate. Requires schema; without schema we cannot
            // resolve the inner expression's dtype, so conservatively refuse.
            let s = schema?;
            let source_pl = resolve_inner_dtype(*inner, arena, s)?;
            if !cast_kind_compatible(&source_pl, target_pl) {
                return None;
            }
            let child = aexpr_to_vortex_expression(*inner, arena, schema)?;
            Some(cast(child, target))
        },

        // --- unsupported shapes (residual) ---
        // Other Function variants (temporal etc.) → PR-2.5. Sort/Gather/Filter/Agg/
        // Ternary/AnonymousFunction/Over/Rolling etc. all fall through to residual
        // unconditionally.
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
        // All other dtypes fall through to None via the catch-all. Notably:
        // - Decimal: deliberately NOT given an explicit arm even with the `dtype-decimal`
        //   feature enabled. Vortex `DType::Decimal(DecimalDType(precision, scale), nullable)`
        //   requires usize ↔ u8 narrow + validation per the project BAN against `as` casts
        //   on Vortex Decimal precision/scale; deferred until polars-vortex validates
        //   `vortex::expr::cast` interactions for Decimal scale/precision.
        // - Int128/UInt128: NOT in the numeric set because Vortex's `PType` ceiling is
        //   I64/U64/F64 and `polars_scalar_to_vortex` has no Int128/UInt128 literal arms
        //   (mirrors `is_vortex_numeric_dtype`'s exclusion for the same reason).
        // - Object / Categorical / Enum / Date / Datetime / Time / Duration / Binary /
        //   List / Struct / Array / Null / Unknown: not in PR-2.3 scope; PR-2.4/.5 may
        //   add some (e.g., struct field access in PR-2.4).
        _ => return None,
    })
}

/// Resolve the Polars [`DataType`] of `node` for CAST source-dtype gating.
///
/// Returns `None` for AExpr shapes we cannot trivially type-check (nested CAST
/// chains, expressions producing dtype-dependent output, AnonymousFunction, etc.) —
/// the CAST arm's `?`-propagation then drops the cast to residual, which is always
/// SAFE.
///
/// Supports the shapes the convertor's own CAST arm cares about: `Column`,
/// `Literal(Scalar)`, and `Cast` (recursive — the cast's output dtype is its target).
/// Comparisons (Eq/Lt/etc.) and Boolean-function outputs (IsNull/IsNotNull/Not)
/// produce Boolean output. Plus produces output matching its operands' numeric type
/// (delegate to the recursive operand_is_numeric machinery).
fn resolve_inner_dtype(node: Node, arena: &Arena<AExpr>, schema: &Schema) -> Option<DataType> {
    match arena.get(node) {
        AExpr::Column(name) => schema.get(name).cloned(),
        AExpr::Literal(LiteralValue::Scalar(s)) => Some(s.dtype().clone()),
        AExpr::Cast {
            dtype: target_pl, ..
        } => Some(target_pl.clone()),
        AExpr::BinaryExpr { left, op, right } => match op {
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::EqValidity
            | Operator::NotEqValidity
            | Operator::LogicalAnd
            | Operator::LogicalOr => Some(DataType::Boolean),
            // Plus output dtype matches the operand dtypes. Self-contained verification
            // (PR-2.4 cycle-2 should-fix F-RESOLVE-PLUS-LHS-DELEGATION-001): don't rely
            // on the convertor's Plus arm gate having fired — verify lhs == rhs here so
            // any future caller of `resolve_inner_dtype` gets a trustworthy answer.
            // Recursive: nested Plus chains `(a + b) + c` resolve correctly.
            Operator::Plus => {
                let l = resolve_inner_dtype(*left, arena, schema)?;
                let r = resolve_inner_dtype(*right, arena, schema)?;
                if l == r { Some(l) } else { None }
            },
            _ => None,
        },
        AExpr::Function {
            function:
                IRFunctionExpr::Boolean(
                    IRBooleanFunction::IsNull
                    | IRBooleanFunction::IsNotNull
                    | IRBooleanFunction::Not,
                ),
            ..
        } => Some(DataType::Boolean),
        // Struct field access — resolve to the inner struct's field dtype.
        // Used by the CAST source-kind gate when a Cast wraps a struct field access,
        // and by the StructExpr arm's recursive gate to chain through nested structs.
        #[cfg(feature = "dtype-struct")]
        AExpr::Function {
            input,
            function: IRFunctionExpr::StructExpr(IRStructFunction::FieldByName(name)),
            ..
        } => {
            let arg_node = input.first().map(|expr_ir| expr_ir.node())?;
            let inner = resolve_inner_dtype(arg_node, arena, schema)?;
            if let DataType::Struct(fields) = &inner {
                fields
                    .iter()
                    .find(|f| f.name() == name)
                    .map(|f| f.dtype().clone())
            } else {
                None
            }
        },
        _ => None,
    }
}

/// Does `dtype` contain a struct field named `name`? Used by the PR-2.4 StructField gate.
///
/// Returns `false` for non-Struct dtypes. Conservatively returns `false` if the dtype
/// isn't a Struct so the caller's None-fallback drops the StructField arm to residual.
#[cfg(feature = "dtype-struct")]
fn struct_field_exists(dtype: &DataType, name: &polars_utils::pl_str::PlSmallStr) -> bool {
    if let DataType::Struct(fields) = dtype {
        fields.iter().any(|f| f.name() == name)
    } else {
        false
    }
}

/// Is the (source, target) CAST pair representable by Vortex's per-array
/// `CastKernel` impls? Verified against vortex-array 0.70.0:
///
/// - `Primitive::cast` returns `Ok(None)` for non-Primitive targets
///   (`arrays/primitive/compute/cast.rs:62-64`)
/// - `Bool::cast` returns `Ok(None)` for non-Bool targets
///   (`arrays/bool/compute/cast.rs:41-43`)
/// - `VarBinView::cast` returns `Ok(None)` unless source AND target are both Utf8
///   (or both Binary) (`arrays/varbinview/compute/cast.rs:60-62`)
///
/// Cross-kind casts (Primitive→Bool, Bool→Utf8, Utf8→Primitive, etc.) cause
/// `cast/mod.rs:120` to `vortex_bail!("No CastKernel to cast canonical array {} from
/// {} to {}")` at scan-time, which propagates as a hard `ComputeError`. The
/// convertor refuses cross-kind so the residual / legacy path handles them.
///
/// Same-kind narrowing (Int32→Int8, Int64→Int32) is allowed; Vortex's
/// `values_fit_in` check at `cast.rs:85-91` produces a scan-time `vortex_bail!` on
/// out-of-range values. This is acceptable under `CastOptions::Strict` semantics
/// (which is the only mode we push down — the cycle-1 gate above).
fn cast_kind_compatible(source: &DataType, target: &DataType) -> bool {
    use DataType::*;
    matches!((source, target), (Boolean, Boolean) | (String, String))
        || (is_vortex_numeric_dtype(source) && is_vortex_numeric_dtype(target))
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
        // CAST to a numeric target produces numeric output. This lets
        // `Cast(int32_col, Int64) + lit_i64` push down (the inner Cast aligns the dtype
        // for Vortex's same-PType `checked_add` requirement).
        AExpr::Cast { dtype, .. } => is_vortex_numeric_dtype(dtype),
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

    // === Shape coverage tests (15 shapes: 13 foundation + Plus + Cast) ===

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

    // The comparison tests below pass `Some(&schema_a_b_int32())` because the
    // PR-2.4 cycle-2 should-fix F-COMPARE-CROSS-PTYPE-001 added a comparison
    // pairwise-equal-PType gate: comparisons require schema to verify operand
    // dtypes match (mirroring the Plus gate's discipline). Without schema, the
    // gate conservatively refuses — see `shape_eq_without_schema_returns_none`.

    #[test]
    fn shape_eq() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 42);
        let n = binop(&mut arena, c, Operator::Eq, l);
        let schema = schema_a_b_int32();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_some());
    }

    #[test]
    fn shape_not_eq() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 42);
        let n = binop(&mut arena, c, Operator::NotEq, l);
        let schema = schema_a_b_int32();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_some());
    }

    #[test]
    fn shape_lt() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 42);
        let n = binop(&mut arena, c, Operator::Lt, l);
        let schema = schema_a_b_int32();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_some());
    }

    #[test]
    fn shape_lt_eq() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 42);
        let n = binop(&mut arena, c, Operator::LtEq, l);
        let schema = schema_a_b_int32();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_some());
    }

    #[test]
    fn shape_gt() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 42);
        let n = binop(&mut arena, c, Operator::Gt, l);
        let schema = schema_a_b_int32();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_some());
    }

    #[test]
    fn shape_gt_eq() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 42);
        let n = binop(&mut arena, c, Operator::GtEq, l);
        let schema = schema_a_b_int32();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_some());
    }

    /// Comparison without schema → conservative refuse (PR-2.4 cycle-2 comparison
    /// pairwise-equal-PType gate from F-COMPARE-CROSS-PTYPE-001).
    #[test]
    fn shape_eq_without_schema_returns_none() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 42);
        let n = binop(&mut arena, c, Operator::Eq, l);
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_none());
    }

    /// Comparison on cross-PType operands (`Int32 == Int64`) — refused by the
    /// pairwise-equal-PType gate. Without the gate, Vortex's `Binary::return_dtype`
    /// would `vortex_bail!` at scan-time. PR-2.4 cycle-2 F-COMPARE-CROSS-PTYPE-001.
    #[test]
    fn shape_eq_cross_ptype_returns_none() {
        use polars_core::prelude::AnyValue;
        let mut arena = Arena::new();
        let c = col(&mut arena, "a"); // Int32
        let l = arena.add(AExpr::Literal(LiteralValue::Scalar(Scalar::new(
            DataType::Int64,
            AnyValue::Int64(42),
        ))));
        let n = binop(&mut arena, c, Operator::Eq, l);
        let schema = schema_a_b_int32();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_none());
    }

    /// Comparison on cross-PType (`Int32 < Float64`) — refused by the gate.
    #[test]
    fn shape_lt_cross_ptype_returns_none() {
        use polars_core::prelude::AnyValue;
        let mut arena = Arena::new();
        let c = col(&mut arena, "a"); // Int32
        let l = arena.add(AExpr::Literal(LiteralValue::Scalar(Scalar::new(
            DataType::Float64,
            AnyValue::Float64(1.0),
        ))));
        let n = binop(&mut arena, c, Operator::Lt, l);
        let schema = schema_a_b_int32();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_none());
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
        // Inner Eq/Lt comparisons fire the PR-2.4 cycle-2 comparison gate, so schema
        // is required.
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let l = lit_i32(&mut arena, 42);
        let left = binop(&mut arena, c, Operator::Eq, l);
        let c2 = col(&mut arena, "b");
        let l2 = lit_i32(&mut arena, 7);
        let right = binop(&mut arena, c2, Operator::Lt, l2);
        let n = binop(&mut arena, left, Operator::LogicalAnd, right);
        let schema = schema_a_b_int32();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_some());
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
        let schema = schema_a_b_int32();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_some());
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
    /// (the `Operator::Plus` arm of `operand_is_numeric`) so all three Int32 columns pass
    /// the gate.
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
    /// `operand_is_numeric` only recurses on the inner `Plus` arm; any other BinaryExpr
    /// op returns false. Both layers refuse: the gate is the first line of defense, the
    /// unsupported Multiply arm is the second.
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

    /// Plus on cross-PType operands `Int32 + Int64` — refused by the PR-2.4 proactive
    /// pairwise-equal-PType gate (addresses PR-2.3 cycle-2 H4 self-reinforcement
    /// finding). Without the gate, Vortex's `Binary::return_dtype` would `vortex_bail!`
    /// at scan-time because `Int32.eq_ignore_nullability(Int64) == false`.
    #[test]
    fn shape_plus_cross_ptype_returns_none() {
        let mut arena = Arena::new();
        let a = col(&mut arena, "a"); // Int32 per schema_a_b_int32
        // Build an Int64 literal (PType differs from the Int32 column).
        use polars_core::prelude::AnyValue;
        let one_i64 = arena.add(AExpr::Literal(LiteralValue::Scalar(Scalar::new(
            DataType::Int64,
            AnyValue::Int64(1),
        ))));
        let n = binop(&mut arena, a, Operator::Plus, one_i64);
        let schema = schema_a_b_int32();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_none());
    }

    /// Plus on Int + Float — both numeric, different PType. Refused by pairwise gate.
    /// (PR-2.4 cycle-2 should-fix F-PLUS-CROSS-FLOAT-INT-TEST-001.)
    #[test]
    fn shape_plus_int_plus_float_returns_none() {
        let mut arena = Arena::new();
        let a = col(&mut arena, "a"); // Int32
        use polars_core::prelude::AnyValue;
        let one_f64 = arena.add(AExpr::Literal(LiteralValue::Scalar(Scalar::new(
            DataType::Float64,
            AnyValue::Float64(1.0),
        ))));
        let n = binop(&mut arena, a, Operator::Plus, one_f64);
        let schema = schema_a_b_int32();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_none());
    }

    /// Plus on UInt + Int — both numeric, different signedness. Refused by pairwise gate.
    /// (PR-2.4 cycle-2 nit N2.)
    #[test]
    fn shape_plus_uint_plus_int_returns_none() {
        let mut arena = Arena::new();
        let a = col(&mut arena, "a"); // UInt32 per this test's schema
        use polars_core::prelude::AnyValue;
        let one_i32 = lit_i32(&mut arena, 1);
        let n = binop(&mut arena, a, Operator::Plus, one_i32);
        let mut schema = Schema::default();
        schema.with_column(PlSmallStr::from("a"), DataType::UInt32);
        let _ = AnyValue::UInt32(1); // explicit construction of the literal type checked elsewhere
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_none());
    }

    /// Plus on same-PType operands wrapped through a CAST `Cast(col_int32, Int64) + lit_i64`
    /// — passes the pairwise gate (resolve_inner_dtype follows the Cast). Verifies the
    /// gate doesn't over-refuse when TYPE_COERCION did its job.
    #[test]
    fn shape_plus_cast_then_same_ptype_passes() {
        let mut arena = Arena::new();
        let a = col(&mut arena, "a"); // Int32 per schema_a_b_int32
        let cast_a = arena.add(AExpr::Cast {
            expr: a,
            dtype: DataType::Int64,
            options: CastOptions::Strict,
        });
        use polars_core::prelude::AnyValue;
        let one_i64 = arena.add(AExpr::Literal(LiteralValue::Scalar(Scalar::new(
            DataType::Int64,
            AnyValue::Int64(1),
        ))));
        let n = binop(&mut arena, cast_a, Operator::Plus, one_i64);
        let schema = schema_a_b_int32();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_some());
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

    /// Helper: schema with String columns `a` and `b` for cross-kind CAST tests.
    fn schema_a_b_string() -> Schema {
        let mut s = Schema::default();
        s.with_column(PlSmallStr::from("a"), DataType::String);
        s.with_column(PlSmallStr::from("b"), DataType::String);
        s
    }

    /// CAST Int32 → Int64 (same kind: Primitive → Primitive) ships in PR-2.3 —
    /// was pre-PR-2.3 None, now Some with schema.
    #[test]
    fn shape_cast_to_int64_ships_in_pr_2_3() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let n = arena.add(AExpr::Cast {
            expr: c,
            dtype: DataType::Int64,
            options: CastOptions::Strict,
        });
        let schema = schema_a_b_int32();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_some());
    }

    /// CAST Int32 → Float64 (same kind: Primitive → Primitive) — supported.
    #[test]
    fn shape_cast_to_float64() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let n = arena.add(AExpr::Cast {
            expr: c,
            dtype: DataType::Float64,
            options: CastOptions::Strict,
        });
        let schema = schema_a_b_int32();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_some());
    }

    /// CAST Boolean → Boolean (degenerate same-kind: validity widening) — supported.
    #[test]
    fn shape_cast_bool_to_bool() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let n = arena.add(AExpr::Cast {
            expr: c,
            dtype: DataType::Boolean,
            options: CastOptions::Strict,
        });
        let schema = schema_a_bool();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_some());
    }

    /// CAST String → String (degenerate same-kind: validity widening) — supported.
    #[test]
    fn shape_cast_string_to_string() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let n = arena.add(AExpr::Cast {
            expr: c,
            dtype: DataType::String,
            options: CastOptions::Strict,
        });
        let schema = schema_a_b_string();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_some());
    }

    /// CAST Int32 → Boolean (cross-kind: Primitive → Bool) — refused (PR-2.3
    /// cycle-1 must-fix). Vortex's `Primitive::CastKernel` returns `Ok(None)` for
    /// non-Primitive targets and `cast/mod.rs:120` then `vortex_bail!`s.
    #[test]
    fn shape_cast_int_to_bool_returns_none() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let n = arena.add(AExpr::Cast {
            expr: c,
            dtype: DataType::Boolean,
            options: CastOptions::Strict,
        });
        let schema = schema_a_b_int32();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_none());
    }

    /// CAST Int32 → String (cross-kind: Primitive → Utf8) — refused.
    #[test]
    fn shape_cast_int_to_string_returns_none() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let n = arena.add(AExpr::Cast {
            expr: c,
            dtype: DataType::String,
            options: CastOptions::Strict,
        });
        let schema = schema_a_b_int32();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_none());
    }

    /// CAST Bool → Int (cross-kind) — refused.
    #[test]
    fn shape_cast_bool_to_int_returns_none() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let n = arena.add(AExpr::Cast {
            expr: c,
            dtype: DataType::Int64,
            options: CastOptions::Strict,
        });
        let schema = schema_a_bool();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_none());
    }

    /// CAST String → Int (cross-kind) — refused.
    #[test]
    fn shape_cast_string_to_int_returns_none() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let n = arena.add(AExpr::Cast {
            expr: c,
            dtype: DataType::Int64,
            options: CastOptions::Strict,
        });
        let schema = schema_a_b_string();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_none());
    }

    /// CAST Bool → String (cross-kind) — refused (PR-2.3 cycle-2 C2-CAST-001).
    #[test]
    fn shape_cast_bool_to_string_returns_none() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let n = arena.add(AExpr::Cast {
            expr: c,
            dtype: DataType::String,
            options: CastOptions::Strict,
        });
        let schema = schema_a_bool();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_none());
    }

    /// CAST String → Bool (cross-kind) — refused (PR-2.3 cycle-2 C2-CAST-001).
    #[test]
    fn shape_cast_string_to_bool_returns_none() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let n = arena.add(AExpr::Cast {
            expr: c,
            dtype: DataType::Boolean,
            options: CastOptions::Strict,
        });
        let schema = schema_a_b_string();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_none());
    }

    /// CAST without schema → conservative refuse (cycle-1 must-fix: the
    /// source-dtype gate cannot resolve without schema).
    #[test]
    fn shape_cast_without_schema_returns_none() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let n = arena.add(AExpr::Cast {
            expr: c,
            dtype: DataType::Int64,
            options: CastOptions::Strict,
        });
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_none());
    }

    /// CAST with `CastOptions::NonStrict` — refused (PR-2.3 cycle-1 must-fix).
    /// Polars NonStrict overflow → null differs from Vortex's fail-on-overflow;
    /// pushing down would convert Polars's null-on-overflow into a scan error.
    #[test]
    fn shape_cast_non_strict_returns_none() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let n = arena.add(AExpr::Cast {
            expr: c,
            dtype: DataType::Int64,
            options: CastOptions::NonStrict,
        });
        let schema = schema_a_b_int32();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_none());
    }

    /// CAST with `CastOptions::Overflowing` — refused (PR-2.3 cycle-1 must-fix).
    /// Polars Overflowing wraps on overflow; Vortex errors. Same divergence as
    /// the NonStrict case.
    #[test]
    fn shape_cast_overflowing_returns_none() {
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let n = arena.add(AExpr::Cast {
            expr: c,
            dtype: DataType::Int64,
            options: CastOptions::Overflowing,
        });
        let schema = schema_a_b_int32();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_none());
    }

    /// CAST to Decimal — refused at the dtype-mapper level (Vortex Decimal
    /// scale/precision interactions are NOT validated at the polars-vortex layer;
    /// tracked in the function doc as deferred).
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
        let schema = schema_a_b_int32();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_none());
    }

    /// CAST nested in a comparison — `col.cast(Int64) > 100` per the plan's PR-13.3
    /// acceptance test. The literal is `lit_i64` (not `lit_i32`) to mirror the
    /// post-TYPE_COERCION shape that production AExpr produces; the PR-2.4 cycle-2
    /// comparison pairwise gate would refuse if the operand dtypes differed.
    #[test]
    fn shape_cast_then_compare() {
        use polars_core::prelude::AnyValue;
        let mut arena = Arena::new();
        let c = col(&mut arena, "a");
        let cast_node = arena.add(AExpr::Cast {
            expr: c,
            dtype: DataType::Int64,
            options: CastOptions::Strict,
        });
        let l = arena.add(AExpr::Literal(LiteralValue::Scalar(Scalar::new(
            DataType::Int64,
            AnyValue::Int64(100),
        ))));
        let n = binop(&mut arena, cast_node, Operator::Gt, l);
        let schema = schema_a_b_int32();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_some());
    }

    // === PR-2.4 / PR-13.4: Struct field access in predicates ===

    /// Helper: build `AExpr::Function` for `IRStructFunction::FieldByName(name)`.
    #[cfg(feature = "dtype-struct")]
    fn struct_field(arena: &mut Arena<AExpr>, name: &str, arg: Node) -> Node {
        let expr_ir = ExprIR::new(arg, OutputName::Alias(PlSmallStr::EMPTY));
        arena.add(AExpr::Function {
            input: vec![expr_ir],
            function: IRFunctionExpr::StructExpr(IRStructFunction::FieldByName(PlSmallStr::from(
                name,
            ))),
            options: Default::default(),
        })
    }

    /// Helper: schema where `s` is `Struct { inner: String, count: Int32 }`.
    #[cfg(feature = "dtype-struct")]
    fn schema_struct() -> Schema {
        use polars_core::prelude::Field;
        let mut s = Schema::default();
        let struct_dtype = DataType::Struct(vec![
            Field::new(PlSmallStr::from("inner"), DataType::String),
            Field::new(PlSmallStr::from("count"), DataType::Int32),
        ]);
        s.with_column(PlSmallStr::from("s"), struct_dtype);
        s
    }

    /// PR-13.4 acceptance: `col.struct.field("inner") == "x"` against a
    /// `Struct { inner: String, .. }` column pushes down.
    #[cfg(feature = "dtype-struct")]
    #[test]
    fn shape_struct_field_then_compare() {
        let mut arena = Arena::new();
        let s = col(&mut arena, "s");
        let field_node = struct_field(&mut arena, "inner", s);
        // Literal "x" — use String scalar.
        use polars_core::prelude::AnyValue;
        let lit_node = arena.add(AExpr::Literal(LiteralValue::Scalar(Scalar::new(
            DataType::String,
            AnyValue::StringOwned(PlSmallStr::from("x")),
        ))));
        let n = binop(&mut arena, field_node, Operator::Eq, lit_node);
        let schema = schema_struct();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_some());
    }

    /// Struct field access without schema → conservative refuse (the gate cannot
    /// verify the field exists in the struct's dtype).
    #[cfg(feature = "dtype-struct")]
    #[test]
    fn shape_struct_field_without_schema_returns_none() {
        let mut arena = Arena::new();
        let s = col(&mut arena, "s");
        let n = struct_field(&mut arena, "inner", s);
        assert!(aexpr_to_vortex_expression(n, &arena, None).is_none());
    }

    /// Struct field access referencing a non-existent field → refuse (Vortex
    /// `GetItem.return_dtype` would `vortex_err!` at scan-time).
    #[cfg(feature = "dtype-struct")]
    #[test]
    fn shape_struct_field_unknown_field_returns_none() {
        let mut arena = Arena::new();
        let s = col(&mut arena, "s");
        let n = struct_field(&mut arena, "nonexistent", s);
        let schema = schema_struct();
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_none());
    }

    /// Struct field access on a non-struct column → refuse (struct_field_exists
    /// returns false for non-Struct dtype).
    #[cfg(feature = "dtype-struct")]
    #[test]
    fn shape_struct_field_on_int_column_returns_none() {
        let mut arena = Arena::new();
        let a = col(&mut arena, "a");
        let n = struct_field(&mut arena, "inner", a);
        let schema = schema_a_b_int32(); // `a` is Int32, not Struct
        assert!(aexpr_to_vortex_expression(n, &arena, Some(&schema)).is_none());
    }

    /// Nested struct access `s.field("outer").field("inner")` — the convertor
    /// recurses through resolve_inner_dtype's StructExpr arm.
    #[cfg(feature = "dtype-struct")]
    #[test]
    fn shape_struct_field_nested() {
        use polars_core::prelude::Field;
        let mut arena = Arena::new();
        let s = col(&mut arena, "s");
        let outer = struct_field(&mut arena, "outer", s);
        let inner = struct_field(&mut arena, "inner", outer);
        // Schema: `s: Struct { outer: Struct { inner: String } }`.
        let inner_struct = DataType::Struct(vec![Field::new(
            PlSmallStr::from("inner"),
            DataType::String,
        )]);
        let outer_struct =
            DataType::Struct(vec![Field::new(PlSmallStr::from("outer"), inner_struct)]);
        let mut schema = Schema::default();
        schema.with_column(PlSmallStr::from("s"), outer_struct);
        assert!(aexpr_to_vortex_expression(inner, &arena, Some(&schema)).is_some());
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
