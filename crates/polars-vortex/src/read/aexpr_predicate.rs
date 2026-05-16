// SPDX-License-Identifier: Apache-2.0
//
// MOVED to `crates/polars-plan/src/plans/aexpr/predicates/vortex_convertor.rs`.
//
// This module was originally drafted here per the PR-2.1 plan row's "Files touched" list,
// but `polars-vortex` cannot depend on `polars-plan` (the dependency goes the other way —
// `polars-plan` declares an optional `polars-vortex` workspace dep behind its `vortex`
// feature, gating the `FileScanIR::Vortex` variant). The convertor needs `AExpr` /
// `LiteralValue` / `Operator` / `IRBooleanFunction` types which live in `polars-plan`, so
// the module has to live there instead.
//
// This file is not registered in `read/mod.rs`; it's a stub left in place to document the
// architectural correction caught during PR-2.1 implementation. The plan PR-2.1 row's
// `Files touched (expected)` column has been amended in the cycle-4 re-plan-of-phase-2
// commit to reflect the corrected location.
