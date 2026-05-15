# Vortex Integration into Polars — big-plans plan

> Continuation of [spiraldb/polars#1](https://github.com/spiraldb/polars/pull/1) (`vortex-integration`, 31 existing commits, +6,497/-80, 73 tests). big-plans takes over the remaining work — retroactive ratification + CI green-up + PR-13 aggressive AExpr pushdown + PR-8 file-stats + PR-6 multi-file/nested coverage + PR-14 benches — and lands as one squash-merged PR onto `spiraldb:main`.

## Current State

```yaml
status: planning
branch: vortex-integration
planning_sub_flow: initial
current_phase: ""
phase_index: 1
current_pr: null
pr_index: 1
outstanding_must_fix: 0
deferred_items_total: 0
last_user_touchpoint: 2026-05-15T14:34:14Z
last_user_touchpoint_what: "all 4 re-opened Step 1.4 decisions resolved by user via sequential AskUserQuestion; passing Step 1.7 gate next"
subagent_invocations_this_pr: 0
subagent_invocations_total: 6
review_cycles_this_pr: 0
phase_entry_sha: null
phase_end_cycle: 0
phase_end_reject_cycles: 0
last_phase_end_verdict: null
last_commit: ce2a2b900
```

## Context

Continuation of the polars-vortex integration. The integration plumbs Vortex columnar files as a first-class peer of Parquet through Polars' DSL / IR / mem-engine / streaming / C-ABI / Python layers. User is a Vortex stakeholder ([spiraldb.com](https://spiraldb.com) / [vortex.dev](https://vortex.dev)); the goal is upstream-grade quality so the work can eventually merge to `pola-rs/polars` after landing on `spiraldb:main`.

Two hand-prompted gauntlet review passes (`33b56a879`, `c12431296`) have already surfaced + fixed substantive bugs (decimal wraparound, u64→usize row-count truncation, dead cache field, field-metadata loss, transmute hardening, dead options removal, `cache`→`segment_cache` rename, hashmap iteration-order determinism). This big-plans task is the **third adversarial review pass** plus the remaining substantive feature work plus CI green-up.

**Work shape**: feature-integration. Highest-leverage insight: **Analogous prior art** — every new PR-13 shape MUST cite the equivalent in [vortex-datafusion/src/convert/exprs.rs](file:///Users/will/git/vortex/vortex-datafusion/src/convert/exprs.rs).

**Success criteria**: four phases land cleanly through inner-loop (2-vote) + phase-end (3-/4-vote) gauntlet reviews; CI is green on the open PR; all phase exit criteria pass; the cumulative diff (31 existing + ~14 new + plan-evolution + plan-deletion) squash-merges as one commit onto `spiraldb:main`.

## Out of scope

- **No architectural reversal of settled decisions.** Per the existing 1,145-line plan and handoff: segment cache default (process-global ON), runtime choice (Polars' global `ASYNC` Tokio runtime; **no second runtime**), C-ABI bridge approach (`mem::transmute` between polars-arrow and upstream Arrow with size+align asserts + runtime length check), `SpecializedColumnPredicate` fast path (preserved during PR-13 transitional phases), crate layout (`crates/polars-vortex/` mirrors `polars-parquet`'s public-facing shape). The Phase 1 4-vote ratification or the architectural-coherence reviewer in Phase 4 may flag concerns; these become explicit tradeoffs (recorded in `Accepted tradeoffs / r1 traps`).
- **No new file formats** beyond Vortex.
- **No public-API breakage** in Polars' non-Vortex surfaces. The default Polars build (without `vortex` feature) must remain unaffected after every phase.
- **No upstream Vortex changes required for Phases 1–3.** PR-13.5 (temporal extracts) may need a Vortex `datetime_parts` op; if absent at the pinned Vortex SHA, PR-13.5 slips to a follow-up rather than blocking Phase 2.
- **No `polars-arrow::ffi::ArrowArray::from_ffi_parts(...)` upstream contribution** in this branch. The current `mem::transmute` with size+align asserts is accepted as a tradeoff. The upstream API is worth filing as a separate polars issue but is out of scope here.
- **No re-introduction of dead options** removed in prior reviews (`use_statistics`, `aggressive_pushdown`, `push_projection` were deleted in `33b56a879`; don't reintroduce without a wired consumer).
- **No `i8`/`i16`/`u8`/`u16` roundtrip tests in Phase 1.** Requires enabling `polars-core/dtype-i*` features in `polars-vortex/Cargo.toml`; deferred to Phase 3 alongside other dtype-completeness work.
- **No upstream Vortex API redesign**. PR-1.1 migrates from the workspace path-dep to `vortex = "0.70.0"` (published on crates.io); if polars-vortex code uses upstream Vortex APIs that 0.70.0 does not expose, PR-1.1 scope expands to make polars-vortex compatible with 0.70.0's surface (or move to a later released version), NOT to push API changes into Vortex.
- **No big-bang single-commit cutover for PR-13.** Per Subagent 7's design recommendation, ship Option B trajectory: parallel path first (PR-13.1–.5), delete fast path last (PR-13.6).

## Prior art / external references

Populated by Phase 1.2 cross-codebase prior-art subagent + user input. Each entry is a load-bearing reference for the going-forward PRs.

- **[`/Users/will/git/vortex/vortex-datafusion/src/convert/exprs.rs`](file:///Users/will/git/vortex/vortex-datafusion/src/convert/exprs.rs)** (1,008 LOC) — line-for-line precedent for PR-13. `ExpressionConvertor` trait with `can_be_pushed_down` + `convert` two-phase pattern. Per-shape coverage maps 1:1 to Polars `AExpr` variants. **PR-13's convertor mirrors this end to end.**
- **[`/Users/will/git/vortex/vortex-duckdb/src/convert/{expr,table_filter,scalar}.rs`](file:///Users/will/git/vortex/vortex-duckdb/src/convert/)** — simpler single-pass `Option<Expression>` form. `StructExtract` arm at `table_filter.rs:71-73` is the cleanest precedent for nested-struct predicates. `Optional` arm (lines 74-80) shows the "swallow inner errors as residual" pattern.
- **[`/Users/will/git/vortex/vortex-array/src/expr/exprs.rs`](file:///Users/will/git/vortex/vortex-array/src/expr/exprs.rs)** — Vortex Expression IR builder helpers (`checked_add`, `cast`, `get_item`, `nested_case_when`, `like`, `list_contains`, `is_null`, `is_not_null`). Every PR-13 shape maps to one of these.
- **[`/Users/will/git/vortex/docs/concepts/expressions.md`](file:///Users/will/git/vortex/docs/concepts/expressions.md)** — Vortex Expression IR concepts. Critical at lines 55-63: **strict type coercion**; only nullability auto-coerces. PR-13's convertor must match types explicitly.
- **[`/Users/will/git/polars/.claude/plans/we-want-to-scope-transient-patterson.md`](file:///Users/will/.claude/plans/we-want-to-scope-transient-patterson.md)** — the existing 1,145-line plan; load-bearing architectural reference. **Don't re-litigate settled decisions.** §5 pushdown coverage table is the contract for PR-13.
- **[`/Users/will/git/polars/.claude/plans/we-re-going-to-take-elegant-cookie.md`](file:///Users/will/.claude/plans/we-re-going-to-take-elegant-cookie.md)** — handoff plan from the planning session; the 4-phase skeleton this big-plans plan refines.
- **`crates/polars-plan/src/plans/aexpr/predicates/column_expr.rs:27-296`** — Polars' own `aexpr_to_column_predicates` extractor; the reference for "iterate minterms, pattern-match `arena.get(node)`, build target IR per shape." PR-13 mirrors this style.
- **`crates/polars-plan/src/plans/optimizer/parquet_metadata_prune.rs`** (added May 2026, upstream PR #27535) — the canonical IR-plan optimizer-pass pattern. Any Vortex IR-plan optimization (e.g., footer trimming) should follow this template.
- **`crates/polars-parquet/`** + **`crates/polars-stream/src/nodes/io_sources/parquet/`** — gold-standard Polars file-format integration. Subagent 4's report distills 5 prior-art lessons; PR-13 (L1, L5), PR-8 (L2), Phase 3 (L3), polars-vortex sink shape (L4).
- **`crates/polars-vortex/README.md`** (~430 LOC) — user-facing docs reflecting the API surface as of HEAD `9a899dc0e`.

## Architecture

The integration's architecture is documented exhaustively in the existing 1,145-line plan. This section captures the **minimum needed** for review/refresh during big-plans phases.

### Layer map (verified by Slot 1 subagent against HEAD `9a899dc0e`)

```
Python
  ├─ py-polars/src/polars/io/vortex/{__init__,functions}.py
  ├─ py-polars/src/polars/{lazyframe/frame.py:3139, dataframe/frame.py}
  └─ py-polars/tests/unit/io/test_vortex.py                                (8 Python tests)

PyO3 bridge
  └─ crates/polars-python/src/lazyframe/general.rs:336-365 (new_from_vortex)
                                                  :765-807 (sink_vortex)

DSL → IR
  ├─ crates/polars-plan/src/dsl/file_scan/mod.rs                           (FileScanDsl::Vortex + FileScanIR::Vortex)
  ├─ crates/polars-plan/src/dsl/options/mod.rs:329-330                     (FileWriteFormat::Vortex)
  └─ crates/polars-plan/src/plans/conversion/dsl_to_ir/scans.rs:293-359    (vortex_file_info — schema discovery)

Mem-engine
  └─ crates/polars-mem-engine/src/planner/lp.rs:448-459                    (Vortex branch: create_skip_batch_predicate = false)

Streaming
  ├─ crates/polars-stream/src/nodes/io_sources/vortex/{mod.rs:303 LoC, builder.rs:71 LoC}
  ├─ crates/polars-stream/src/nodes/io_sinks/writers/vortex/mod.rs         (153 LoC)
  └─ crates/polars-stream/src/physical_plan/to_graph.rs:843                (PR-13 integration point — AExpr arena live here)

polars-vortex crate                                                        (2,697 LoC, 42 unit + 23 integration = 65 Rust tests)
  ├─ src/lib.rs (re-exports), src/session.rs (global VortexSession + cache)
  ├─ src/read/{schema,read_at,predicate,array_bridge,options}.rs + mod.rs
  └─ src/write/{strategy,writer,sink_writer,array_bridge,df_to_stream,options}.rs + mod.rs

Vortex (upstream, path-dep at ../../../../vortex/vortex)
  └─ vortex / vortex-array / vortex-file / vortex-scan / vortex-layout / vortex-io
```

### Four settled architectural moves (from existing plan + memory; do NOT re-litigate)

1. **Mem-engine delegates to streaming.** All file-format scans route through `polars-stream`. The mem-engine planner sets `create_skip_batch_predicate = false` for Vortex; Vortex's `LayoutReader::pruning_evaluation` already does zone-level pruning.
2. **Polars' global `ASYNC` Tokio runtime as the single executor.** `Handle::new(Arc::downgrade(Arc::new(ASYNC.handle()) as Arc<dyn Executor>))` plumbs Vortex async work onto Polars' threads. No second runtime.
3. **Arrow C Data Interface as a zero-copy bridge.** `polars-arrow::ffi::ArrowArray` and `arrow_array::ffi::FFI_ArrowArray` are `#[repr(C)]` with identical 9-field layout. `mem::transmute` between them moves ~80 bytes. Compile-time `size_of`+`align_of` asserts + runtime length check provide safety. Works both directions (read + write).
4. **`SpecializedColumnPredicate` already extracted by the optimizer.** Polars' `ColumnPredicates::predicates` map exposes per-column predicates in structured form. The current convertor pattern-matches on the specialized variants and emits Vortex `Expression`s. PR-13 extends with an AExpr-direct path (architectural fork resolved in Step 1.4).

### Architectural discovery from Phase 1.2 (informs PR-13 design)

At the file-reader call site (`crates/polars-stream/src/nodes/io_sources/vortex/mod.rs:242`), **the `AExpr` arena is gone**. The reader receives a `ScanIOPredicate` (`crates/polars-io/src/predicates.rs:459-475`) where `predicate: Arc<dyn PhysicalIoExpr>` is opaque. The only site where `ctx.expr_arena` is live alongside `pred: &ExprIR` is `crates/polars-stream/src/physical_plan/to_graph.rs:843` (inside the `FileScanIR::Vortex` branch of `lower_node`).

**PR-13's AExpr-direct convertor must run at `to_graph.rs:843`**, not inside `polars-vortex/src/read/predicate.rs`. The Vortex `Expression` is attached to the Vortex builder via a Vortex-specific side channel (parallel to how `metadata: VortexFooterRef` already threads). This reshapes the Option A vs B framing recorded in `Key decisions`.

## Key decisions

Populated incrementally during Step 1.4 design-tree interview. Each row is a leaf of the design tree resolved during planning.

| Decision | Choice | Rationale |
|---|---|---|
| Reuse existing 31 commits vs rewrite from scratch | **Reuse** | 73 tests passing locally; two prior gauntlet review passes already surfaced + fixed substantive bugs; architecture documented; prior planning session decided same with full context. (Resolved Phase 0.) |
| Work shape | **feature-integration** | Adding Vortex into existing Polars systems with many touch points. Highest-leverage insight: Analogous prior art. (Resolved Step 1.1.) |
| Phase 1.2 fan-out | **6 subagents** (Slots 1, 3, 4, 5, 6, 7) | Skipped Slot 2 (threading settled per existing plan). Each slot's report informs a distinct plan section. (Resolved Step 1.2.) |
| CI green-up approach | **crates.io `vortex = "0.70.0"`** (user-confirmed; Vortex is published) | Replace workspace path-dep `path = "../../../../vortex/vortex"` with `version = "0.70.0"`. Strictly better than git-rev, CI-workflow clone, or feature-gating: cleanest dep, no release blocker, version-pinned. PR-1.1 scope. (Resolved Step 1.4 by user.) |
| Final phase plan | **4 phases as drafted (handoff plan)** | (1) Ratify + crates.io transition, (2) PR-13 AExpr pushdown, (3) PR-8 file-stats + PR-6 multi-file/nested, (4) PR-14 benches + final polish. ~14 PRs across 4 phases. Clean separation of concerns; bundled Phase 3 reflects that PR-8 (backend feature) and PR-6 (coverage tests) both target the same new-coverage objective. (Resolved Step 1.4 by user, 2026-05-15.) |
| PR-13 architecture | **Option B → A via PR-2.6 cutover** | Ship AExpr-direct convertor as parallel path through PR-2.2/.3/.4/.5 (new path runs first; SpecializedColumnPredicate fast path remains as fallback). PR-2.6 (last sub-PR of Phase 2) deletes the fast path. `POLARS_VORTEX_VERIFY_PUSHDOWN=1` debug env var (added in PR-2.2) emits divergences between paths during the parallel window. End-state = Option A's single-path. More bisectable than pure Option A's big-bang cutover; semantic-divergence catches happen during parallel window. (Resolved Step 1.4 by user, 2026-05-15.) |
| PR-8 leads Parquet on `table_statistics` | **Lead the pattern; no API change; no upstream issue** | `pub struct TableStatistics(pub Arc<DataFrame>)` already exists at `crates/polars-plan/src/dsl/file_scan/mod.rs:290`; the consumed contract is `{col}_min`/`{col}_max`/`{col}_nc` per live predicate column, one row per file (see `polars-mem-engine/src/scan_predicate/functions.rs:276-306`). polars-vortex implements `VortexFile::file_stats() → DataFrame` in this shape and wires through `vortex_file_info`. PR-3.1 also refines the polars-mem-engine override at `planner/lp.rs:448-459` so file-level pruning fires (Vortex's INTERNAL pruning stays disabled; file-level pruning is a different layer). Land on spiraldb:main; defer upstream coordination to upstreaming time. (Resolved Step 1.4 by user, 2026-05-15.) |
| Per-phase review-counts | **4 / 4 / 4 / 4** | All four phase-end reviews use the 4-vote `phase-4` gauntlet preset (Spec-adherence + Correctness skeptic + Maintainability auditor + Architectural-coherence auditor). Max thoroughness across phases; matches the skill's "thoroughness over cost" calibration literally. Pros: architectural-coherence applied at every checkpoint (drift caught earliest); cost-not-priority. Trade: ~25% more reviewer cost per phase-end vs 3-vote default. (Resolved Step 1.6 by user, 2026-05-15.) |

## Project-specific BANS

Reviewers must flag these as immediate **must-fix** if found in the diff. Seeded by Phase 1.2 conventions-extraction subagent (Slot 6).

**General (apply regardless of language/shape):**

- Justification-comment reward-hacking: long `// TODO`, `// FIXME`, `// PORT NOTE`, or any `// SAFETY:` >100 chars explaining why a hack is OK.
- Behavioral drift without test coverage: any change to observable behavior requires an added or updated test.
- Silent error swallowing: `let _ = fallible_op()`, `.ok()`, bare `try { } catch {}` without explicit rationale.

**Polars-wide:**

- **No `std::collections::{HashMap, HashSet}`** — workspace `crates/clippy.toml` disallows both. Use `PlHashMap` / `PlHashSet` from `polars_utils::aliases`.
- **No direct `regex::Regex::new`** — workspace `crates/clippy.toml` disallows. Use `polars_utils::regex_cache::compile_regex` for cached compilation.
- **No `.unwrap()` / `.expect(...)`** in production paths. Test code only. Use `polars_err!(...)?` or `polars_bail!(...)`.
- **No proposing AI-generated PRs** against `pola-rs/polars` upstream that violate `AI_POLICY.md` (no "good first issue", no unaccepted issues; must be disclosed + human-tested). Scope: relevant if/when we eventually upstream.

**Vortex-integration-specific:**

- **No bare `PlHashMap::new()` / `PlHashSet::new()`** in code paths the `vortex` feature reaches. Hashbrown 0.16 (Polars) + 0.17 (Vortex transitive) coexistence breaks type inference. Use explicit `<K, V>` annotations. (Pattern: `crates/polars-sql/src/context.rs:2017-2027`.)
- **No `vortex::dtype::DType::to_arrow_schema()`** — returns upstream `arrow_schema::Schema`, not polars-arrow's `ArrowSchema`. Use `polars_vortex::read::schema::vortex_dtype_to_schema`.
- **No `VortexSession::default()` stored in `LazyLock` / static**. Always `.with_handle(ASYNC.handle())`. `Handle::find()` returns `None` outside Tokio context.
- **No `mem::transmute` between Arrow FFI structs without BOTH `size_of` + `align_of` compile-time asserts AND runtime length-equality check** post-import. Size-only check misses alignment divergence.
- **No `as` casts on Vortex `DecimalDType` precision/scale** to/from Polars `Decimal(usize, usize)`. Use `try_into().ok()?` (predicate convertor, returns None → residual) or `polars_bail!` (schema convertor, errors). Negative scales MUST error.
- **No `as`-cast `u64 → usize` on row counts**. Use `usize::try_from(row_count).unwrap_or(usize::MAX)` (32-bit platform truncation guard).
- **No iterating `ColumnPredicates::predicates` (a `PlHashMap`) without sorting by column name first**. Deterministic ordering required; Vortex's pruning evaluator short-circuits left-to-right.
- **No adding fields to `VortexScanOptions` / `VortexWriteOptions` / `ScanArgsVortex` without a wired downstream consumer**. Dead-option scaffolding is the bug pattern that produced two prior fix waves.
- **No naming a `VortexScanOptions` field colliding with `ScanArgsVortex` of different type** (`cache: VortexCacheMode` vs `cache: bool` → `segment_cache`).
- **No rebuilding `Field` from `(name, dtype, nullable)` without `with_metadata(...)`**. Per-field metadata must survive C-ABI roundtrip.
- **No second Tokio runtime.** Vortex's `Handle` wraps `ASYNC.handle()`. Spawning your own is forbidden.
- **No `pub` types from upstream `arrow-array` / `arrow-schema` leaking out of `polars-vortex`**. Today's `pub use` in `read/array_bridge.rs:198-199` is the single justified bridge point.
- **No bypassing `PolarsInstrumentedVortexReadAt`** for any read path. Local / cloud / in-memory all route through it for concurrency-budget + IOMetrics plumbing.
- **No new direct `vortex`-internal symbol use outside the bridge files**. Other crates depend on `polars_vortex::vortex::...` and bridge helpers, not directly on upstream `vortex-*` crate names.
- **No re-introduction of the workspace path-dep** to Vortex source after PR-1.1 migrates to crates.io. New direct dependencies on `vortex`-internal symbols (vs the re-export through `polars_vortex::vortex`) are still discouraged: they re-couple polars-vortex to upstream Vortex's unstable internals.
- **No omitting the explicit Vortex branch in `polars-mem-engine/src/planner/lp.rs:448-459`** that forces `create_skip_batch_predicate = false`.
- **No new `polars-vortex` dtype feature without propagating** through `polars-vortex` → `polars-plan` → `polars-lazy` → `polars` (and `polars-mem-engine` + `polars-stream` where applicable).

**User preferences:**

- **No emoji** in code/comments/commit messages unless explicitly requested.
- **Cargo invocations** always prefixed with `GIT_CONFIG_GLOBAL=/tmp/clean-home/.gitconfig CARGO_NET_GIT_FETCH_WITH_CLI=false` in this worktree (workspace `[patch.crates-io]` SSH-rewrite issue).

**Work-shape highest-leverage insight (feature-integration):** **Analogous prior-art** — every new PR-13 shape MUST cite the equivalent in `/Users/will/git/vortex/vortex-datafusion/src/convert/exprs.rs`. If a shape has no DF analogue, that's a flag that it's genuinely novel and deserves extra adversarial review (reviewers should query: is this shape representable at all in Vortex's expression IR?).

## Phases and PRs

### Phase summary

Initial draft from handoff-plan skeleton, refined by Phase 1.2 subagent findings. Per-phase scope finalized by Step 1.4 design-tree interview. Review-counts confirmed by user in Step 1.6.

| Phase | Name | Scope (one line) | Exit criteria (machine-checkable) | PR count | Review-count |
|---|---|---|---|---|---|
| 1 | Ratify + crates.io transition | Retroactive 4-vote gauntlet of cumulative diff vs `main` + path-dep → crates.io migration + cheap polish | (a) 4-vote phase-end review accepts. (b) `cargo check -p polars --features vortex,cloud,parquet,dtype-full` clean. (c) `cargo test -p polars-vortex --features dtype-date,dtype-datetime,dtype-time,dtype-decimal` → 65 Rust tests pass. (d) `pytest py-polars/tests/unit/io/test_vortex.py` → 8 tests pass. (e) `gh pr checks 1 --repo spiraldb/polars` shows green for Rust + Python core checks. | 3-4 | **4-vote** |
| 2 | PR-13 aggressive AExpr pushdown | New convertor module (Option B trajectory per user Step 1.4 decision); arithmetic / CAST / struct field access / temporal extracts shipped incrementally; PR-2.6 (final sub-PR) deletes the `SpecializedColumnPredicate` fast path | (a) 4-vote review accepts. (b) Every row in existing plan's §5 pushdown coverage table implemented + tested OR documented as deliberately deferred. (c) New e2e tests verify pushdown engagement via Vortex `Expression::display_tree()` or `POLARS_VERBOSE` log assertion. (d) Build/test suite still green. | 5-6 | **4-vote** |
| 3 | PR-8 file-stats + PR-6 multi-file / nested coverage | Populate `UnifiedScanArgs::table_statistics` from Vortex footer (lead the pattern, no API change); add multi-file scan tests + schema-evolution policy round-trips (`missing_columns`/`extra_columns`/`cast_options`) + nested-type (List/Struct) end-to-end + small-int dtypes (i8/i16/u8/u16) | (a) 4-vote review accepts. (b) Multi-file scan with file-level stats shows whole-file skips (via `EXPLAIN` or Vortex pruning counters). (c) Schema-evolution policy tests pass across the missing/extra/cast matrix. (d) Nested-type roundtrip tests pass. (e) Small-int dtype roundtrip tests pass. (f) Build/test suite green. | 3-4 | **4-vote** |
| 4 | PR-14 benches + final polish + merge prep | Criterion benches (`crates/polars/benches/io_vortex.rs`): cold-cache full scan, filtered scan with column predicates (TPC-H Q6/Q14 style), cloud-read latency, second-run cache-hit ratio, write throughput. Top-level README mention; remaining unblocked deferred items resolved; final 4-vote review on the FULL cumulative diff vs main | (a) 4-vote review accepts. (b) `cargo bench --features vortex --bench io_vortex` compiles + runs. (c) TPC-H Q6/Q14 comparison documented in `crates/polars-vortex/README.md`. (d) `gh pr checks 1 --repo spiraldb/polars` still green. (e) Ready for squash-merge. | 2-3 | **4-vote** |

### PR enumeration

Initial draft. Refined during Step 1.4 + per-PR scope-checks at the start of each Phase 2 inner-loop iteration.

| PR | Phase | Scope (one line) | Files touched (expected) | Acceptance (specific, testable) |
|---|---|---|---|---|
| PR-1.1 | 1 | crates.io transition: replace workspace path-dep with `vortex = "0.70.0"` | `Cargo.toml` (workspace `:118`), `Cargo.lock`. If polars-vortex code uses Vortex internals not in 0.70.0's surface: scope expands to either pin a later released version or adapt polars-vortex. | (a) `cargo check -p polars --features vortex,cloud,parquet,dtype-full` clean. (b) 65 Rust + 8 Python tests pass locally. (c) `gh pr checks 1` shows green on clippy-stable / check-features / test (ubuntu) / Lint Rust / build-rust-docs. |
| PR-1.2 | 1 | Phase 1 polish: `VortexCacheMode` Python surface (`cache_mode=` param), in-memory `ScanSourceRef::Buffer` zero-copy if low-effort, `polars-python::lazyframe::visitor::nodes.rs::scan_type_to_pyobject` feature-gating fix | `polars-python/src/lazyframe/general.rs`, `py-polars/src/polars/io/vortex/functions.py`, `polars-python/src/lazyframe/visitor/nodes.rs`, `polars-vortex/src/read/read_at.rs` | `cache_mode` Python parameter exposed + tested; visitor fix verified; remaining items moved to `Deferred work` |
| PR-1.3 | 1 | Address any must-fix items from Phase 1 retroactive 4-vote gauntlet | varies | Phase 1 end-of-phase review accepts |
| PR-2.1 | 2 | PR-13.1 — AExpr-direct convertor module foundation (Column / Literal / Eq/NotEq/Lt/LtEq/Gt/GtEq/And/Or / IsNull/IsNotNull/Not) | `crates/polars-vortex/src/read/aexpr_predicate.rs` (new), `crates/polars-vortex/src/read/mod.rs` | Module compiles; unit tests for each shape pass with arena-constructed inputs; no wire-up yet |
| PR-2.2 | 2 | PR-13.2 — Wire convertor at `to_graph.rs:843` + ship arithmetic in predicates | `aexpr_predicate.rs` (extend), `crates/polars-stream/src/physical_plan/to_graph.rs:843`, `crates/polars-stream/src/nodes/io_sources/vortex/builder.rs` + `mod.rs` | e2e test scans `col + 1 == 5` and asserts pushed Vortex `Expression` is non-None; `POLARS_VORTEX_VERIFY_PUSHDOWN=1` debug-mode comparison emits no divergences |
| PR-2.3 | 2 | PR-13.3 — CAST in predicates | `aexpr_predicate.rs`, possibly `polars-vortex/src/read/schema.rs` | e2e test for `col.cast(Int64) > 100` over `Int32` column pushes down; decimal-cast residual case documented |
| PR-2.4 | 2 | PR-13.4 — Struct field access in predicates | `aexpr_predicate.rs` | e2e test for `col.struct.field("inner") == "x"` pushes down (gated on `dtype-struct`) |
| PR-2.5 | 2 | PR-13.5 — Temporal extracts (gated on Vortex `datetime_parts` op availability at pinned SHA) | `aexpr_predicate.rs`, possibly Vortex pinning bump | e2e test for `col.dt.year() == 2024` pushes down; if Vortex op unavailable, this PR is moved to `Deferred work` with explicit rationale and Phase 2 still completes |
| PR-2.6 | 2 | PR-13.6 — Delete `SpecializedColumnPredicate` fast path (Option B → Option A migration) | `crates/polars-vortex/src/read/predicate.rs` (mostly delete), `aexpr_predicate.rs` (absorb scalar / LIKE helpers), `crates/polars-stream/src/nodes/io_sources/vortex/mod.rs:242` (call-site change) | All 65 Rust + 8 Python tests still pass; old path gone; `read/predicate.rs` reduced to scalar+LIKE helpers or deleted entirely |
| PR-3.1 | 3 | PR-8 — File-level stats → `UnifiedScanArgs::table_statistics` from Vortex footer (lead the pattern; no API change) | `crates/polars-plan/src/plans/conversion/dsl_to_ir/scans.rs:293-359`, `crates/polars-vortex/src/read/file_stats.rs` (new), `crates/polars-mem-engine/src/planner/lp.rs:448-459` (refine Vortex override: internal pruning stays disabled, file-level table_statistics pruning fires) | Multi-file scan with stats-pruning shows whole-file skips via `EXPLAIN` or pruning-counter inspection; new module unit-tested; DataFrame contract matches existing `{col}_min`/`{col}_max`/`{col}_nc` API at `polars-plan/src/dsl/file_scan/mod.rs:290` |
| PR-3.2 | 3 | PR-6.1 — Multi-file scan tests + schema-evolution policy round-trips | `py-polars/tests/unit/io/test_multiscan.py` (add Vortex to `SCAN_AND_WRITE_FUNCS`), `py-polars/tests/unit/io/test_vortex.py`, `crates/polars-vortex/tests/roundtrip.rs` | `missing_columns`/`extra_columns`/`cast_options` policy tests pass; `pl.scan_vortex(["a.vortex","b.vortex"])` test covers shape, ordering, schema unification |
| PR-3.3 | 3 | PR-6.2 — Nested-type (List/Struct) end-to-end roundtrip + small-int dtypes (i8/i16/u8/u16) + filter-pushdown engagement assertion | `crates/polars-vortex/tests/roundtrip.rs`, `crates/polars-vortex/Cargo.toml` (add `dtype-i8/i16/u8/u16` features), `crates/polars-stream/src/nodes/io_sources/vortex/mod.rs` (emit `POLARS_VERBOSE` line) | Nested-type roundtrip tests pass; small-int dtype tests pass; `POLARS_VERBOSE=1` engagement assertion via `capfd` in Python tests works |
| PR-4.1 | 4 | PR-14 — Criterion benches | `crates/polars/benches/io_vortex.rs` (new), bench harness wiring | `cargo bench --features vortex --bench io_vortex` compiles + runs all 5 bench categories (cold full scan / filtered Q6/Q14 / cloud / cache-hit / write throughput) |
| PR-4.2 | 4 | Final polish + README refresh | `crates/polars-vortex/README.md`, top-level `README.md` | TPC-H Q6/Q14 comparison numbers documented; remaining deferred items resolved or explicitly carried in `Deferred work` |
| PR-4.3 | 4 | Address any must-fix items from final 4-vote architectural-coherence review | varies | Phase 4 end-of-phase review accepts; PR is merge-ready |

Total: ~14 PRs across 4 phases. Each PR fits the 1-3 commit granularity per `/big-plans` discipline.

## Reference tables

Optional for feature-integration shape; the existing plan's [§5 pushdown coverage table](file:///Users/will/.claude/plans/we-want-to-scope-transient-patterson.md) is the canonical contract for PR-13. Cite verbatim during PR-13 implementation; do not duplicate here.

## Critical files

Files central to the work. Subagents 1, 4, 5 seed.

**polars-vortex crate (the new surface; review boundary)**

- `crates/polars-vortex/src/read/predicate.rs` (498 LOC) — PR-13 refactor target
- `crates/polars-vortex/src/read/schema.rs` (459 LOC) — DType → Arrow + nullable-roundtrip
- `crates/polars-vortex/src/read/read_at.rs` (288 LOC) — instrumented decorator (local / cloud / memory factories)
- `crates/polars-vortex/src/read/array_bridge.rs` (199 LOC) — read-side C-ABI bridge
- `crates/polars-vortex/src/write/df_to_stream.rs` (130 LOC) + `array_bridge.rs` (125 LOC) — write-side
- `crates/polars-vortex/src/session.rs` (81 LOC) — global session + cache
- `crates/polars-vortex/tests/roundtrip.rs` (515 LOC) — 23 integration tests
- `crates/polars-vortex/README.md` (~430 LOC) — user-facing docs

**Integration touchpoints (Polars-side)**

- `crates/polars-plan/src/dsl/file_scan/mod.rs` — DSL + IR `Vortex` variants
- `crates/polars-plan/src/dsl/options/mod.rs:329-330` — `FileWriteFormat::Vortex`
- `crates/polars-plan/src/plans/conversion/dsl_to_ir/scans.rs:293-359` — `vortex_file_info` (PR-8 extends to populate `table_statistics`)
- `crates/polars-plan/src/plans/aexpr/predicates/column_expr.rs:27-296` — reference for AExpr-walking style (PR-13 mirrors)
- `crates/polars-plan/src/plans/optimizer/parquet_metadata_prune.rs` — IR-plan optimizer pass template (May 2026 pattern)
- `crates/polars-mem-engine/src/planner/lp.rs:448-459` — explicit Vortex branch (`create_skip_batch_predicate = false`)
- `crates/polars-stream/src/physical_plan/to_graph.rs:843` — **THE PR-13 integration point** (AExpr arena live here)
- `crates/polars-stream/src/nodes/io_sources/vortex/{mod.rs, builder.rs}` — streaming source (capabilities, begin_read)
- `crates/polars-stream/src/nodes/io_sinks/writers/vortex/mod.rs` — streaming sink
- `crates/polars-python/src/lazyframe/general.rs:336-365, :765-807` — PyO3 bridge
- `py-polars/src/polars/io/vortex/{__init__,functions}.py` + `py-polars/src/polars/lazyframe/frame.py:3139-3240` — Python API

**External references (cross-codebase)**

- `/Users/will/git/vortex/vortex-datafusion/src/convert/exprs.rs` (1,008 LOC) — PR-13's reference convertor
- `/Users/will/git/vortex/vortex-array/src/expr/exprs.rs` — Vortex Expression IR builders
- `/Users/will/git/vortex/vortex-array/src/scalar_fn/fns/` — verify `datetime_parts` op availability before PR-13.5
- `/Users/will/git/vortex/vortex-duckdb/src/convert/{expr,table_filter}.rs` — secondary reference

**CI workflow files**

- `.github/workflows/test-rust.yml` — `test` + `integration-test` + `check-features` (CI-fail source: Vortex path-dep at `Cargo.toml:118`)
- `.github/workflows/test-python.yml` — Python test matrix
- `.github/workflows/lint-rust.yml` — `clippy-stable`, `clippy-nightly`, `rustfmt`, `miri`, `deny`

## Risks

Numbered; each with probability / impact / mitigation.

1. **crates.io `vortex = "0.70.0"` API mismatch with polars-vortex** — P=medium; impact=moderate. The local Vortex checkout at `/Users/will/git/vortex/` may have unreleased changes that polars-vortex depends on; migrating to 0.70.0 could fail to compile if the code uses symbols added after 0.70.0's tag. Mitigation: PR-1.1 IS the verification step. If failure, scope expands to (a) bump to a later released Vortex version that includes the needed symbols, or (b) refactor polars-vortex to use 0.70.0-compatible APIs. Either way Phase 1 absorbs the work, not later phases.
2. **PR-13.5 temporal extracts may need upstream Vortex change** — P=medium; impact=moderate. Vortex's `datetime_parts` op must exist at the pinned SHA. Mitigation: Subagent 5 flagged this; if absent, PR-13.5 slips to `Deferred work` and Phase 2 still completes with PR-13.1–.4 + PR-13.6.
3. **Phase 2 cutover (PR-13.6) deletes the load-bearing `SpecializedColumnPredicate` path** — P=medium; impact=moderate. A predicate shape only the old path covers (not the new AExpr-direct path) would silently lose pushdown after cutover. Mitigation: Option B trajectory means new path is exercised by PR-13.2–.5 e2e tests before cutover; `POLARS_VORTEX_VERIFY_PUSHDOWN=1` debug env var emits divergences during the parallel-path window (added in PR-2.2 alongside wire-up).
4. **PR-8 leads Parquet on `UnifiedScanArgs::table_statistics`** — P=low; impact=low. The TableStatistics API is already stable and public (`crates/polars-plan/src/dsl/file_scan/mod.rs:290`). polars-vortex is the first FILE-FORMAT consumer (vs PythonDataset/Iceberg-style catalog consumers); no API change required. Mitigation: PR-3.1 follows the existing `{col}_min`/`{col}_max`/`{col}_nc` contract verbatim. Subtle interaction with the Vortex polars-mem-engine override (`planner/lp.rs:448-459`) is flagged for PR-3.1 implementation — Vortex's internal pruning stays disabled, file-level table_statistics pruning fires. No upstream coordination needed in this branch.
5. **Mem-engine + streaming engine + plan plumbing fragile to upstream refactors** — P=low; impact=moderate. The integration crosses many crates; an upstream refactor on `to_graph.rs` / `dsl_to_ir/scans.rs` / `polars-mem-engine/planner/lp.rs` could break the integration during Phases 2–4. Mitigation: keep feature gate clean (`#[cfg(feature = "vortex")]`) so disabling vortex restores pre-integration code paths verbatim; pin upstream base SHA at PR open; rebase intentionally.
6. **Hashbrown 0.16/0.17 coexistence type-inference breakage** — P=medium; impact=minor. Any new code path under `vortex` feature can hit this. Mitigation: BAN forces explicit annotations; recurring fix pattern is well-known; reviewers explicitly check for `PlHashMap::new()` without annotation.
7. **AI_POLICY.md / upstream `pola-rs/polars` adoption hesitation** — P=low; impact=high (long-term). spiraldb/polars#1 lives on spiraldb's fork; eventual upstream merge requires `pola-rs/polars` adoption. Mitigation: out of big-plans scope. Merge to `spiraldb:main` is the explicit end-state. Upstreaming is a separate decision after merge.
8. **`Implementation status` ledger grows large** — P=medium over task lifetime; impact=minor. Mitigation: at phase boundaries, archive older entries to `<repo>/.big-plans/vortex-integration-history.md` if section exceeds 500 lines (per plan-template guidance).
9. **The plan file itself becomes load-bearing** — P=low; impact=moderate. Multi-week task; plan-evolution commits accumulate. Mitigation: every plan edit is its own `plan:` commit (BAN #12 in skill); `Current State` YAML is the canonical resume state.

## Verification

### Phase exit verification commands

```sh
cd /Users/will/git/polars/.claude/worktrees/naughty-mccarthy-270d36

# Fast feedback loop (any phase)
GIT_CONFIG_GLOBAL=/tmp/clean-home/.gitconfig CARGO_NET_GIT_FETCH_WITH_CLI=false \
  cargo check -p polars --features vortex,cloud,parquet,dtype-full

# Rust test suite (any phase)
GIT_CONFIG_GLOBAL=/tmp/clean-home/.gitconfig CARGO_NET_GIT_FETCH_WITH_CLI=false \
  cargo test -p polars-vortex --features dtype-date,dtype-datetime,dtype-time,dtype-decimal

# Default Polars build (must remain unaffected — Out of scope contract)
GIT_CONFIG_GLOBAL=/tmp/clean-home/.gitconfig CARGO_NET_GIT_FETCH_WITH_CLI=false \
  cargo check -p polars

# Python smoke
cd py-polars && pytest tests/unit/io/test_vortex.py -v

# Workspace lints (catch convention violations early)
GIT_CONFIG_GLOBAL=/tmp/clean-home/.gitconfig CARGO_NET_GIT_FETCH_WITH_CLI=false \
  cargo clippy --features vortex,cloud,parquet,dtype-full --workspace -- -D warnings

# CI status (Phase 1 exit criterion; revisited at Phase 4)
gh pr view 1 --repo spiraldb/polars
gh pr checks 1 --repo spiraldb/polars

# Phase 4: benches
GIT_CONFIG_GLOBAL=/tmp/clean-home/.gitconfig CARGO_NET_GIT_FETCH_WITH_CLI=false \
  cargo bench --features vortex --bench io_vortex
```

### End-to-end smoke (Python; per phase end)

```python
import polars as pl

# Phase 1 baseline — existing pushdown shapes
lf = pl.scan_vortex("data.vortex").filter(pl.col("a") == 42).slice(-100, 50)
df = lf.collect()
assert df.height == expected_count

# Phase 2 (PR-13 additions, per sub-PR)
lf = pl.scan_vortex("data.vortex").filter(pl.col("a") + 1 > 5).collect()                    # PR-2.2 arithmetic
lf = pl.scan_vortex("data.vortex").filter(pl.col("a").cast(pl.Int64) > 100).collect()       # PR-2.3 CAST
lf = pl.scan_vortex("data.vortex").filter(pl.col("s").struct.field("x") == "v").collect()   # PR-2.4 struct
lf = pl.scan_vortex("data.vortex").filter(pl.col("d").dt.year() == 2024).collect()          # PR-2.5 temporal

# Phase 3 (multi-file / schema-evolution / nested)
lf = pl.scan_vortex(["a.vortex", "b.vortex"])                                                # multi-file
lf = pl.scan_vortex("data.vortex", missing_columns="insert")                                # schema-evolution
df = pl.read_vortex("nested.vortex")  # List/Struct roundtrip

# Phase 4 — sustained correctness; cloud read + sink + cache + benches all green
```

## Implementation status

Living ledger — populated by inner-loop and phase-end reviews. Empty at planning time.

## Deferred work

Items deferred from inner-loop or phase-end reviews. Each row is fed into subsequent review prompts as "known accepted tradeoff — don't re-flag."

Seeded with carry-forward items from the existing plan's §13 that may surface as should-fix during reviews:

- **`polars-arrow::ffi::ArrowArray::from_ffi_parts(...)` upstream API**: would let polars-vortex drop the `mem::transmute` in `read/array_bridge.rs` + `write/array_bridge.rs`. Deferred: scope is a separate polars upstream contribution; current safety triplet (size+align asserts + runtime length check) is accepted (see `Accepted tradeoffs`).
- **Vortex `Duration` extension dtype** (upstream Vortex addition): would unblock Polars `Duration` pushdown. Deferred to a future Vortex release.
- **`takeable_rows_provider` morsel size**: defaults to IPC's 122,880 rows; Vortex's natural zone block is 8,192. Deferred — low ROI, may surface during Phase 4 benches if measurement shows it matters.
- **~~Workspace `vortex = { path = "..." }` → crates.io version~~**: **RESOLVED in PR-1.1.** Vortex 0.70.0 is published; the migration is in Phase 1 scope, not deferred.
- **Pre-existing visitor feature-gating** in `polars-python::lazyframe::visitor::nodes.rs::scan_type_to_pyobject`: latent (py-polars wheel always enables `json`). Targeted for inclusion in PR-1.2 (Phase 1 polish).
- **In-memory `ScanSourceRef::Buffer` zero-copy** (currently `to_vec()`s): targeted for inclusion in PR-1.2 if low-effort; else deferred.
- **Rust-level tests for the polars-stream Vortex source/sink** (currently only Python): deferred to Phase 3 or follow-up.
- **Hard-cached segment-cache hit count test**: Moka's `Cache` doesn't expose hit/miss stats; would need wrapping in `InstrumentedSegmentCache` from upstream Vortex. Deferred.

## Accepted tradeoffs / r1 traps

Items reviewers may otherwise re-surface but that the user has explicitly accepted. Carried forward across all subsequent review prompts.

- **`mem::transmute` in `read/array_bridge.rs` + `write/array_bridge.rs` + `write/df_to_stream.rs`**: accepted. polars-arrow's `ArrowArray` / `ArrowSchema` FFI structs have `pub(super)` fields — `mem::transmute` is the only way an external crate can construct these from upstream FFI structs. Compile-time `size_of` + `align_of` asserts + runtime length check provide safety. The clean alternative (`from_ffi_parts(...)` upstream API) is a separate polars contribution.
- **Single Tokio runtime (Polars' global `ASYNC`) for Vortex async work**: accepted; the alternative (Vortex spinning its own runtime) doubles thread-pool overhead. `Handle::new(Arc::downgrade(Arc::new(ASYNC.handle()) as Arc<dyn Executor>))` is the established pattern.
- **`SpecializedColumnPredicate` fast path preserved during PR-13 transitional phases (Option B trajectory)**: accepted. Subagent 7 recommended this for de-risking; PR-13.6 deletes the old path once the AExpr-direct path proves itself across PR-13.2–.5 e2e tests.
- **`hashbrown 0.16` (Polars) + `0.17` (Vortex transitive) coexistence**: accepted. BAN against bare `PlHashMap::new()` is the established workaround.
- **~~Workspace `vortex = { path = "..." }` path-dep~~**: **NO LONGER ACCEPTED** — PR-1.1 migrates to crates.io `vortex = "0.70.0"`. (Kept in the audit trail as a strikethrough so reviewers cross-referencing the existing 1,145-line plan understand the change.)
- **`create_skip_batch_predicate = false` for Vortex** (explicit branch at `polars-mem-engine/src/planner/lp.rs:448-459`): accepted. Vortex's `LayoutReader::pruning_evaluation` already does zone-level pruning; Polars' per-row-group skip-predicate machinery would be redundant. Explicit branch makes this intentional, not implicit.
- **Sorting `ColumnPredicates::predicates` by column name before AND-collect** (`read/predicate.rs:44-52`): accepted. Vortex's pruning evaluator short-circuits left-to-right; `PlHashMap` iteration is non-deterministic. Deterministic ordering is a reproducibility guarantee.
- **`row_count: usize::try_from(u64).unwrap_or(usize::MAX)` clamp** at `nodes/io_sources/vortex/mod.rs:221`: accepted. 32-bit platforms with files >2^32 rows are an extreme edge case; clamp to `usize::MAX` rather than truncate.
- **`arrow_schema::Schema` vs `polars_arrow::ArrowSchema` distinction**: accepted. The two crates are nominally distinct but wire-compatible; `vortex_dtype_to_schema` emits polars-arrow types directly.
- **`PolarsInstrumentedVortexReadAt` mandatory wrapping for all read paths**: accepted. Every read goes through it for concurrency-budget + IOMetrics plumbing; bypass is forbidden.

---

## Phase 1.2 subagent reports (audit trail)

The six parallel exploration subagents that seeded this draft. Reports are summarized into the sections above; full reports remain in the conversation history (not duplicated here).

1. **Slot 1 — polars-vortex current state**: validated all four plan claims (test counts, C-ABI bridge safety pattern, mem-engine guard, workspace dep). **No drift.** Inventory: 2,697 LoC across 17 files; 6 `unsafe` blocks all with `// SAFETY:` comments; zero `TODO`/`FIXME`/`todo!()` markers.
2. **Slot 3 — testing surface map**: 73 tests verified. Identified PR-6 gaps (multi-file, schema-evolution policies, nested types, small-ints, engagement asserts, hypothesis tests, pyarrow cross-validation) and PR-14 bench targets.
3. **Slot 4 — Polars file-format prior art**: 5 prior-art lessons (L1 reuse `aexpr_to_column_predicates`; L2 PR-8 leads Parquet on `table_statistics`; L3 IR-plan optimizer pass module pattern; L4 `FileReader::initialize`/`begin_read` split already mirrored; L5 small focused PRs).
4. **Slot 5 — cross-codebase prior art**: vortex-datafusion's `convert/exprs.rs` is the line-for-line precedent. Vortex Expression IR supports all PR-13 targets except temporal extracts (requires Vortex SHA verification). `from_ffi_parts` upstream contribution is feasible but out of scope.
5. **Slot 6 — Polars conventions extraction**: 20 BANS items + 10 style conventions. Workspace `clippy.toml` disallows `HashMap`/`HashSet` and `regex::Regex::new`. `AI_POLICY.md` strict rules. Recurring fix-wave themes: silent integer-cast wraparound, dead options, name collisions, hashmap non-determinism.
6. **Slot 7 — PR-13 design fork**: **Architectural discovery** — AExpr arena gone at reader site; PR-13 convertor must run at `to_graph.rs:843`. Recommendation: Option B (parallel paths) with planned migration to Option A via PR-13.6 cutover. Six sub-PRs: PR-13.1 foundation, PR-13.2 wire+arithmetic, PR-13.3 CAST, PR-13.4 struct, PR-13.5 temporal (gated on Vortex op), PR-13.6 delete fast path.
