# Vortex Integration into Polars — big-plans plan

> Continuation of [spiraldb/polars#1](https://github.com/spiraldb/polars/pull/1) (`vortex-integration`, 31 existing commits, +6,497/-80, 73 tests). big-plans takes over the remaining work — retroactive ratification + CI green-up + PR-13 aggressive AExpr pushdown + PR-8 file-stats + PR-6 multi-file/nested coverage + PR-14 benches — and lands as one squash-merged PR onto `spiraldb:main`.

## Current State

```yaml
status: phase-boundary
branch: vortex-integration
planning_sub_flow: null
current_phase: "Ratify + crates.io transition"
phase_index: 1
current_pr: null
pr_index: 5
outstanding_must_fix: 0
deferred_items_total: 6
last_user_touchpoint: 2026-05-16T15:35:00Z
last_user_touchpoint_what: "PR-1.4 CI-reopen complete (confidence: high, 0 new deferred); advancing to Phase 3.1 (pr_index==phase_pr_count+1)"
subagent_invocations_this_pr: 0
subagent_invocations_total: 16
review_cycles_this_pr: 0
phase_entry_sha: 657c78c97
phase_end_cycle: 3
phase_end_reject_cycles: 0
last_phase_end_verdict: reject
current_pr_is_ci_reopen: null
last_commit: 0677ffcc9
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
- ~~**Cargo invocations** always prefixed with `GIT_CONFIG_GLOBAL=/tmp/clean-home/.gitconfig CARGO_NET_GIT_FETCH_WITH_CLI=false`~~: **RESOLVED 2026-05-15** — the `init-agent-pats` setup provides credentials libgit2 can use; plain `cargo …` invocations now succeed in this worktree. Memory `polars_build_tips.md` updated to reflect the new state.

**Work-shape highest-leverage insight (feature-integration):** **Analogous prior-art** — every new PR-13 shape MUST cite the equivalent in `/Users/will/git/vortex/vortex-datafusion/src/convert/exprs.rs`. If a shape has no DF analogue, that's a flag that it's genuinely novel and deserves extra adversarial review (reviewers should query: is this shape representable at all in Vortex's expression IR?).

## Phases and PRs

### Phase summary

Initial draft from handoff-plan skeleton, refined by Phase 1.2 subagent findings. Per-phase scope finalized by Step 1.4 design-tree interview. Review-counts confirmed by user in Step 1.6.

| Phase | Name | Scope (one line) | Exit criteria (machine-checkable) | PR count | Review-count |
|---|---|---|---|---|---|
| 1 | Ratify + crates.io transition | Retroactive 4-vote gauntlet of cumulative diff vs `main` + path-dep → crates.io migration + cheap polish | (a) 4-vote phase-end review accepts. (b) `cargo check -p polars --features vortex,cloud,parquet,dtype-full` clean. (c) `cargo test -p polars-vortex --features dtype-date,dtype-datetime,dtype-time,dtype-decimal` → at least 66 Rust tests pass. (d) `pytest py-polars/tests/unit/io/test_vortex.py` → at least 10 tests pass. (e) `gh pr checks 1 --repo spiraldb/polars` shows green for Rust + Python core checks. | 4 | **4-vote** |
| 2 | PR-13 aggressive AExpr pushdown | New convertor module (Option B trajectory per user Step 1.4 decision); arithmetic / CAST / struct field access / temporal extracts shipped incrementally; PR-2.6 (final sub-PR) deletes the `SpecializedColumnPredicate` fast path | (a) 4-vote review accepts. (b) Every row in existing plan's §5 pushdown coverage table implemented + tested OR documented as deliberately deferred. (c) New e2e tests verify pushdown engagement via Vortex `Expression::display_tree()` or `POLARS_VERBOSE` log assertion. (d) Build/test suite still green. | 5-6 | **4-vote** |
| 3 | PR-8 file-stats + PR-6 multi-file / nested coverage | Populate `UnifiedScanArgs::table_statistics` from Vortex footer (lead the pattern, no API change); add multi-file scan tests + schema-evolution policy round-trips (`missing_columns`/`extra_columns`/`cast_options`) + nested-type (List/Struct) end-to-end + small-int dtypes (i8/i16/u8/u16) | (a) 4-vote review accepts. (b) Multi-file scan with file-level stats shows whole-file skips (via `EXPLAIN` or Vortex pruning counters). (c) Schema-evolution policy tests pass across the missing/extra/cast matrix. (d) Nested-type roundtrip tests pass. (e) Small-int dtype roundtrip tests pass. (f) Build/test suite green. | 3-4 | **4-vote** |
| 4 | PR-14 benches + final polish + merge prep | Criterion benches (`crates/polars/benches/io_vortex.rs`): cold-cache full scan, filtered scan with column predicates (TPC-H Q6/Q14 style), cloud-read latency, second-run cache-hit ratio, write throughput. Top-level README mention; remaining unblocked deferred items resolved; final 4-vote review on the FULL cumulative diff vs main | (a) 4-vote review accepts. (b) `cargo bench --features vortex --bench io_vortex` compiles + runs. (c) TPC-H Q6/Q14 comparison documented in `crates/polars-vortex/README.md`. (d) `gh pr checks 1 --repo spiraldb/polars` still green. (e) Ready for squash-merge. | 2-3 | **4-vote** |

### PR enumeration

Initial draft. Refined during Step 1.4 + per-PR scope-checks at the start of each Phase 2 inner-loop iteration.

| PR | Phase | Scope (one line) | Files touched (expected) | Acceptance (specific, testable) |
|---|---|---|---|---|
| PR-1.1 | 1 | crates.io transition: replace workspace path-dep with `vortex = "0.70.0"` | `Cargo.toml` (workspace `:118`), `Cargo.lock`. If polars-vortex code uses Vortex internals not in 0.70.0's surface: scope expands to either pin a later released version or adapt polars-vortex. | (a) `cargo check -p polars --features vortex,cloud,parquet,dtype-full` clean. (b) 65 Rust + 8 Python tests pass locally. (c) `gh pr checks 1` shows green on clippy-stable / check-features / test (ubuntu) / Lint Rust / build-rust-docs. |
| PR-1.2 | 1 | Phase 1 polish: `VortexCacheMode` Python surface (`cache_mode=` param), visitor feature-gating fix (actual shape: `serde_json` non-optional in `polars-python/Cargo.toml`, since the `scan_type_to_pyobject` Vortex arm uses `serde_json::to_string` while the dep was `optional`-gated on the `json` feature — see commit `6f0fe06a9`), in-memory `ScanSourceRef::Buffer` zero-copy if low-effort | `polars-python/src/lazyframe/general.rs`, `py-polars/src/polars/io/vortex/functions.py`, `polars-python/Cargo.toml` (serde_json non-optional), `polars-vortex/src/read/read_at.rs` | `cache_mode` Python parameter exposed + tested (including bool/float/u64-overflow rejection paths); visitor cfg-gating fix at the Cargo.toml level; in-memory buffer zero-copy deferred (assessed: not low-effort); CI-greenup absorbed (cargo fmt sweep, ruff, dprint, mypy stubs, clippy approx_constant, deny 0BSD, dsl-schema wiring + hashes regen) |
| PR-1.3 | 1 | Address any must-fix items from Phase 1 retroactive 4-vote gauntlet | varies | Phase 1 end-of-phase review accepts |
| PR-1.4 | 1 | Phase 1 cycle-2 cleanup: thread `VortexScanOptions::segment_cache` through `vortex_file_info`; narrow `pub use ::vortex;` to 5 sub-modules; inline `read::metadata` back-compat shim (one caller update); plan-doc fixes (row-184 stale test counts; producer/writer determinism Deferred entry update) | `crates/polars-plan/src/plans/conversion/dsl_to_ir/scans.rs:300,336,344-345`, `crates/polars-plan/src/dsl/file_scan/mod.rs:21`, `crates/polars-vortex/src/lib.rs:18`, `crates/polars-vortex/src/read/mod.rs:18-22`, `.big-plans/vortex-integration.md` | (a) `cargo check -p polars-stream --features vortex,cloud` clean. (b) 66 Rust + 10 Python tests still pass. (c) Phase 1 end-of-phase review cycle 3 accepts. |
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
cargo check -p polars --features vortex,cloud,parquet,dtype-full

# Rust test suite (any phase)
cargo test -p polars-vortex --features dtype-date,dtype-datetime,dtype-time,dtype-decimal

# Default Polars build (must remain unaffected — Out of scope contract)
cargo check -p polars

# Python smoke
cd py-polars && pytest tests/unit/io/test_vortex.py -v

# Workspace lints (catch convention violations early)
cargo clippy --features vortex,cloud,parquet,dtype-full --workspace -- -D warnings

# CI status (Phase 1 exit criterion; revisited at Phase 4)
gh pr view 1 --repo spiraldb/polars
gh pr checks 1 --repo spiraldb/polars

# Phase 4: benches
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

Living ledger — populated by inner-loop and phase-end reviews.

### PR-1.2: Phase 1 polish + CI green-up (8 PR-work commits, ending at `b2aeb2b8b`)

- **Scope shipped**:
  - **VortexCacheMode Python surface**: `pl.scan_vortex(..., cache_mode=...)` and `pl.read_vortex(..., cache_mode=...)` accept `Literal["global", "off"] | int | None`. Python helper `_resolve_cache_mode` in `py-polars/src/polars/io/vortex/functions.py:186-217` dispatches to a 2-arg pyo3 pair (`cache_mode_kind: &str`, `cache_dedicated_bytes: Option<u64>`); pyo3 `new_from_vortex` at `crates/polars-python/src/lazyframe/general.rs:336-372` matches into the Rust `VortexCacheMode::{Global, Off, Dedicated(N)}` enum. Bool/float/u64-overflow input handling explicit. Resolver return type narrowed to `tuple[Literal["global", "off", "dedicated"], int | None]` post-review.
  - **Visitor cfg-gating fix (root-cause)**: `serde_json` made non-optional in `crates/polars-python/Cargo.toml` (was `optional = true` and only enabled via the `json` feature). Latent bug: visitor `scan_type_to_pyobject` arms for `csv`/`parquet`/`vortex` use `serde_json::to_string` but the dep was only pulled in under `json`. Real fix at the dep layer; visitor source unchanged.
  - **CI green-up absorbed (10+ items)**: ruff (4 lints — missing `_init_credential_provider_builder` import in `sink_vortex`, unused noqa, TC003 type-check import, TRY300 try/else); dprint (`.big-plans/` exclude in `dprint.json`, README `*sink*`→`_sink_` emphasis, README table alignment collapse); cargo fmt sweep across 19 polars-vortex `.rs` files (pre-existing formatting drift); mypy stubs added to `_plr.pyi` (`new_from_vortex`, `sink_vortex`, `set_vortex_cache_bytes`); mypy `redundant-expr` in `_resolve_cache_mode` (bool check restructured); clippy `approx_constant` (`predicate.rs:293` literal `3.14`→`2.5`); cargo deny `0BSD` license added to allow list (for transitive `enum-iterator 2.3.0` from Vortex); dsl-schema feature wiring (`polars-vortex?/dsl-schema` added to `polars-plan/Cargo.toml`) + 4 new hashes in `crates/polars-plan/dsl-schema-hashes.json` (VortexCacheMode/Compression/ScanOptions/WriteOptions).
- **Tests added**: `test_cache_mode_accepts_all_valid_inputs` (4 valid inputs: None / "global" / "off" / positive int) and `test_cache_mode_rejects_invalid_inputs` (5 invalid: 0, -5, "invalid" string, bool True/False, float 1.5, u64 overflow 2**64) in `py-polars/tests/unit/io/test_vortex.py:135-181`. Rust-side new test count unchanged (66 Rust tests).
- **Review**: 2-vote (gauntlet `preset=pr-2`, lenses=fresh+correctness) / **accepted at cycle 2** (cycles: 2). Cycle 1: 1 must-fix (bool coverage), 2 should-fix, 6 nits — disagreement on bool-coverage severity resolved to must-fix per HIGHEST-severity rule. Cycle 2: 0 must-fix, 0 should-fix, 3 nits (all test-quality observations; dismissed).
- **Confidence**: high
- **Deferred items**: 1 (PR-1.2 Rust dispatch tighten + Rust unit test for `new_from_vortex` match arms — cycle-1 should-fix #3, deferred to follow-up PR; defensive arms have no Rust test coverage and `('global'|'off', Some(_))` silently ignore `cache_dedicated_bytes`).
- **Surprises during implementation**:
  - PR-1.2's planned scope was ~3-4 specific polish items; actual scope grew to 10+ items because PR-1.1's crates.io transition unblocked CI which surfaced layered pre-existing failures (ruff, dprint, mypy, clippy, deny, dsl-schema). Each push to CI surfaced the next layer; chained ~5 pushes before all fixes landed. Defensible given Phase 1's stated goal ("Ratify + crates.io transition + cheap polish"), but a future big-plans run should split CI-greenup into its own dedicated PR rather than absorbing into a polish PR.
  - The "visitor feature-gating fix" item in the plan was originally framed as a `visitor/nodes.rs` edit; the actual fix lived at the Cargo.toml level (making `serde_json` non-optional). Plan-vs-reality drift caught by gauntlet cycle 1 nit #8 and corrected in the plan PR-1.2 row.
  - `cargo fmt --check` had to clean up 19 polars-vortex files of pre-existing formatting drift before clippy/clippy-nightly could pass (the fmt step runs first).
  - The deferred should-fix #3 (Rust dispatch tighten) is mitigated by the fact that Python's `_resolve_cache_mode` is the sole caller — but a future Rust integration test or fuzz harness would expose the silent-ignore behavior.

### PR-1.1: crates.io transition (2 PR-work commits, ending at `018f2ce43`)

- **Scope shipped**: workspace `Cargo.toml:115` replaced `vortex = { path = "../../../../vortex/vortex", ... }` with `vortex = { version = "0.70.0", default-features = false, features = ["files", "tokio"] }`. Single API-drift fix: `DType::Union(_)` arm removed from `crates/polars-vortex/src/read/schema.rs` (not present in 0.70.0); `DType::Variant(_)` arm retained (present in 0.70.0). Cargo.lock refreshed: +87 / −31 lines, mechanical version bumps from path-local `0.1.0` to crates.io `0.70.0` for 29 `vortex-*` crates; `smallvec` transitive dep dropped from `vortex-array` and `vortex-sequence`.
- **Tests added**: `variant_dtype_errors_with_clear_message` in `crates/polars-vortex/src/read/schema.rs:243-253` (covers the retained `DType::Variant(_)` bail-arm; resolves gauntlet cycle 1 should-fix #1).
- **Review**: 2-vote (gauntlet `preset=pr-2`, lenses=fresh+correctness) / **accepted** (cycles: 1). 0 must-fix, 2 should-fix, 2 nit. Both reviewers `overall: accept` at `confidence: high`. Full Synthesizer Output JSON in plan-commit `a21aab471` `<details>` block.
- **Confidence**: high
- **Deferred items**: 1 (Python local verification → CI; PR-1.1 criterion (b) subsumed by Phase 1 (e) `gh pr checks 1`).
- **Surprises during implementation**:
  - vortex 0.70.0 doesn't expose `DType::Union` (the local Vortex workspace's `0.1.0` did); fix was a single-arm removal in `schema.rs`. `DType::Variant` is in 0.70.0 — the original PR-1.1 fix incorrectly removed both arms; rolled back to keep `Variant`.
  - The `GIT_CONFIG_GLOBAL=/tmp/clean-home/.gitconfig CARGO_NET_GIT_FETCH_WITH_CLI=false` env-shim is no longer required (user notified 2026-05-15 mid-cycle). Memory `polars_build_tips.md` + plan BAN updated. Plan-commit `79a9ecdbf` strikethrough'd the BAN.

### PR-1.3: Address must-fix items from Phase 1 retroactive 4-vote gauntlet (10 PR-work commits, ending at `7018c4582`)

- **Scope shipped**:
  - **6 phase-end must-fix items from cycle 1** of the Phase 1 4-vote gauntlet, all in pre-existing 31-commit code:
    - **MF-001** [`polars-stream/src/nodes/io_sinks/writers/vortex/mod.rs:135`]: writer task `ASYNC.spawn(...)` now wrapped in `tokio_handle_ext::AbortOnDropHandle` (mirrors IPC sink at `ipc/mod.rs:101`). Prevents silent-truncation when the outer async_executor task fails before reaching the explicit `write_handle.await`.
    - **MF-002** [same file, producer task lines 110-130]: producer-task errors from `dataframe_to_vortex_chunks(&df)` now forwarded through the `mpsc::channel::<VortexResult<VortexArrayRef>>` as `Err(...)` BEFORE the channel is dropped. The writer's `ArrayStreamAdapter` propagates the Err instead of seeing a clean-EOS-then-write-footer signal that would produce a truncated-but-valid Vortex file on disk.
    - **MF-003** [`polars-plan/src/plans/conversion/dsl_to_ir/scans.rs:345`]: `vxf.row_count() as usize` replaced with `usize::try_from(...).unwrap_or(usize::MAX)` per project BAN, matching the established clamp at `polars-stream/src/nodes/io_sources/vortex/mod.rs:217`.
    - **MF-004** [`crates/polars-vortex/README.md:297`]: documented struct-field name `cache` corrected to actual `segment_cache` (rename was intentional per `read/options.rs:26-27` comment to distinguish from the LazyFrame query cache).
    - **MF-005** [`crates/polars-vortex/README.md:325-332`]: `pl.scan_vortex` documented signature now includes the new `cache_mode=` parameter shipped in PR-1.2.
    - **MF-006** [`polars-stream/src/nodes/io_sources/vortex/builder.rs:49`]: `set_io_metrics`'s silent `let _ = self.io_metrics.set(...)` replaced with `.ok().unwrap()` to align with the CSV/IPC/NDJSON 3-of-4 sibling consensus (panic on duplicate `set` surfaces refactor regressions immediately). Cycle 1's first fix was a comment-add; the inner-loop gauntlet flagged that comment as fabricating a "by design" trait contract not supported by the actual single-call site (verified via cycle-2's trace through `multi_scan/mod.rs:185-200` + `pipeline/initialization.rs:366` + `physical_plan/lower_ir.rs:769-775`).
  - **2 inner-loop cycle-1 must-fix items** found during PR-1.3 self-review:
    - `vortex::error::vortex_err!` build break at `vortex/mod.rs:139`: `polars-stream` has no direct `vortex` crate dep — only `polars-vortex`. Cycle-1 verification gap: `cargo check -p polars` (umbrella, without `new_streaming`) didn't activate polars-stream's vortex code; the right check is `-p polars-stream --features vortex,cloud`. Fix at `e1688a567` uses `polars_vortex::vortex::error::vortex_err!` via the `pub use ::vortex;` re-export at `polars-vortex/src/lib.rs:18`.
    - MF-006 comment fabrication (described above; fix at `f408469d3`).
  - **2 inner-loop cycle-1 should-fix items** fixed in PR-1.3:
    - `rbs as usize` BAN class at `crates/polars-vortex/src/write/strategy.rs:26` — same pattern as MF-003 (`usize::try_from(rbs).unwrap_or(usize::MAX)`). Commit `297fb7cf9`.
    - `cargo fmt` import-group ordering at `polars-stream/.../vortex/mod.rs:35` (the `use crate::utils::tokio_handle_ext;` added in MF-001 was mid-group). Commit `7018c4582`.
- **Tests added**: None new. Existing 66 Rust unit+integration tests + 10 Python tests continue to pass; the MF-001/MF-002 guarantees are not yet covered by a regression test (deferred per Deferred work — Rust-level test infrastructure and Python-level test both captured).
- **Review**: 2-vote (gauntlet `preset=pr-2`, lenses=`fresh`+`correctness`) / **accepted at cycle 2** (cycles: 2). Cycle 1: 2 must-fix (vortex_err build break, MF-006 comment fabrication), 4 should-fix (2 fixed-now: rbs clamp / cargo fmt; 2 deferred: producer/writer error race / Python regression test), 3 nits dismissed. Cycle 2: 0 must-fix, 1 should-fix deferred (pre-existing E0004 build break under `polars-stream --features vortex` without `cloud` — predates PR-1.3), 0 nit. Full Synthesizer Output JSON in plan-commit bodies for cycle-1 (`a708bbe12`) and cycle-2 (`400f3e1a6`).
- **Confidence**: high
- **Deferred items**: 3 new in PR-1.3 (Vortex sink producer/writer error-path determinism polish; Python-level regression test for MF-001/MF-002; pre-existing E0004 build break under `vortex`-without-`cloud` combo). Cumulative `deferred_items_total: 6`.
- **Surprises during implementation**:
  - **Cycle-1 MF-006 fix was reward-hacking the silent-discard BAN.** The "fix" (adding a comment explaining why the silent-discard was acceptable) fabricated a "multi-call by design" trait contract that didn't exist. The cycle-2 inner-loop review caught this and routed to a strictly better fix: align with the 3-of-4 sibling consensus via `.ok().unwrap()`. Lesson: when adding a comment to justify a silent-error swallow, check whether the underlying pattern is correct rather than just explaining why the swallow is acceptable.
  - **Cycle-1 MF-002 fix introduced a build break that my verification missed.** The umbrella `cargo check -p polars --features vortex,cloud,parquet,dtype-full` succeeded because that feature combo doesn't include `new_streaming`, so polars-stream's vortex sink code wasn't even compiled. The right verification is `cargo check -p polars-stream --features vortex,cloud`. Process gap; should be added to BAN / verification checklist for future PR-N inner-loops touching polars-stream.
  - **All 6 phase-end must-fix items were in PRE-EXISTING 31-commit code** (not in PR-1.1 or PR-1.2 directly). Phase 1's "retroactive ratification" framing is real — the phase-4 gauntlet's purpose at this phase was specifically to surface latent issues in the pre-existing integration foundation. Verdict: framing worked as designed.
  - **Cycle-2 surfaced a pre-existing build break unrelated to PR-1.3**: `polars-stream --features vortex` (without `cloud`) errors with E0004 on the Vortex sink Writeable match — `Writeable::Cloud(_)` arm is `#[cfg(feature = "cloud")]` but the underlying enum's Cloud variant remains visible because `polars-io`'s `file_cache` feature transitively enables `polars-io/cloud`. CI doesn't catch this combo. Tracked as Deferred work for Phase 2 cleanup or follow-up PR.

### PR-1.4: Phase 1 cycle-2 cleanup (5 PR-work commits, ending at `5c634f26f`)

- **Scope shipped**:
  - **Thread `VortexScanOptions::segment_cache` through `vortex_file_info`** (commit `68f9d5c14`): IR-build-time postscript schema-discovery read now honors the user's `cache_mode='off'` / `'global'` / `Dedicated(N)` choice (was previously hardcoded to the global cache). Mirrors the streaming-source pattern at `io_sources/vortex/mod.rs:138`. Signature change: `vortex_file_info` now takes an extra `segment_cache: Arc<dyn SegmentCache>` parameter; sole caller resolves `options.segment_cache.resolve()` and passes it through.
  - **Narrow `pub use ::vortex;` to 5 sub-modules** (commit `38035b2f0`): `pub mod vortex { pub use ::vortex::{array, error, file, io, layout}; }`. Audit confirmed 11 actual cross-crate `polars_vortex::vortex::*` paths span exactly these 5 sub-modules; the project BAN against new `vortex`-internal symbol use is now machine-checkable.
  - **Inline `read::metadata` back-compat shim** (commit `3479c6bdb`): deleted the 5-line `pub mod metadata { pub use super::VortexFooterRef; }` shim at `read/mod.rs:18-22`. Two callers (`polars-plan/src/dsl/file_scan/mod.rs:21`, `polars-plan/src/plans/conversion/dsl_to_ir/scans.rs:300`) updated to use the canonical `polars_vortex::read::VortexFooterRef` path.
  - **Plan-doc fix on Vortex sink producer/writer error-path determinism** (commit `2eb45441c`): corrected the Deferred work entry per cycle-2 correctness-lens finding (producer-Err path is deterministic per `producer.await?` ordering, not race-y).
  - **Cycle-1-inner-loop must-fix `.clone()` removal** (commit `5c634f26f`): dropped redundant `.clone()` on `VortexCacheMode` (Copy-derived) at `scans.rs:1036`. Triggered `clippy::clone_on_copy` warning which CI's `tools/cargo-fail-warning.py` treats as failure; fix is one-character delta matching the streaming-source prior art at `io_sources/vortex/mod.rs:138`.
- **Tests added**: None new. Existing 66 Rust + 10 Python tests continue to pass. The cycle-1 phase-end coverage gap ("vortex_file_info honors VortexScanOptions::segment_cache") is now CLOSED in source but still ships without a dedicated regression test — deferred per Deferred work entry (same priority bucket as the MF-001/MF-002 Python regression test).
- **Review**: 2-vote (gauntlet `preset=pr-2`, lenses=`fresh`+`correctness`) / **accepted at cycle 2** (cycles: 2). Cycle 1: 1 must-fix (clippy::clone_on_copy CI-blocker), 4 should-fix (all 4 deferred to Phase 2 entry — Dedicated double-resolve perf; missing regression test; long doc-comments; signature-shape architecture nit), 1 nit (dismissed). Cycle 2: 0 findings, both reviewers accept at confidence: high. Full Synthesizer Output JSON in plan-commit bodies for cycle-1 (`24f8f5d4f`) and cycle-2 (`3fc5ee4bd`).
- **Confidence**: high
- **Deferred items**: 4 cycle-1 should-fix items deferred to Phase 2 entry (Dedicated double-resolve; segment_cache regression test; long doc-comments; signature shape). Cumulative `deferred_items_total: 6` (unchanged — the 4 PR-1.4 should-fix items are tracked in cycle-1's synthesizer-output JSON committed to `24f8f5d4f` body, not in the Deferred work section yet; they'll be migrated as part of Phase 2.0 housekeeping if/when that sub-PR runs).
- **Surprises during implementation**:
  - **The cycle-1 phase-end review's top should-fix recommendations all landed cleanly as a focused 4-commit cleanup PR**, validating the cycle-2 phase-end framework: when reviewers consistently surface fixable items at the phase boundary, an explicit "Phase N.4 cleanup" sub-PR is a natural fit rather than absorbing into the next phase's work or carrying as Deferred work indefinitely.
  - **PR-1.4's cycle-1 inner-loop reject (clippy::clone_on_copy)** was a small-cost-high-signal find: both reviewers caught it independently (fresh as nit, correctness as must-fix per the `cargo-fail-warning.py` CI-blocker analysis). Process win: the strict-warning CI setup ensures even nit-level lints get surfaced as gate-blocking; verification checklist should include `cargo clippy -p <touched-crate> --features <features>` in addition to `cargo check`.

#### Re-completion (CI-reopen at phase-boundary, before cycle-3 phase-end review)

PR-1.4 was re-opened at the phase boundary after CI surfaced 2 failures on commit `7eaf4e2fe`: (a) `rustfmt` on `scans.rs:300-303` (multi-line `PolarsResult<(FileInfo, Option<VortexFooterRef>)>` return type) + `read/mod.rs:17` (trailing blank line); (b) `typos` on `MIS-CHARACTERIZES` at plan lines 1197 + 1441. Both were mechanical fixes: `cargo fmt --all` restored the rustfmt-required form, two `MIS-CHARACTERIZES` occurrences replaced with `MISCHARACTERIZES` (correct closed-compound spelling).

- **Scope shipped (CI-reopen)**:
  - **rustfmt restoration** (commit `0a71bc2f4`): collapsed 4-line return-type at `scans.rs:300-303` to a single 77-char line; removed trailing blank line at `read/mod.rs:17`. Mechanical `cargo fmt --all` output.
  - **Typos fix** (commit `f0e99e43f`, bundled with plan re-open): `MIS-CHARACTERIZES` → `MISCHARACTERIZES` at both occurrences in the cycle-2 phase-end review section (narrative prose at `:1197` + JSON-archive blob at `:1441`). Closed-compound spelling; zero semantic drift.
- **Tests added**: None — cosmetic CI fixes, no behavioral change. Existing 66 Rust + 10 Python tests unaffected.
- **Review (CI-reopen iteration)**: 2-vote gauntlet `preset=pr-2` (lenses=`fresh`+`correctness`) over diff range `7eaf4e2fe..HEAD`. **Accepted at cycle 1** (both reviewers `high` confidence). 0 must-fix, 0 should-fix, 3 nits — all meta-process commentary about big-plans skill itself (non-canonical `current_pr_is_ci_reopen` field; `last_commit` anchoring ambiguity; typo-fix verification). None map to BANS / Accepted tradeoffs / Deferred work. All 3 nits dismissed per Step 2.4 nit-handling convention. Full Synthesizer Output JSON in plan-commit `0677ffcc9` body.
- **Confidence**: high
- **Deferred items**: 0 new (cumulative `deferred_items_total: 6` unchanged).
- **Surprises during fix-application**:
  - **The dirty edits the prior session left behind WERE the rustfmt fix** — auto-classifier UI-language ("cosmetic formatter changes") obscured their load-bearing role; the resumption session initially discarded them before checking CI, then had to re-derive via `cargo fmt --all`. Process lesson: at any phase-boundary resume, check `gh pr checks` BEFORE proposing to discard a prior session's uncommitted edits. The same-shape recovery this time was trivial (`cargo fmt` restored byte-for-byte) but the framing mistake is the bug to learn from.

## Resolved phase-end must-fix items — Phase 1: Ratify + crates.io transition — cycle 1

| Severity | File:line | Description | Implicated PR | Resolved |
|----------|-----------|-------------|---------------|----------|
| must-fix | `crates/polars-stream/src/nodes/io_sinks/writers/vortex/mod.rs:135-146` | Vortex sink's `write_handle = ASYNC.spawn(...)` returns a bare tokio::task::JoinHandle, not wrapped in `tokio_handle_ext::AbortOnDropHandle`. Divergent from the IPC sink (ipc/mod.rs:101-112) which DOES wrap. If the ou... | PR-1.3 | [x] |
| must-fix | `crates/polars-stream/src/nodes/io_sinks/writers/vortex/mod.rs:113-128` | Silent data corruption: when `dataframe_to_vortex_chunks(&df)?` returns Err inside the producer task, `?` propagates the error and drops `tx` (the chunk channel). The writer task's `ArrayStreamAdapter` sees channel cl... | PR-1.3 | [x] |
| must-fix | `crates/polars-plan/src/plans/conversion/dsl_to_ir/scans.rs:345` | `let row_count = vxf.row_count() as usize;` violates the explicit project BAN: 'No `as`-cast `u64 → usize` on row counts. Use `usize::try_from(row_count).unwrap_or(usize::MAX)`'. On 32-bit platforms with Vortex files ... | PR-1.3 | [x] |
| must-fix | `crates/polars-vortex/README.md:297` | README documents the Rust struct field as `pub cache: VortexCacheMode` but the actual field is `pub segment_cache: VortexCacheMode` (read/options.rs:28). The rename was intentional (read/options.rs:26-27 comment expla... | PR-1.3 | [x] |
| must-fix | `crates/polars-vortex/README.md:325-332` | The documented `pl.scan_vortex(...)` signature in the README is missing the `cache_mode=` parameter that PR-1.2 shipped — the headline new public-API surface of this phase. README is the canonical reference doc and is... | PR-1.3 | [x] |
| must-fix | `crates/polars-stream/src/nodes/io_sources/vortex/builder.rs:49` | `let _ = self.io_metrics.set(io_metrics);` silently swallows the `Err(io_metrics)` returned by `OnceLock::set` when called twice. Violates the project BAN: 'Silent error swallowing: `let _ = fallible_op()` ... without... | PR-1.3 | [x] |

## Phase 1 raw gauntlet responses (archive)

### Cycle 3 — preset=phase-4 — reject

<details><summary>Full Synthesizer Output JSON (gauntlet schema_version: 1)</summary>

```json
{
  "schema_version": 1,
  "preset": "phase-4",
  "lenses_used": ["spec", "correctness", "maint", "arch"],
  "review_count": 4,
  "unified_findings": [
    {
      "severity": "must-fix",
      "kind": "missing-acceptance",
      "file_line": ".big-plans/vortex-integration.md:404",
      "found_by": ["spec"],
      "description": "PR-1.4 CI-reopen declared scope is 'fix rustfmt + typos CI failures'. The typos-fix narrative at lines 404 and 408 introduces 3 new occurrences of the typos-banned token while explaining the fix, recreating the exact CI failure. gh pr checks 1 shows main (Lint global / Spell Check with Typos) FAIL on current HEAD 1a244a0f7. Phase 1 exit criterion (e) not met.",
      "recommended_fix": "Edit plan lines 404 and 408 to reference the prior typo via a typos-safe construction (rephrase as 'closed-compound form', or wrap each reference in extra backticks/code fence). Verify with local typos . before pushing."
    },
    {
      "severity": "must-fix",
      "kind": "doc-quality",
      "file_line": "crates/polars-vortex/README.md:244",
      "found_by": ["maint"],
      "description": "README crate-layout block describes mod.rs as 'VortexFooterRef alias + back-compat metadata re-export' — but PR-1.4 commit 3479c6bdb inlined the metadata re-export. README is the canonical reference doc and is now factually wrong; a fresh engineer following the README will hit a not-found compile error.",
      "recommended_fix": "Update crates/polars-vortex/README.md:244 to drop the back-compat phrase — e.g., '# VortexFooterRef = Arc<Footer> alias'. Audit the rest of the crate-layout block in one sweep for any other stale references to PR-1.4 inlined/narrowed items."
    },
    {
      "severity": "should-fix",
      "kind": "scope-drift",
      "file_line": ".big-plans/vortex-integration.md:196",
      "found_by": ["spec"],
      "description": "PR-1.4 declared scope: plan-doc fixes (row-184 stale test counts). Only the Phase 1 row was updated. Stale '65 Rust + 8 Python' references remain at lines 77 (layer map), 96 (polars-vortex crate inventory), 196 (PR-1.1 row), 205 (PR-2.6 row).",
      "recommended_fix": "Sweep the plan doc for all '65 Rust' / '8 Python' / '65+8' references and update each to 'at least 66 / at least 10' matching the line-185 fix."
    },
    {
      "severity": "should-fix",
      "kind": "fragmentation",
      "file_line": "crates/polars-plan/src/plans/conversion/dsl_to_ir/scans.rs:293-303 + :1033-1039 (overlaps with crates/polars-vortex/src/read/options.rs:67-74)",
      "found_by": ["maint", "arch"],
      "description": "Dedicated double-resolve: vortex_file_info and the streaming source each call VortexCacheMode::Dedicated(N).resolve() which allocates a fresh Moka cache. Two independent caches per scan; segments fetched at schema-discovery time don't carry into data read.",
      "recommended_fix": "Either (a) thread the resolved cache from vortex_file_info through to the streaming source so one cache backs both reads, or (b) document the dual-cache pattern with rationale + perf tradeoff."
    },
    {
      "severity": "should-fix",
      "kind": "load-bearing-complexity",
      "file_line": "crates/polars-stream/src/nodes/io_sinks/writers/vortex/mod.rs:85-177",
      "found_by": ["arch"],
      "description": "Producer/writer dual-task pattern lacks a consolidated top-of-module rationale comment. The dual-runtime split (producer on async_executor, writer on Tokio ASYNC) is load-bearing for the cross-runtime channel mediation but rationale is scattered.",
      "recommended_fix": "Add a 5-10 line module-doc block explaining: why two tasks, who signals shutdown, channel buffer-depth meaning, where error paths reunite. Reference the Deferred-work producer-error comment trim entry for the inline cleanup."
    },
    {
      "severity": "should-fix",
      "kind": "architecture-vs-plan-drift",
      "file_line": "crates/polars-vortex/src/session.rs:43-49 + .big-plans/vortex-integration.md:108",
      "found_by": ["arch"],
      "description": "Plan spec at line 108 reads 'Handle::new(Arc::downgrade(Arc::new(ASYNC.handle()) as Arc<dyn Executor>))' which would create a Weak<dyn Executor> that IMMEDIATELY dangles. Actual implementation at session.rs:40-48 holds a static LazyLock<Arc<dyn Executor>> so the Weak always resolves. Source is correct; spec is wrong.",
      "recommended_fix": "Update plan line 108 to match session.rs:40-48: 'Handle::new(Arc::downgrade(&EXECUTOR))' where EXECUTOR is a static LazyLock<Arc<dyn Executor>>."
    },
    {
      "severity": "should-fix",
      "kind": "doc-quality",
      "file_line": "crates/polars-vortex/src/lib.rs:14-21",
      "found_by": ["maint"],
      "description": "Narrowing doc-comment claims '8 paths' but enumerates 12 distinct items across 5 sub-modules. Audit miscount or pre-update text.",
      "recommended_fix": "Update the count to 12 (or replace path-by-path enumeration with the invariant 'Anything outside vortex::{array, error, file, io, layout} fails to compile. Re-run the audit before adding a new path.')."
    },
    {
      "severity": "should-fix",
      "kind": "doc-quality",
      "file_line": "crates/polars-plan/src/plans/conversion/dsl_to_ir/scans.rs:293-303",
      "found_by": ["maint"],
      "description": "vortex_file_info has no function-level doc-comment; the fully-qualified type 'std::sync::Arc<dyn polars_vortex::vortex::layout::segments::SegmentCache>' is unwieldy at the signature site.",
      "recommended_fix": "Add /// doc-comment summarizing (1) when called (IR-build-time postscript schema discovery), (2) each parameter's purpose, (3) the segment_cache contract. Introduce a type alias VortexSegmentCacheRef and use it at cross-crate signatures."
    },
    {
      "severity": "should-fix",
      "kind": "name-quality",
      "file_line": "crates/polars-plan/src/plans/conversion/dsl_to_ir/scans.rs:302",
      "found_by": ["maint"],
      "description": "Fully-qualified segment_cache type at signature site (overlaps with the doc-quality finding above; name-quality angle).",
      "recommended_fix": "Same remediation — introduce a type alias and use it at the signature site."
    },
    {
      "severity": "should-fix",
      "kind": "scaffolding",
      "file_line": "crates/polars-vortex/src/read/predicate.rs:1-21",
      "found_by": ["maint"],
      "description": "Missing scaffolding marker on the SpecializedColumnPredicate fast path slated for deletion in PR-2.6. Without a TODO marker, engineers landing PR-2.6 will have to re-derive the deletion scope.",
      "recommended_fix": "Add '// TODO(PR-2.6): remove this SpecializedColumnPredicate fast path once the general predicate-pushdown path lands. Tracked: .big-plans/vortex-integration.md:<row>' comment."
    },
    {
      "severity": "should-fix",
      "kind": "hidden-assumption",
      "file_line": "crates/polars-stream/src/nodes/io_sources/vortex/mod.rs:173-204",
      "found_by": ["maint"],
      "description": "Undocumented behavioral assumption: morsel_rx.recv() returning Err is treated as clean EOS. If upstream changes behavior, silent-swallowing risk.",
      "recommended_fix": "Add a comment documenting the assumption. Optionally add a debug_assert that no error has been queued on the error channel at this point."
    },
    {
      "severity": "should-fix",
      "kind": "doc-quality",
      "file_line": ".big-plans/vortex-integration.md:339",
      "found_by": ["spec"],
      "description": "PR-1.2 impl-status bullet missing RUSTSEC-2024-0436 enumeration (was added to deny.toml in PR-1.2 alongside 0BSD).",
      "recommended_fix": "Add RUSTSEC-2024-0436 to the PR-1.2 impl-status bullet enumeration."
    }
  ],
  "disagreements": [
    {
      "topic": "Overall verdict — 2 reject vs 2 accept",
      "positions": [
        { "lens": "spec", "position": "reject (must-fix: typos recurrence at plan:404)" },
        { "lens": "correctness", "position": "accept (0 findings)" },
        { "lens": "maint", "position": "reject (must-fix: README:244 back-compat phrase outdated)" },
        { "lens": "arch", "position": "accept (0 must-fix, 3 should-fix)" }
      ],
      "synthesizer_call": "reject — conservative-union: ANY must-fix forces reject. Two reviewers found distinct must-fix items at distinct file_lines (plan:404 doc-typos recurrence; README:244 source-of-truth drift). Both are doc-only fixes that are quick to apply."
    }
  ],
  "dropped_re_flags": [
    {
      "topic": "Vortex sink producer-error inline comment trim (mod.rs:127-137)",
      "reason": "covered by Deferred work",
      "reference": "Deferred work:1515"
    }
  ],
  "executive_summary": "Overall verdict: REJECT. Two must-fix items prevent Phase 1 close-out, both doc-only and both mechanical to apply. (1) Spec lens caught a typos-CI regression at .big-plans/vortex-integration.md:404,408 where the narrative explaining the typos fix reintroduces three occurrences of the banned token, recreating the exact CI failure. The cycle-3 inner-loop CI-reopen 2-vote accepted at confidence:high without verifying actual post-push CI state. (2) Maint lens caught crates/polars-vortex/README.md:244 still describing a 'metadata' back-compat re-export shim that PR-1.4 commit 3479c6bdb deleted — fresh engineers will hit a not-found compile error. 2-vs-2 split: spec + maint reject; correctness + arch accept. Synthesizer applies conservative-union. Notable surprises: 4 unfixed stale-test-count locations beyond row 185; EXECUTOR Arc pattern at plan:108 mis-specifies session.rs:43-49; Dedicated double-resolve allocates 2 independent caches per scan (flagged from 2 angles); morsel_rx.recv() Err-as-EOS is undocumented; RUSTSEC-2024-0436 enumeration missing from PR-1.2 impl-status. The producer-error inline comment finding drops to dropped_re_flags (already a Deferred work entry). Counts: 2 must-fix, 10 should-fix, 6 nits. Estimated remediation: ~30 minutes for the 2 must-fix items + reverify CI green.",
  "overall": "reject",
  "must_fix_count": 2,
  "should_fix_count": 10,
  "nit_count": 6,
  "review_cycles_this_invocation": 1
}
```

</details>

## Phase 1: Ratify + crates.io transition — end-of-phase review (cycle 1) — rejected (4-vote)

**Synthesizer output from `/spiral:gauntlet` (`preset=phase-4`, lenses=`spec`+`correctness`+`maint`+`arch`); full Synthesizer Output JSON in the `<details>` block at the end of this section.**

### Executive summary

Phase 1 ('Ratify + crates.io transition') of the polars-vortex integration is the cumulative 31-commit base plus PR-1.1 (path-dep → crates.io `vortex = "0.70.0"`), PR-1.2 (Python `cache_mode=` surface + visitor cfg-gating root-cause fix + CI greenup absorption — 10+ items including ruff/dprint/mypy stubs/cargo fmt sweep across 19 files/cargo deny 0BSD + RUSTSEC-2024-0436/dsl-schema hashes), and the implicit ratification of the four settled architectural moves (mem-engine delegates to streaming; single Tokio runtime via Polars' global ASYNC; C-ABI bridge via mem::transmute with size+align asserts + runtime length check; SpecializedColumnPredicate fast path preserved). Reviewed by 4 lenses (spec, correctness, maint, arch) per the phase-4 preset.

The review surfaces 6 must-fix items concentrated in 4 areas:
(a) silent data-corruption hazards in the PRE-EXISTING streaming sink writer-task lifecycle: `write_handle = ASYNC.spawn(...)` is not wrapped in `AbortOnDropHandle` (divergent from IPC sink which does wrap), and the producer task drops `tx` on error rather than forwarding the Err — both paths produce valid-looking-but-truncated Vortex files on disk;
(b) a `u64 as usize` BAN violation at `dsl_to_ir/scans.rs:345` inconsistent with the BAN-compliant clamp at the matching streaming-source site `io_sources/vortex/mod.rs:217`;
(c) README documentation drift: documented field name `cache` vs actual `segment_cache`, and the documented `scan_vortex` signature missing the new `cache_mode=` parameter (the headline new API surface of this phase);
(d) one silent error swallow `let _ = self.io_metrics.set(...)` at `builder.rs:49` without rationale comment.

Notable surprises also include: `vortex_file_info` at `scans.rs:336` hardcodes the global segment cache for schema discovery (user's `cache_mode='off'` is silently ignored at that read — captured as should-fix); write-side has zero direct unit tests; `time_extension_days_unit_errors` asserts nothing; `set_global_cache_bytes` (public Python API via `set_vortex_cache_bytes`) has zero functional tests and undocumented mid-scan transient-memory semantics.

Key tradeoffs revisited: (1) per-phase review-counts (4/4/4/4) revisit-but-keep — this 4-vote Phase-1 review's bug-find rate justifies the cost retrospectively, but arch's case that Phases 2-3 (focused new work) could use 3-vote is worth re-evaluating at Phase 4 entry. (2) Work shape (feature-integration) revisit-but-keep — the prior-art-alignment insight directly produced correctness finding 1 (AbortOnDropHandle missing because IPC sink does wrap), vindicating the shape; but `pl.read_vortex` vs `pl.scan_vortex` surface is asymmetric vs the `read_parquet`/`scan_parquet` template. (3) Final phase plan (4-phase) revisit-but-keep — boundaries hold; future phases should pre-declare any CI-greenup as a separate sub-PR rather than absorbing.

Verdict split: spec and arch ACCEPT on their own lens scope (no must-fix from their lenses); correctness and maint REJECT (3 + 4 must-fix from their lenses, with 1 overlap). Conservative-union synthesis is REJECT with 6 must-fix items going into PR-1.3, plus 28 should-fix items and 13 nits captured for Phase 2-4 absorption.

### Summary of changes

Phase 1 ratifies the cumulative 31-commit vortex-integration branch and ships two new PRs (PR-1.1 + PR-1.2) plus an absorbed CI-greenup. Organized by concept:

(1) CRATES.IO TRANSITION (PR-1.1, 2 commits, ending `018f2ce43`): workspace Cargo.toml switched from path-dep `vortex = { path = "../../../../vortex/vortex", ... }` to `vortex = { version = "0.70.0", default-features = false, features = ["files", "tokio"] }`. Cargo.lock refreshed. Single API-drift fix in `crates/polars-vortex/src/read/schema.rs`: the `DType::Union(_)` arm was removed (Vortex 0.70.0 doesn't expose it; the local-workspace 0.1.0 did). `DType::Variant(_)` was retained with a clear bail-with-message and a new test `variant_dtype_errors_with_clear_message`. The `GIT_CONFIG_GLOBAL=...` env-shim is no longer required after the path-dep removal. Clean, single-arm transition.

(2) PYTHON CACHE-MODE SURFACE (PR-1.2): `pl.scan_vortex(..., cache_mode=...)` and `pl.read_vortex(..., cache_mode=...)` accept `Literal['global', 'off'] | int | None`. Python helper `_resolve_cache_mode` at `py-polars/src/polars/io/vortex/functions.py:186-217` dispatches to a 2-arg pyo3 pair `(cache_mode_kind: &str, cache_dedicated_bytes: Option<u64>)`; pyo3 `new_from_vortex` at `crates/polars-python/src/lazyframe/general.rs:336-372` matches into the Rust `VortexCacheMode::{Global, Off, Dedicated(N)}` enum. Bool/float/u64-overflow rejection paths are tested. Dispatch mirrors the `parse_parquet_compression` precedent — strong prior-art alignment. The Python resolver's return type was narrowed to `tuple[Literal['global', 'off', 'dedicated'], int | None]` post-review.

(3) VISITOR CFG-GATING ROOT-CAUSE FIX (PR-1.2): `crates/polars-python/Cargo.toml:53` moved `serde_json` from `optional = true` (gated on the `json` feature) to non-optional. The visitor `scan_type_to_pyobject` arms for csv/parquet/vortex use `serde_json::to_string` regardless of the `json` feature. Fix at the dep-declaration layer rather than at the source-edit layer originally planned.

(4) CI GREEN-UP ABSORBED (10+ items in PR-1.2): ruff (4 lints), dprint (.big-plans/ exclude + README emphasis + README table alignment), cargo fmt sweep across 19 polars-vortex files, mypy stubs added to `_plr.pyi`, mypy redundant-expr in `_resolve_cache_mode`, clippy `approx_constant` (predicate.rs:293 literal 3.14 → 2.5), cargo deny `0BSD` license + `RUSTSEC-2024-0436` (paste unmaintained — transitive via Vortex), dsl-schema feature wiring + 4 new hashes in `crates/polars-plan/dsl-schema-hashes.json`. Tests added: `test_cache_mode_accepts_all_valid_inputs` (4 valid inputs) and `test_cache_mode_rejects_invalid_inputs` (5 invalid inputs).

What's at risk (6 must-fix items):
- Two silent-data-corruption hazards in the PRE-EXISTING streaming sink writer-task lifecycle (AbortOnDropHandle missing; producer error not forwarded).
- A `u64 as usize` BAN violation at `dsl_to_ir/scans.rs:345` inconsistent with the matching streaming-source pattern at `io_sources/vortex/mod.rs:217`.
- README documentation drift: field name `cache` vs actual `segment_cache`; documented `scan_vortex` signature missing the new `cache_mode=` parameter.
- One silent error swallow `let _ = self.io_metrics.set(...)` in pre-existing streaming-source builder code.

These 6 items concentrate in 4 areas (streaming-sink lifecycle; row_count cast; README docs; silent error swallow) and are all fixable in a focused PR-1.3.

### Surprises and discoveries

- **Vortex streaming sink's `write_handle = ASYNC.spawn(...)` is NOT wrapped in `tokio_handle_ext::AbortOnDropHandle`, divergent from the IPC sink (ipc/mod.rs:101-112) which DOES wrap. On outer-task failure, the writer task continues, sees the closed chunk channel as clean EOS, and produces a truncated-but-valid-looking Vortex file.**
  - How handled: Not yet — surfaced in this Phase-1 review. Goes into PR-1.3.
  - Amend plan: `yes`
- **Producer task at `vortex/mod.rs:113-128` uses `?` to propagate errors from `dataframe_to_vortex_chunks(&df)`. On Err, `tx` is dropped without sending an Err item; the writer task sees the channel close as clean EOS and writes the Vortex footer on truncated data. The channel type `VortexResult<VortexArrayRef>` CAN carry errors but the producer never uses the Err path.**
  - How handled: Not yet — surfaced in this Phase-1 review. Goes into PR-1.3.
  - Amend plan: `yes`
- **`crates/polars-plan/src/plans/conversion/dsl_to_ir/scans.rs:345` uses `vxf.row_count() as usize` while the matching site at `crates/polars-stream/src/nodes/io_sources/vortex/mod.rs:217` correctly uses the BAN-compliant `usize::try_from(...).unwrap_or(usize::MAX)`. Inconsistent treatment of the row-count clamp across the two sites that read `vxf.row_count()`.**
  - How handled: Not yet — surfaced in this Phase-1 review. Goes into PR-1.3.
  - Amend plan: `yes`
- **README documents `pub cache: VortexCacheMode` but the actual field is `pub segment_cache: VortexCacheMode`. Rename was intentional (collision avoidance with `ScanArgsVortex::cache: bool`) but the canonical reference doc lags.**
  - How handled: Not yet — surfaced in this Phase-1 review. Goes into PR-1.3.
  - Amend plan: `yes`
- **README's documented Python `scan_vortex(...)` signature is missing the new `cache_mode=` parameter that is the headline new public-API surface of PR-1.2.**
  - How handled: Not yet — surfaced in this Phase-1 review. Goes into PR-1.3.
  - Amend plan: `yes`
- **`crates/polars-stream/src/nodes/io_sources/vortex/builder.rs:49` does `let _ = self.io_metrics.set(io_metrics);` — silently swallows the `Err(io_metrics)` returned by `OnceLock::set` on double-init, with no comment.**
  - How handled: Not yet — surfaced in this Phase-1 review. Goes into PR-1.3.
  - Amend plan: `yes`
- **`vortex_file_info` at `dsl_to_ir/scans.rs:336` hardcodes `session::segment_cache()` for schema discovery, ignoring `VortexScanOptions::segment_cache`. Users passing `cache_mode='off'` still hit the global cache during the postscript read. Streaming-source path correctly threads the option through.**
  - How handled: Not yet — surfaced in this Phase-1 review.
  - Amend plan: `yes`
- **Write-side (`crates/polars-vortex/src/write/{array_bridge,df_to_stream,strategy,sink_writer,writer}.rs`) has zero direct unit tests — only the roundtrip integration catches regressions.**
  - How handled: Not yet — surfaced in this review.
  - Amend plan: `no`
- **Test `time_extension_days_unit_errors` in `schema.rs:429-441` asserts nothing — constructs a Nanoseconds Time, discards it, exits. Anti-help test.**
  - How handled: Not yet — surfaced in this review.
  - Amend plan: `no`
- **PR-1.2's planned scope was ~3-4 polish items; actual scope grew to 10+ items because PR-1.1's crates.io transition unblocked CI which surfaced layered pre-existing failures.**
  - How handled: Absorbed into PR-1.2; impl-status self-flags. Plan-row PR-1.2 retroactively amended.
  - Amend plan: `already-done`
- **The 'visitor feature-gating fix' was originally framed as a `visitor/nodes.rs` source edit; actual fix lives at the Cargo.toml level (`serde_json` non-optional).**
  - How handled: Plan-row corrected at commit `c42274a6a`.
  - Amend plan: `already-done`
- **Vortex 0.70.0 has `DType::Variant` (retained with bail-arm) but lacks `DType::Union` (which the local-workspace 0.1.0 had).**
  - How handled: Single-arm removal; new test for Variant bail.
  - Amend plan: `already-done`
- **Phase 1 exit criteria say '65 Rust tests' / '8 Python tests' but actual is 66 / 10 (PR-1.1 added 1 Rust test; PR-1.2 added 2 Python tests).**
  - How handled: Impl-status records the correct counts; phase-row text wasn't updated.
  - Amend plan: `yes`
- **Deny.toml gained an undocumented `RUSTSEC-2024-0436` (paste) ignore alongside the planned 0BSD addition.**
  - How handled: Commit `9dbb23c62` added the ignore; plan PR-1.2 row didn't enumerate.
  - Amend plan: `yes`
- **In-memory `ScanSourceRef::Buffer` zero-copy assessed and properly deferred (not-low-effort).**
  - How handled: Plan-commit `2c9abdf4e`.
  - Amend plan: `already-done`
- **`set_global_cache_bytes` is a public Python API (via `set_vortex_cache_bytes`) with zero functional tests. Concurrent invocation against in-flight scans has undocumented transient-memory semantics.**
  - How handled: Not yet — surfaced in this review.
  - Amend plan: `no`
- **`pl.read_vortex` is missing parameters that `pl.scan_vortex` exposes. Prior-art-alignment with `pl.read_parquet` / `pl.scan_parquet` is partial.**
  - How handled: Not yet — surfaced in this review.
  - Amend plan: `no`
- **Scaffolding markers are largely absent (predicate.rs is scheduled for PR-2.6 deletion but no inline marker; read/mod.rs back-compat shim has no deletion date; write/mod.rs:4 references PR-10 with no link).**
  - How handled: Not yet — surfaced in this review.
  - Amend plan: `no`


### Testing coverage assessment

**Tested cases:**

| Case | Test location | Confidence |
|------|---------------|------------|
| `DType::Variant` produces clear bail error | `crates/polars-vortex/src/read/schema.rs:243-258` | high |
| `cache_mode=None`/'global'/'off'/positive int accepted | `py-polars/tests/unit/io/test_vortex.py:135-148` | high |
| `cache_mode=0`/`-5` raise ValueError | `py-polars/tests/unit/io/test_vortex.py:157-160` | high |
| `cache_mode='invalid'` raises TypeError | `py-polars/tests/unit/io/test_vortex.py:163-164` | high |
| `cache_mode=True/False` raise TypeError (bool guard) | `py-polars/tests/unit/io/test_vortex.py:170-173` | high |
| `cache_mode=1.5` raises TypeError | `py-polars/tests/unit/io/test_vortex.py:176-177` | high |
| `cache_mode=2**64` raises OverflowError | `py-polars/tests/unit/io/test_vortex.py:181-182` | high |
| VortexCacheMode::Global/Off/Dedicated resolve semantics | `crates/polars-vortex/src/read/options.rs:81-131` | high |
| C-ABI bridge size_of + align_of asserts (both directions) + runtime length parity | `crates/polars-vortex/src/read/array_bridge.rs:143-144,175 + write/array_bridge.rs:40-43` | high |
| Predicate convertor for Equal/Between/EqualOneOf/StartsWith/EndsWith/RegexMatch and primitives/temporal/decimal | `crates/polars-vortex/src/read/predicate.rs:286-500` | high |
| Decimal precision/scale overflow + negative-scale rejection | `crates/polars-vortex/src/read/predicate.rs:391-398 + schema.rs:311-323` | high |
| DType→ArrowDataType mapping for all supported dtypes | `crates/polars-vortex/src/read/schema.rs:178-505` | high |
| Roundtrip read/write for primitives, nullable, utf8, binary, datetime+tz, decimal, lists, structs | `crates/polars-vortex/tests/roundtrip.rs` | high |
| Python read/write roundtrip + filter / projection-pushdown / negative-slice | `py-polars/tests/unit/io/test_vortex.py:55-132` | high |
| `PolarsInstrumentedVortexReadAt` forwards reads + records IOMetrics | `crates/polars-vortex/src/read/read_at.rs:221-280` | medium |

**Untested cases (priority-ranked):**

| Case | Priority | Why untested |
|------|----------|--------------|
| Vortex streaming sink producer-error silent-truncation (the must-fix bug at mod.rs:113-128) | high | No Rust-level streaming-sink tests; no Python test asserts file completeness or non-existence on producer-error paths. The two must-fix concurrency bugs would not be caught by any existing test. |
| Vortex sink writer-task abort propagation when outer task fails (must-fix at mod.rs:135-146) | high | Same Rust-test gap. |
| Vortex sink with non-bridgeable column type | high | Natural integration test to catch the must-fix silent-truncation bug — none exists. |
| `set_global_cache_bytes` functional behavior (Arc-swap semantics + transient-memory) | high | Public Python API with zero test scaffolding. |
| Concurrent `set_global_cache_bytes` + in-flight scan (old-cache lifetime) | high | No race-test scaffolding; semantics undocumented. |
| `vortex_file_info` honors `VortexScanOptions::segment_cache` (currently doesn't — should-fix arch finding) | high | Discovered during this review. |
| `as usize` cast at scans.rs:345 on a >2^32-row file (32-bit truncation) | medium | 32-bit platform tests not run in CI. |
| Rust-level `new_from_vortex` dispatch arms (`('global', None)`, `('off', None)`, `('dedicated', Some(N))`, defensive `('dedicated', None)` + `(other, _)`) | medium | Deferred per plan; pyo3 dispatch is sole entry-point from Python. |
| Visitor cfg-gating regression test (vortex built without `json` feature) | medium | No feature-matrix CI step added. |
| Empty DataFrame (0-row, 0-column) write/read round-trip | medium | `dataframe_to_vortex_chunks` has a 0-chunk early-return path that is uncovered. |
| Categorical/Enum → UTF-8 write fallback (README claim at line 374-375) | medium | README documents the behavior but no test verifies it. |
| Write-side `polars_chunk_to_upstream_record_batch` field-metadata preservation in isolation | medium | Write-side unit tests entirely missing. |
| Multi-file scan with mixed local + cloud sources + shared cache | medium | Deferred to Phase 3. |
| Rust-level streaming source/sink integration tests (general) | medium | Deferred to Phase 3. |
| set_vortex_cache_bytes(True), (1.5), (-1) Python-side rejection (inconsistent with cache_mode validator) | low | No validation exists in Python at this site. |
| Hard cache hit/miss counters | low | Moka's Cache doesn't expose stats. Deferred. |
| In-memory Buffer zero-copy semantics (scans.rs:324, io_sources/vortex/mod.rs:95) | low | Deferred per 'if low-effort' clause; not-low-effort assessment recorded. |

**Recommendations:** Highest-leverage additions for PR-1.3 (Phase 1 must-fix fixup PR): (1) a Rust integration test for the streaming Vortex sink simulating producer error — this single test would catch both must-fix concurrency bugs once they're fixed; (2) a unit test for `vortex_file_info` honoring the user's `segment_cache` choice (covers the should-fix arch finding); (3) a unit test for `set_global_cache_bytes` Arc-swap semantics; (4) replace the assertion-free `time_extension_days_unit_errors` test; (5) empty-DataFrame round-trip. Defer to Phase 3: multi-file scan, broader Rust streaming-sink coverage, Categorical/Enum fallback. Defer to Phase 4: 32-bit platform regression tests, deny.toml audit at merge prep.


### Tradeoffs re-evaluation

| Decision | Original choice | Verdict | Rationale |
|----------|-----------------|---------|----------|
| Reuse existing 31 commits vs rewrite from scratch | Reuse — 73 tests passing locally; two prior gauntlet review passes already surfaced + fixed substantive bugs | `keep` | All 4 reviewers agree. The 4-vote Phase 1 review IS the verification gate for the reused base, and it surfaced 6 must-fix items concentrated in pre-existing code — exactly the value the gate is meant to deliver. A rewrite would have burned weeks; the must-fix list is bounded and fixable in PR-1.3. |
| Work shape — feature-integration | feature-integration; Adding Vortex into existing Polars systems with many touch points. Highest-leverage insight: Analogous prior art. | `revisit-but-keep` | Most-pessimistic across lenses. Spec/correctness/arch say keep; maint flags partial prior-art-alignment realization. The cache_mode dispatch IS aligned with `parse_parquet_compression`; the `read_vortex` / `scan_vortex` surface is NOT aligned with `read_parquet` / `scan_parquet` (asymmetric parameter list). Correctness's first finding (AbortOnDropHandle missing because IPC sink does wrap) is ALSO produced by the analogous-prior-art insight — vindicating the work shape. Keep, but close the read/scan-surface asymmetry in Phase 3. |
| CI green-up approach — crates.io `vortex = "0.70.0"` | Replace workspace path-dep with version = '0.70.0'. Strictly better than git-rev/CI-clone/feature-gating: cleanest dep, no release blocker, version-pinned. | `keep` | All 4 reviewers agree. PR-1.1 transitioned cleanly with a single API-drift fix (DType::Union removal). The transitive arrow-* major version coupling (polars-vortex pinned to `arrow-* = "58"` to match Vortex's transitive) deserves an upgrade-runbook comment (captured as should-fix), but the decision itself is sound. |
| Final phase plan — 4 phases as drafted | (1) Ratify + crates.io transition, (2) PR-13 AExpr pushdown, (3) PR-8 file-stats + PR-6 multi-file/nested, (4) PR-14 benches + final polish. | `revisit-but-keep` | Most-pessimistic across lenses. Spec/arch want a scoping lesson captured; correctness/maint say keep. The 4-phase structure should hold; Phase 2 entry should pre-declare any CI-greenup as a separate sub-PR rather than absorbing it into a polish PR mid-phase. Phase 1's actual scope grew 3x but the boundary is clean — the lesson is about sub-PR pre-declaration, not about the phase split. |
| PR-13 architecture — Option B → A via PR-2.6 cutover | Parallel paths in PR-2.2/.3/.4/.5; PR-2.6 deletes the SpecializedColumnPredicate fast path. | `keep` | All 4 reviewers agree. Strategy is mechanically tractable; SpecializedColumnPredicate extraction is contained to a single file (predicate.rs). Captured a should-fix scaffolding-marker finding (predicate.rs needs an inline 'scheduled for PR-2.6 deletion' marker) but the decision itself stands. |
| PR-8 leads Parquet on `table_statistics` | Lead the pattern, no API change (Phase 3 scope) | `keep` | 3 of 4 reviewers say keep; maint says `no-longer-applicable` (out of scope to evaluate from this phase). Per most-pessimistic enum ordering, `no-longer-applicable` is treated as a non-verdict (lens can't evaluate) and falls behind `keep` from the other 3, so the synthesized verdict is `keep`. No architectural blocker for Phase 3 to populate `UnifiedScanArgs::table_statistics` from the Vortex footer. |
| Per-phase review-counts — 4 / 4 / 4 / 4 | All four phase-end reviews use the 4-vote phase-4 preset. Max thoroughness. | `revisit-but-keep` | Most-pessimistic across lenses. Spec/correctness/maint say keep (citing this 4-vote review's bug-find rate as direct validation); arch says revisit-but-keep (4-vote may be over-thorough for Phases 2 and 3, which are focused new work rather than cumulative artifacts). The current 4-vote Phase 1 review found 6 must-fix items that prior 2-vote per-PR reviews did not surface — that's the empirical case for keeping the cost. But arch's framing has merit: revisit at Phase 4 entry whether Phases 2 and 3 actually need 4-vote or whether 3-vote would have caught the same items. |
| Single Tokio runtime (Polars' global `ASYNC`) for Vortex async work | Avoid doubling thread-pool overhead | `keep` | Unchanged by Phase 1. Sound; session.rs implements correctly via Handle::new(Arc::downgrade(Arc::new(ASYNC.handle()) as Arc<dyn Executor>)). |
| `mem::transmute` between Arrow FFI structs with size+align asserts + runtime length check | Compile-time asserts + runtime length check provide safety; the clean alternative (from_ffi_parts upstream API) is a separate polars contribution | `keep` | Phase 1 didn't touch the transmute call-sites. SAFETY comments are well-written; the should-fix arch finding on read/array_bridge.rs:126 is about the comment LENGTH violating the BAN spirit, not the underlying approach. Compress the comment but keep the approach. |
| `SpecializedColumnPredicate` fast path preserved during PR-13 transitional phases (Option B) | Option B trajectory: parallel path first, delete fast path last | `keep` | Phase 2 scope; unchanged. |
| `create_skip_batch_predicate = false` for Vortex | Vortex's pruning_evaluation handles it; explicit branch at polars-mem-engine/src/planner/lp.rs:448-459 | `keep` | Branch is present and well-documented at lp.rs:448-459. Phase 1 didn't touch this. |
| `hashbrown 0.16` (Polars) + `0.17` (Vortex transitive) coexistence | BAN against bare PlHashMap::new() is the established workaround | `keep` | Phase 1 BAN compliance verified across the diff. |
| `PolarsInstrumentedVortexReadAt` mandatory wrapping | Local/cloud/in-memory all route through it | `keep` | Verified at all three sites; Phase 1 didn't change the wrapping. |
| `row_count: usize::try_from(u64).unwrap_or(usize::MAX)` clamp | Accepted: 32-bit edge case clamp at nodes/io_sources/vortex/mod.rs:221 | `revisit-but-keep` | The clamp DECISION is correct at the streaming-source site. Application is INCONSISTENT: the matching IR-build-time site at dsl_to_ir/scans.rs:345 uses `as usize` directly — a BAN violation captured as must-fix in the unified findings. Keep the decision (clamp pattern); revise by applying it consistently to both row-count read sites in PR-1.3. |
| Sorting `ColumnPredicates::predicates` by column name before AND-collect | Deterministic ordering is a reproducibility guarantee | `keep` | Confirmed at predicate.rs:50. Phase 1 didn't change this. |


### Disagreements

- **Topic:** Overall verdict (accept vs reject)
  - `spec`: accept — zero must-fix from the spec lens; PR-1.1 + PR-1.2 ship declared scope plus documented CI-greenup absorption.
  - `correctness`: reject — 3 must-fix (2 silent-data-corruption hazards in pre-existing streaming sink writer-task lifecycle; 1 BAN violation on row_count cast).
  - `maint`: reject — 4 must-fix (1 BAN violation overlapping with correctness; 2 README doc-drift; 1 silent OnceLock::set error).
  - `arch`: accept — architectural moves are coherent; the issues found are all should-fix quality concerns at the arch lens, not blocking.
  - **Synthesizer call:** reject — conservative-union semantics. Two of four reviewers reject on their own lens scope; the unified must-fix set is 6 items after dedupe, which is well above any 'accept with caveats' threshold. The two accept lenses (spec, arch) both surface 4+ should-fix items each, so the gap between their verdict and the synthesized reject is narrow — they accept because the issues they personally found are not blockers, not because the issues correctness+maint found are wrong. The phase-1-retroactive-ratification framing is: 4-vote phase-end review is the verification gate, it surfaced 6 must-fix items, those items now go into PR-1.3.
- **Topic:** Per-phase review-counts tradeoff (4/4/4/4 vs reducing to 3-vote for Phases 2-3)
  - `spec`: keep — current 4-vote Phase 1 gauntlet is canonical validation.
  - `correctness`: keep — cost justified by bug-find rate (this 4-vote review found 3 must-fix items that prior 2-vote per-PR reviews did not surface, including 2 silent-data-corruption bugs).
  - `maint`: keep — this maintainability review found 4 must-fix items earlier 2-vote per-PR reviews missed.
  - `arch`: revisit-but-keep — 4-vote appropriate for Phases 1 and 4 (cumulative architecture); for Phases 2 and 3 (focused new work) 4-vote may be over-thorough. Consider 3-vote; not a strong recommendation.
  - **Synthesizer call:** revisit-but-keep — most-pessimistic enum ordering wins, and arch's framing has merit: the 4-vote at Phase 1 paid off precisely because the artifact was the cumulative 31-commit base + 2 new PRs, where many lenses' coverage compounds. Phases 2 and 3 will deliver narrower, more-bounded artifacts (PR-13 AExpr convertor; PR-8 table_statistics + multi-file). 3-vote for those phases is defensible. But this is a Phase 4 decision — for now the plan stays 4/4/4/4.
- **Topic:** Work shape tradeoff (feature-integration prior-art alignment)
  - `spec`: keep — cache_mode dispatch mirrors `parse_parquet_compression` precedent.
  - `correctness`: keep — analogous prior art insight directly produced finding 1 (AbortOnDropHandle missing because IPC sink does wrap).
  - `maint`: revisit-but-keep — prior-art alignment isn't fully realized: `pl.read_vortex` is missing parameters present in `pl.read_parquet`; README's Python API surface needs updating; `segment_cache` field naming choice needs cross-referenced docs.
  - `arch`: keep — touch points are minimal; each is a discrete branch or single-file insertion.
  - **Synthesizer call:** revisit-but-keep — most-pessimistic enum ordering wins. Maint's framing is sharp: the work shape is correct, but the prior-art alignment is partial. The cache_mode dispatch IS aligned with `parse_parquet_compression`; the read_vortex / scan_vortex surface is NOT aligned with read_parquet / scan_parquet. Phase 1.3 or Phase 3 should close the read/scan surface asymmetry.
- **Topic:** Final phase plan tradeoff (4-phase split)
  - `spec`: revisit-but-keep — Phase 1 expanded substantially; subsequent phases should pre-budget for cross-cutting CI-greenup.
  - `correctness`: keep — phase boundaries remain coherent.
  - `maint`: keep — Phase 1's actual scope grew but the boundary is clean.
  - `arch`: revisit-but-keep — Phase 1's scope crept; one-off due to crates.io unblocking pre-existing CI failures. Keep 4-phase split, note scoping lesson.
  - **Synthesizer call:** revisit-but-keep — half the reviewers want a scoping lesson captured even though all four agree the 4-phase structure should hold. Phase 2 PR-13 entry should pre-declare any CI-greenup as a separate sub-PR (sub-1.2 sibling) rather than absorbing it.


### Dropped re-flags (carry-forward items reviewers re-surfaced)

- **In-memory ScanSourceRef::Buffer zero-copy at scans.rs:324 (`buf.as_slice().to_vec()`)** — reason: covered by Deferred work; reference: `Deferred work:147 — `In-memory ScanSourceRef::Buffer zero-copy (dsl_to_ir/scans.rs:323 does buf.as_slice().to_vec()): considered in PR-1.2, deferred as not-low-effort.``
- **Rust dispatch tighten for new_from_vortex match arms (correctness's recommendation that includes the 'tighten the arms' ask)** — reason: covered by Deferred work (tighten part); the coverage half is kept as a unified should-fix; reference: `Deferred work:151 — `PR-1.2 Rust dispatch tighten + Rust unit test (crates/polars-python/src/lazyframe/general.rs:334-359): pyo3 new_from_vortex match silently ignores cache_dedicated_bytes for ('global', _) / ('off', _) arms. Defensive ('dedicated', None) and (other, _) arms are unreachable from Python's _resolve_cache_mode and have no Rust test coverage. Deferred to a follow-up PR.``
- **No Rust-level streaming source/sink tests at all (the 'general framing' re-flag — distinct from the producer-error specific test that the unified should-fix retains)** — reason: covered by Deferred work; reference: `Deferred work:148 — `Rust-level tests for the polars-stream Vortex source/sink (currently only Python): deferred to Phase 3.``
- **mem::transmute SAFETY comment length at write/array_bridge.rs (maint nit on lines 160-164, framed as 'well-written but borderline')** — reason: covered by Accepted tradeoffs; reference: `Accepted tradeoffs:132 — `mem::transmute in read/array_bridge.rs + write/array_bridge.rs + write/df_to_stream.rs: accepted ... Compile-time size_of + align_of asserts + runtime length check provide safety.` Note: the arch finding at read/array_bridge.rs:126 is RETAINED as a should-fix because it's a different concern (the SAFETY-comment-length BAN, not the underlying mem::transmute approach).`
- **Hard cache hit/miss counters (Moka doesn't expose stats)** — reason: covered by Deferred work; reference: `Deferred work:149 — `Hard-cached segment-cache hit count test: Moka's Cache doesn't expose hit/miss stats. Deferred.``


<details><summary>Full Synthesizer Output JSON (gauntlet schema_version: 1, 69KB)</summary>

```json
{
  "schema_version": 1,
  "preset": "phase-4",
  "lenses_used": ["spec", "correctness", "maint", "arch"],
  "review_count": 4,
  "review_cycles_this_invocation": 1,
  "prior_cycle_dropped_re_flags": [],
  "unified_findings": [
    {
      "severity": "must-fix",
      "kind": "concurrency",
      "file_line": "crates/polars-stream/src/nodes/io_sinks/writers/vortex/mod.rs:135-146",
      "description": "Vortex sink's `write_handle = ASYNC.spawn(...)` returns a bare tokio::task::JoinHandle, not wrapped in `tokio_handle_ext::AbortOnDropHandle`. Divergent from the IPC sink (ipc/mod.rs:101-112) which DOES wrap. If the outer task fails or is dropped before `write_handle.await`, the writer task is NOT cancelled — it continues, sees the closed chunk channel as EOS, finalizes the Vortex footer, and produces a valid-but-truncated file on disk.",
      "recommended_fix": "Wrap with `polars_utils::async_utils::tokio_handle_ext::AbortOnDropHandle(ASYNC.spawn(...))` matching the IPC sink pattern; unwrap accordingly at the await site so the underlying task is aborted on outer-task failure.",
      "found_by": ["correctness"]
    },
    {
      "severity": "must-fix",
      "kind": "bug",
      "file_line": "crates/polars-stream/src/nodes/io_sinks/writers/vortex/mod.rs:113-128",
      "description": "Silent data corruption: when `dataframe_to_vortex_chunks(&df)?` returns Err inside the producer task, `?` propagates the error and drops `tx` (the chunk channel). The writer task's `ArrayStreamAdapter` sees channel close as clean end-of-stream, writes the Vortex footer, and produces a valid-looking but truncated file. The channel type is `VortexResult<VortexArrayRef>` so errors CAN be forwarded — the producer just never uses the Err path.",
      "recommended_fix": "Before returning Err from the producer task, send `tx.send(Err(vortex_err)).await` so the writer task observes the error item, aborts mid-stream, and does NOT finalize the footer. Combined with the AbortOnDropHandle fix, this closes the silent-truncation hole on both producer-error and outer-task-failure paths.",
      "found_by": ["correctness"]
    },
    {
      "severity": "must-fix",
      "kind": "overflow",
      "file_line": "crates/polars-plan/src/plans/conversion/dsl_to_ir/scans.rs:345",
      "description": "`let row_count = vxf.row_count() as usize;` violates the explicit project BAN: 'No `as`-cast `u64 → usize` on row counts. Use `usize::try_from(row_count).unwrap_or(usize::MAX)`'. On 32-bit platforms with Vortex files >2^32 rows, this silently truncates, propagating into `FileInfo::row_estimation` (line 358) and breaking downstream negative-slice / known-size logic. The matching streaming-source site at `crates/polars-stream/src/nodes/io_sources/vortex/mod.rs:217` already uses the BAN-compliant `try_from(...).unwrap_or(usize::MAX)` pattern; the IR-build-time path here is inconsistent.",
      "recommended_fix": "Replace with `let row_count = usize::try_from(vxf.row_count()).unwrap_or(usize::MAX);` to match the established pattern at `nodes/io_sources/vortex/mod.rs:217` and satisfy the documented Accepted-tradeoff invariant. Both row-count read sites must use the same clamp pattern.",
      "found_by": ["correctness", "maint", "arch"]
    },
    {
      "severity": "must-fix",
      "kind": "doc-quality",
      "file_line": "crates/polars-vortex/README.md:297",
      "description": "README documents the Rust struct field as `pub cache: VortexCacheMode` but the actual field is `pub segment_cache: VortexCacheMode` (read/options.rs:28). The rename was intentional (read/options.rs:26-27 comment explains it's to avoid collision with `ScanArgsVortex::cache: bool`), but the canonical reference README still shows the old name. Public-API doc drift; fresh engineers reading the README will be misled.",
      "recommended_fix": "Change README line 297 to `pub segment_cache: VortexCacheMode,`. Optionally add the cross-reference from options.rs:26-27 explaining why the field is named `segment_cache` and not `cache` (collision avoidance with `ScanArgsVortex::cache: bool`).",
      "found_by": ["maint"]
    },
    {
      "severity": "must-fix",
      "kind": "doc-quality",
      "file_line": "crates/polars-vortex/README.md:325-332",
      "description": "The documented `pl.scan_vortex(...)` signature in the README is missing the `cache_mode=` parameter that PR-1.2 shipped — the headline new public-API surface of this phase. README is the canonical reference doc and is now factually wrong about Phase-1's user-facing scope.",
      "recommended_fix": "Add `cache_mode=None` (typed `Literal['global', 'off'] | int | None`) to the documented signature with a brief description, and add a pointer to `_resolve_cache_mode` for the dispatch contract. Verify the documented signature matches `py-polars/src/polars/io/vortex/functions.py:80-99`.",
      "found_by": ["maint"]
    },
    {
      "severity": "must-fix",
      "kind": "error-path",
      "file_line": "crates/polars-stream/src/nodes/io_sources/vortex/builder.rs:49",
      "description": "`let _ = self.io_metrics.set(io_metrics);` silently swallows the `Err(io_metrics)` returned by `OnceLock::set` when called twice. Violates the project BAN: 'Silent error swallowing: `let _ = fallible_op()` ... without an explicit comment explaining why the error is acceptable.'",
      "recommended_fix": "Either (a) add a comment explaining why double-init is acceptable (e.g., `// set_io_metrics may be called once per builder lifecycle; subsequent calls are no-op by design`), or (b) replace with `debug_assert!(self.io_metrics.set(io_metrics).is_ok(), \"io_metrics initialized twice\");` to surface duplicate initialization in debug builds.",
      "found_by": ["maint"]
    },
    {
      "severity": "should-fix",
      "kind": "architecture",
      "file_line": "crates/polars-plan/src/plans/conversion/dsl_to_ir/scans.rs:336",
      "description": "`vortex_file_info` hardcodes `session::segment_cache()` for the postscript schema-discovery read, ignoring `VortexScanOptions::segment_cache`. User passing `cache_mode='off'` still hits the global cache during schema discovery — the cache opt-out is partial. The matching streaming source path at `io_sources/vortex/mod.rs:138` correctly threads `options.segment_cache.resolve()` through.",
      "recommended_fix": "Thread the resolved segment cache through `vortex_file_info` — change the signature to accept the cache mode (or `&VortexScanOptions`) and call `.with_segment_cache(options.segment_cache.resolve())` at line 336. Mirror the streaming-source pattern.",
      "found_by": ["arch"]
    },
    {
      "severity": "should-fix",
      "kind": "overflow",
      "file_line": "crates/polars-vortex/src/write/strategy.rs:26",
      "description": "`strategy_builder.with_row_block_size(rbs as usize)` casts `u64 → usize` unguarded. On 32-bit platforms with `row_block_size > usize::MAX` (>4 GiB block size), this silently truncates — same BAN principle as the row_count cast, applied to a write-time configuration knob.",
      "recommended_fix": "Use `usize::try_from(rbs).map_err(|_| polars_err!(ComputeError: \"row_block_size {} exceeds usize::MAX on this platform\", rbs))?` so misconfiguration errors loudly. Unlike the row_count case (where clamping is the accepted tradeoff because the value is observed from a file), a misconfigured row_block_size should error.",
      "found_by": ["correctness"]
    },
    {
      "severity": "should-fix",
      "kind": "layering",
      "file_line": "crates/polars-vortex/src/lib.rs:18",
      "description": "`pub use ::vortex;` re-exports the entire Vortex API surface through `polars_vortex::vortex`. The documented justification covers a narrow use case but the re-export is total — exposes the full upstream Vortex namespace to any downstream consumer of polars-vortex, blurring the layering boundary that the BAN against 'arrow-array/arrow-schema pub types leaking out' is meant to enforce.",
      "recommended_fix": "Replace with a narrowed module that re-exports only what downstream actually needs (e.g., `pub mod vortex { pub use ::vortex::file::Footer; pub use ::vortex::io::VortexReadAt; ... }`). Audit current call sites to enumerate the actual usage surface before narrowing.",
      "found_by": ["arch"]
    },
    {
      "severity": "should-fix",
      "kind": "doc-quality",
      "file_line": "crates/polars-vortex/src/read/array_bridge.rs:126",
      "description": "The SAFETY comment for the release-callback handoff is 120+ chars and reasons about cross-crate drop behavior; precisely the shape the project BAN against 'justification-comment reward-hacking: any `// SAFETY:` >100 chars explaining why a hack is OK' flags. While the underlying `mem::transmute` approach is an Accepted Tradeoff, the comment length itself violates the spirit of the BAN.",
      "recommended_fix": "Compress to 1-2 lines pointing at the Arrow C ABI spec section that governs release-callback ownership; move the detailed cross-crate reasoning into the module-level doc-comment where it's natural prose, not a SAFETY justification.",
      "found_by": ["arch"]
    },
    {
      "severity": "should-fix",
      "kind": "scope-creep",
      "file_line": ".big-plans/vortex-integration.md:215",
      "description": "PR-1.2 scope grew from ~3-4 declared polish items to 10+ items (ruff, dprint, mypy stubs, clippy approx_constant, cargo deny 0BSD + paste/RUSTSEC-2024-0436, dsl-schema wiring + 4 new hashes, cargo fmt sweep across 19 files, lazyframe F821 fix). Plan-row was retroactively amended; impl-status self-flags 'a future big-plans run should split CI-greenup into its own dedicated PR'.",
      "recommended_fix": "Accept the absorbed expansion (already documented), but capture as a process lesson — future Phase 1 polish PRs should pre-declare 'CI-greenup' as a separate sub-PR with its own bounded scope, especially when transitioning from path-dep to crates.io (which is exactly the kind of move that surfaces latent CI failures).",
      "found_by": ["spec"]
    },
    {
      "severity": "should-fix",
      "kind": "scope-creep",
      "file_line": "deny.toml:24",
      "description": "`RUSTSEC-2024-0436` (paste unmaintained, transitive via vortex 0.70.0) was added to the deny.toml ignore list but NOT enumerated in plan PR-1.2 row's 'CI greenup absorbed' bullet list (which listed `0BSD` license only). Defensible as CI-greenup but the plan-row record is incomplete.",
      "recommended_fix": "Update the plan's PR-1.2 row to enumerate `RUSTSEC-2024-0436 (paste unmaintained)` alongside `cargo deny 0BSD` so future readers see the full set of deny.toml mutations introduced by this phase. Phase 4 polish step should audit the cumulative deny.toml additions before merge prep.",
      "found_by": ["spec", "arch"]
    },
    {
      "severity": "should-fix",
      "kind": "weak-exit-criteria",
      "file_line": ".big-plans/vortex-integration.md:184",
      "description": "Phase 1 exit criterion (c) reads '→ 65 Rust tests pass' but actual is 66 (PR-1.1 added `variant_dtype_errors_with_clear_message`). Criterion (d) reads '8 Python tests pass' but actual is 10 (PR-1.2 added 2 `cache_mode` tests). Strict reading makes the criterion over-met / stale.",
      "recommended_fix": "Either (1) reframe criteria as 'at least N tests pass' (more durable across phases) or (2) update the numeric counts to 66 / 10 to match shipped state. The plan-row counts currently diverge from the impl-status counts noted at line 337.",
      "found_by": ["spec"]
    },
    {
      "severity": "should-fix",
      "kind": "coverage",
      "file_line": "crates/polars-python/src/lazyframe/general.rs:336-372",
      "description": "Pyo3 `new_from_vortex` dispatch defensive arms `('dedicated', None)` and `(other, _)` are unreachable from Python's `_resolve_cache_mode` and have no Rust test coverage. Additionally, `('global', _)` and `('off', _)` arms silently ignore `cache_dedicated_bytes` rather than erroring on a stray non-None value — a future non-Python Rust caller could pass `('global', Some(N))` and the N would be discarded silently.",
      "recommended_fix": "Add a Rust `#[test]` exercising all four arms directly. Optionally tighten the silent-ignore arms — e.g., `('global', None) | ('off', None) => ...` and explicit `('global'|'off', Some(_)) => bail!(...)`. Both the tighten and the test are on the existing deferred-work list; this finding is the coverage half.",
      "found_by": ["spec", "correctness", "maint"]
    },
    {
      "severity": "should-fix",
      "kind": "coverage",
      "file_line": "py-polars/tests/unit/io/test_vortex.py:135-181",
      "description": "Cache_mode rejection-path coverage gaps: missing tests for `cache_mode=b'global'` (bytes), `cache_mode='Global'` (case-sensitivity), `cache_mode=[]` / dict (container types), `cache_mode=2**64 - 1` (max u64 boundary that should succeed), `cache_mode=2**63` (i64 boundary).",
      "recommended_fix": "Add bytes-literal, case-sensitivity, and non-int-container rejection cases to `test_cache_mode_rejects_invalid_inputs`. Add `cache_mode=2**64 - 1` to `test_cache_mode_accepts_all_valid_inputs` to nail the boundary.",
      "found_by": ["correctness"]
    },
    {
      "severity": "should-fix",
      "kind": "coverage",
      "file_line": "crates/polars-vortex/src/session.rs:62",
      "description": "Public Python API `set_vortex_cache_bytes` / `set_global_cache_bytes` has zero functional tests. The Arc-clone + RwLock swap semantics are public contract but uncovered. Concurrent `set_global_cache_bytes(bytes)` against an in-flight scan that holds an `Arc` to the OLD cache leaves both old and new caches in memory until the in-flight scan completes — unbounded transient memory during frequent reconfiguration is undocumented.",
      "recommended_fix": "Add a unit test in `session.rs` confirming swap semantics, Arc-clone safety, and (if practical) the old-cache lifetime. Also document the transient-memory consideration in the `set_global_cache_bytes` doc-comment.",
      "found_by": ["correctness", "maint"]
    },
    {
      "severity": "should-fix",
      "kind": "doc-quality",
      "file_line": "crates/polars-vortex/src/session.rs:62-72",
      "description": "`set_global_cache_bytes` doc-comment fails to document thread-safety, mid-scan safety, the transient-memory implication of the swap (old cache lives until in-flight scans drop their Arc), and the `POLARS_VORTEX_CACHE_BYTES` env-var interaction (parse failure silently falls back to 512 MiB).",
      "recommended_fix": "Expand the doc-comment to cover: thread-safety guarantees, mid-scan safety (or lack thereof), old-cache lifetime/memory, env-var default precedence, and behavior on env-var parse failure (currently silent fallback to default — consider logging a warning).",
      "found_by": ["correctness", "maint"]
    },
    {
      "severity": "should-fix",
      "kind": "boundary",
      "file_line": "py-polars/src/polars/io/vortex/functions.py:217-239",
      "description": "`set_vortex_cache_bytes(byte_budget: int)` does no Python-side validation. `set_vortex_cache_bytes(True)` silently sets cache to 1 byte (`int(True) == 1`), `set_vortex_cache_bytes(1.5)` raises TypeError at pyo3 boundary, `set_vortex_cache_bytes(-1)` raises OverflowError. `_resolve_cache_mode` applies bool/float/negative hygiene; `set_vortex_cache_bytes` doesn't — inconsistent surface.",
      "recommended_fix": "Apply the same bool/float/negative rejection as `_resolve_cache_mode`: raise TypeError for bool and float, ValueError for negative ints, before calling the pyo3 entry point. Mirror the resolver's normalization pattern.",
      "found_by": ["correctness"]
    },
    {
      "severity": "should-fix",
      "kind": "doc-quality",
      "file_line": "py-polars/src/polars/io/vortex/functions.py:86-99",
      "description": "`cache_mode=0` raises ValueError, but `set_vortex_cache_bytes(0)` disables the global cache. Two distinct meanings for `0` is a documentation footgun.",
      "recommended_fix": "Add to the `cache_mode` docstring: 'Note: unlike `set_vortex_cache_bytes(0)` which disables the global cache, `cache_mode=0` is rejected — pass `cache_mode=\"off\"` to disable caching for this scan.'",
      "found_by": ["maint"]
    },
    {
      "severity": "should-fix",
      "kind": "convention",
      "file_line": "crates/polars-stream/src/nodes/io_sources/vortex/mod.rs:173",
      "description": "Production-path code uses `.expect(\"initialized\")` four times (lines 173, 183, 191, 204), violating project BAN: 'No `.unwrap()` / `.expect(...)` in production paths. Test code only.' These are gated by a separately-required `initialize()` call but the BAN is strict.",
      "recommended_fix": "Either (a) refactor to a type-state pattern (separate Uninitialized/Initialized types so the method only exists post-init), or (b) replace each `.expect(...)` with `polars_err!(InvalidOperation: \"VortexFileReader method called before initialize()\")?`. (a) is the more durable fix.",
      "found_by": ["maint"]
    },
    {
      "severity": "should-fix",
      "kind": "encapsulation",
      "file_line": "crates/polars-python/src/lazyframe/general.rs:351-365",
      "description": "The match arms `(\"global\", _)` and `(\"off\", _)` silently discard `cache_dedicated_bytes` if a non-Python Rust caller passes a non-None value. No inline comment documents the invariant that Python's `_resolve_cache_mode` is the only caller and always passes None when kind != 'dedicated'.",
      "recommended_fix": "Add an inline comment: `// Invariant: Python's _resolve_cache_mode is the only caller; it always passes None for cache_dedicated_bytes when kind != 'dedicated'.` Or use `debug_assert!(cache_dedicated_bytes.is_none(), \"...\")` to make the invariant load-bearing in debug builds.",
      "found_by": ["maint"]
    },
    {
      "severity": "should-fix",
      "kind": "scaffolding",
      "file_line": "crates/polars-vortex/src/read/predicate.rs:1",
      "description": "Per the plan, the entire `SpecializedColumnPredicate` fast path in this file is scheduled for deletion in PR-2.6 (after PR-13's AExpr-direct convertor lands). No scaffolding marker in the file; a future engineer reading it cold has no idea this is intentional dead-code-in-waiting.",
      "recommended_fix": "Add a module-level comment: `// SCAFFOLDING: this entire file is the PR-13 transitional fast path; scheduled for deletion in PR-2.6 once aexpr_predicate.rs replaces it. See .big-plans/vortex-integration.md PR-2.6.`",
      "found_by": ["maint"]
    },
    {
      "severity": "should-fix",
      "kind": "scaffolding",
      "file_line": "crates/polars-vortex/src/read/mod.rs:18-22",
      "description": "`pub mod metadata { pub use super::VortexFooterRef; }` is a back-compat shim with no deletion date or enumeration of downstream consumers. Scaffolding-marker convention used elsewhere in Polars is to name the call sites that hold the shim alive.",
      "recommended_fix": "Either inline the shim (if call sites are now empty) or annotate: `// SCAFFOLDING: pre-PR-X reference path; remove once <list specific call sites that still import metadata::VortexFooterRef>`.",
      "found_by": ["maint"]
    },
    {
      "severity": "should-fix",
      "kind": "coverage",
      "file_line": "crates/polars-vortex/src/write/array_bridge.rs:1",
      "description": "Write-side C-ABI bridge (`polars_array_to_upstream`, `polars_chunk_to_upstream_record_batch`) has zero unit tests. If the metadata-preservation or field-by-field schema construction regresses in isolation, only the full roundtrip catches it — and there's no Rust streaming-sink integration test either.",
      "recommended_fix": "Add a `#[cfg(test)] mod tests` in `write/array_bridge.rs` covering: (1) round-trip a single column with field metadata, (2) column count mismatch error path, (3) field-name preservation across the bridge.",
      "found_by": ["maint"]
    },
    {
      "severity": "should-fix",
      "kind": "coverage",
      "file_line": "crates/polars-vortex/README.md:374-375",
      "description": "README claims '`Categorical` / `Enum` doesn't have a direct Vortex representation; writes fall back to UTF-8.' No test verifies this fallback — the documented behavior is untested.",
      "recommended_fix": "Add a Python or Rust test that writes a DataFrame with a Categorical column and asserts the read-back column is Utf8 (with the original string values preserved). Mirror for Enum.",
      "found_by": ["maint"]
    },
    {
      "severity": "should-fix",
      "kind": "coverage",
      "file_line": "crates/polars-vortex/src/read/schema.rs:429-441",
      "description": "Test `time_extension_days_unit_errors` asserts nothing — it constructs a Nanoseconds Time, discards it, and exits. A test that doesn't test anything is anti-help: it inflates coverage metrics, gives false confidence, and obscures what the original author meant to verify.",
      "recommended_fix": "Either (a) remove the test and move the explanation into a module-level comment, or (b) rewrite it to construct the unreachable arm via a manual `DType::Extension` build and assert the bail message. Don't leave assertion-free tests.",
      "found_by": ["maint"]
    },
    {
      "severity": "should-fix",
      "kind": "coverage",
      "file_line": "crates/polars-stream/src/nodes/io_sinks/writers/vortex/mod.rs",
      "description": "No Rust-level integration test for the streaming Vortex sink simulating producer error. The two must-fix concurrency bugs (AbortOnDropHandle missing + producer error not forwarded) would not be caught by any existing test — Python-level tests only cover happy paths, and there is no Rust streaming-sink test at all.",
      "recommended_fix": "Add at least one integration test that simulates producer error (e.g., a DataFrame column that fails C-ABI bridging or a deliberate Err from `dataframe_to_vortex_chunks`) and asserts the resulting file does NOT exist or is detectably invalid. This test should fail before the must-fix fixes land and pass after.",
      "found_by": ["correctness"]
    },
    {
      "severity": "should-fix",
      "kind": "coverage",
      "file_line": "py-polars/tests/unit/io/test_vortex.py",
      "description": "Python coverage gaps: (1) empty DataFrame (0-row, 0-column) write/read round-trip (the `dataframe_to_vortex_chunks` has a 0-chunk early-return path), (2) multi-file scan, (3) explicit `cache_mode='off'` after 'global' for cache isolation, (4) Vortex sink with producer-error simulation (would catch the silent-truncation must-fix), (5) `n_rows=0` boundary, (6) glob path resolution.",
      "recommended_fix": "Prioritize (1) empty-DF and (4) producer-error simulation in PR-1.3 (the Phase 1 must-fix fixup PR). (2) and (3) can land in Phase 3 per the plan.",
      "found_by": ["correctness"]
    },
    {
      "severity": "should-fix",
      "kind": "error-path",
      "file_line": "crates/polars-vortex/src/read/array_bridge.rs:175",
      "description": "`if imported.len() as i64 != expected_len` casts `usize → i64` unguarded. For an array length exceeding i64::MAX (~9.2 EB items, theoretical), this wraps to negative and the comparison silently masks the bug-detection signal. Same concern at `write/array_bridge.rs:45,64`.",
      "recommended_fix": "Use `i64::try_from(imported.len()).map_err(|_| polars_err!(ComputeError: \"imported array length {} exceeds i64\", imported.len()))?` so the parity check fails loudly on the (extreme) overflow case rather than wrapping.",
      "found_by": ["correctness"]
    },
    {
      "severity": "should-fix",
      "kind": "doc-quality",
      "file_line": "crates/polars-vortex/src/read/array_bridge.rs:62-77",
      "description": "The debug-only field-name parity check uses `.unwrap()` on `polars_schema.get_at_index(col_idx)` at line 67. Release builds skip the check entirely — there's no comment explaining the debug-only choice or the tradeoff.",
      "recommended_fix": "Document the tradeoff explicitly (`// Debug-only: per-batch cost not justified for release builds.`) or promote to a release check if the per-batch cost is acceptable. Either way, the unwrap-in-debug-only-path warrants a one-line comment.",
      "found_by": ["maint"]
    },
    {
      "severity": "should-fix",
      "kind": "doc-quality",
      "file_line": "crates/polars-vortex/src/write/mod.rs:4",
      "description": "Comment `FileWriteFormat::Vortex variant — landed in PR-10.` references a PR number with no link to upstream or repo. Future readers won't know which PR-10 (this branch's or upstream Polars's).",
      "recommended_fix": "Remove the PR reference, or link to the upstream PR URL (e.g., `landed in PR-10 of the vortex-integration branch: https://github.com/spiraldb/polars/pull/N`).",
      "found_by": ["maint"]
    },
    {
      "severity": "should-fix",
      "kind": "convention",
      "file_line": "crates/polars-stream/src/nodes/io_sources/vortex/mod.rs:152",
      "description": "`vxf.dtype().to_arrow_schema()` is called here without an inline comment justifying its use, while the project BAN forbids `to_arrow_schema()` for polars-arrow consumption: 'No `vortex::dtype::DType::to_arrow_schema()` — returns upstream `arrow_schema::Schema`, not polars-arrow's `ArrowSchema`. Use `polars_vortex::read::schema::vortex_dtype_to_schema`.' At this call site `to_arrow_schema()` IS correct (the consumer wants upstream arrow_schema for a Vortex API boundary) but it's not obvious from the code.",
      "recommended_fix": "Add comment: `// NOTE: to_arrow_schema() returns upstream arrow_schema::Schema; we want that here because <reason>. For polars-arrow consumption use vortex_dtype_to_schema — BAN: do not mix these.`",
      "found_by": ["maint"]
    },
    {
      "severity": "should-fix",
      "kind": "doc-quality",
      "file_line": "crates/polars-vortex/Cargo.toml:33-35",
      "description": "Direct deps `arrow-array = \"58\"`, `arrow-data = \"58\"`, `arrow-schema = \"58\"` are pinned to a major version that must match Vortex's transitive arrow-* major version. No upgrade-runbook comment; a Vortex bump that changes its arrow-* major could silently introduce a duplicate-crate compile failure or layout divergence.",
      "recommended_fix": "Add a comment: `# When upgrading Vortex: verify these match Vortex's arrow-* dep in the next-version's Cargo.lock. The mem::transmute size+align asserts catch layout divergence at compile time, but a major-version mismatch may duplicate crates and break the bridge.`",
      "found_by": ["maint"]
    },
    {
      "severity": "should-fix",
      "kind": "boundary",
      "file_line": "py-polars/src/polars/io/vortex/functions.py:242-276",
      "description": "`pl.read_vortex` is missing many parameters that `pl.scan_vortex` exposes. The docstring 'See scan_vortex for the full parameter list' is misleading — the surface area is asymmetric, breaking the prior-art alignment with `pl.read_parquet` / `pl.scan_parquet`.",
      "recommended_fix": "Either (a) accept `**kwargs` and forward to `scan_vortex` (with collect()), or (b) explicitly list the supported subset in the docstring with a note about which scan_vortex args are not supported and why. (a) is the prior-art-aligned choice.",
      "found_by": ["maint"]
    },
    {
      "severity": "nit",
      "kind": "convention",
      "file_line": "crates/polars-vortex/src/write/array_bridge.rs:107",
      "description": "Write path's `polars_chunk_to_upstream_record_batch` builds Field-by-Field; read path's `record_batch_to_dataframe` transmutes whole arrays. Two 'mirror' bridges operate at different granularities.",
      "recommended_fix": "Add a one-line comment explaining the asymmetry: upstream's `RecordBatch::try_new` validates schema-array consistency so the write path must hand-build schemas, whereas the read path can transmute the whole record batch in one shot.",
      "found_by": ["arch"]
    },
    {
      "severity": "nit",
      "kind": "convention",
      "file_line": "crates/polars-vortex/src/read/predicate.rs:25",
      "description": "Vortex symbol-path inconsistency: `vortex::array::scalar::Scalar` vs `vortex::scalar::DecimalValue`. Both work; readability suffers from the inconsistent path style.",
      "recommended_fix": "Pick one path style and use consistently — preferred is `vortex::scalar::*` (shorter and more canonical).",
      "found_by": ["arch"]
    },
    {
      "severity": "nit",
      "kind": "architecture",
      "file_line": "crates/polars-stream/src/nodes/io_sources/vortex/mod.rs:130",
      "description": "`Arc::unwrap_or_clone(footer)` to pass an owned `Footer` to `with_footer`. Idiomatic but slightly noisy; a `with_footer_arc` variant upstream would be cleaner.",
      "recommended_fix": "Consider `with_footer(footer.as_ref().clone())` for symmetry, or leave as-is and open a Vortex issue requesting `with_footer_arc`. Low-priority polish.",
      "found_by": ["arch"]
    },
    {
      "severity": "nit",
      "kind": "architecture",
      "file_line": "crates/polars-vortex/src/write/options.rs:21",
      "description": "`include_dtype: false` is a write-time footgun: produces files unreadable without out-of-band schema. No safety net or warning at the call site.",
      "recommended_fix": "Either document the footgun more clearly in the doc-comment, or rename to `embed_dtype` and require explicit opt-in for the dtype-less mode (rather than opt-out for the dtype-included mode).",
      "found_by": ["arch"]
    },
    {
      "severity": "nit",
      "kind": "perf",
      "file_line": "crates/polars-plan/src/plans/conversion/dsl_to_ir/scans.rs:324",
      "description": "In-memory `ScanSourceRef::Buffer` case allocates via `buf.as_slice().to_vec()` and the parallel allocation at `io_sources/vortex/mod.rs:95`. The plan's Deferred-work entry explicitly acknowledges this but the call site has no `// PERF:` marker.",
      "recommended_fix": "Add inline `// PERF: in-memory ScanSourceRef::Buffer zero-copy is deferred — see .big-plans/vortex-integration.md Deferred work. Modest ROI: cloud/local paths are already zero-copy.` at both call sites.",
      "found_by": ["maint"]
    },
    {
      "severity": "nit",
      "kind": "doc-quality",
      "file_line": "crates/polars-vortex/src/read/options.rs:43-59",
      "description": "`VortexCacheMode::Global` is `#[default]` but the doc-comment doesn't note the multi-tenant implication: cached segments are shared across queries in the same process — fine for single-tenant workloads, possibly surprising for multi-tenant servers.",
      "recommended_fix": "Add a sentence to the `VortexCacheMode` doc-comment noting that `Global` is the default and explaining the multi-tenant consideration (e.g., 'Set `cache_mode=\"off\"` for multi-tenant workloads where you do not want cross-query segment cache sharing').",
      "found_by": ["correctness"]
    },
    {
      "severity": "nit",
      "kind": "doc-quality",
      "file_line": "crates/polars-vortex/src/read/options.rs:26-28",
      "description": "The doc-comment on `segment_cache` correctly explains the naming choice but doesn't reference the `cache: bool` field on `ScanArgsVortex` that prompted the rename.",
      "recommended_fix": "Add pointer in the doc-comment: `Named 'segment_cache' (not 'cache') to avoid collision with the LazyFrame query cache (see ScanArgsVortex::cache: bool).` Closes the loop between the two related fields.",
      "found_by": ["maint"]
    },
    {
      "severity": "nit",
      "kind": "doc-quality",
      "file_line": "py-polars/tests/unit/io/test_vortex.py:138-141",
      "description": "Test docstring buries the test intent ('the cache_mode dispatch is a perf knob — all four modes must return identical data') in parens after a 'Moka doesn't expose hit/miss stats' note.",
      "recommended_fix": "Lead the docstring with intent: `The cache_mode dispatch is a perf knob — all four modes must return identical data. (Note: Moka doesn't expose hit/miss stats, so we test value equality rather than cache behavior.)`",
      "found_by": ["maint"]
    },
    {
      "severity": "nit",
      "kind": "doc-quality",
      "file_line": "crates/polars-vortex/src/session.rs:11",
      "description": "`POLARS_VORTEX_CACHE_BYTES` env var has no documentation on what happens if it's set to a non-numeric value (`parse::<u64>().ok()` silently falls back to 512 MiB).",
      "recommended_fix": "Either log a warning on parse failure (preferable — users debugging cache sizing will notice the fallback) or document explicitly: `// If POLARS_VORTEX_CACHE_BYTES is set but not a valid u64, falls back to 512 MiB silently.`",
      "found_by": ["maint"]
    },
    {
      "severity": "nit",
      "kind": "doc-quality",
      "file_line": "crates/polars-vortex/src/read/predicate.rs:48",
      "description": "The tuple destructure uses `_` for the unused `PhysicalIoExpr` parameter. When PR-13 lands, this `_` becomes the canonical AExpr-direct convertor input — the inline name would document the type.",
      "recommended_fix": "Replace `_` with `_physical_expr` so the parameter type is documented inline. Reads better in the PR-13 transition window.",
      "found_by": ["maint"]
    },
    {
      "severity": "nit",
      "kind": "cross-cutting",
      "file_line": "crates/polars-plan/src/dsl/builder_dsl.rs:12",
      "description": "The cargo fmt sweep across 19 polars-vortex files + 4 polars-plan/polars-stream files in commit `69dad1e1c` is mechanically benign but materially expands the file-touched count (33 in PR-1.2 vs. 4 declared). Each formatting-only file is hard to distinguish from semantic changes during review.",
      "recommended_fix": "For future big-plans phase boundaries: run `cargo fmt --check` as part of phase-entry validation so style drift accumulates in one place (or in a dedicated mechanical-formatting PR) rather than being absorbed into a polish PR mid-phase.",
      "found_by": ["spec"]
    },
    {
      "severity": "nit",
      "kind": "convention",
      "file_line": "crates/polars-vortex/src/write/array_bridge.rs:80",
      "description": "`#[allow(clippy::disallowed_types)]` on `polars_chunk_to_upstream_record_batch` is justified by upstream `arrow_schema::Field::with_metadata` requiring `std::collections::HashMap`. The allow-with-comment is sound; flagged only for review-completeness as the canonical pattern.",
      "recommended_fix": "No fix — well-handled. Worth tracking as the canonical boundary pattern for future upstream-API friction.",
      "found_by": ["spec"]
    },
    {
      "severity": "nit",
      "kind": "scope-creep",
      "file_line": "deny.toml:23",
      "description": "Phase 1 added `RUSTSEC-2024-0436` (paste unmaintained) and `0BSD` license to the deny.toml ignore set, both introduced by transitive Vortex 0.70.0 deps. Per-phase, the deny.toml grows — a trend worth tracking before merge.",
      "recommended_fix": "Track in Phase 4 polish: audit deny.toml additions at merge prep, file upstream Vortex issues to dlete the dependencies (paste is unmaintained per RUSTSEC-2024-0436; 0BSD license is a permissive but uncommon license requiring per-org policy approval).",
      "found_by": ["arch"]
    }
  ],
  "disagreements": [
    {
      "topic": "Overall verdict (accept vs reject)",
      "positions": [
        {"lens": "spec", "position": "accept — zero must-fix from the spec lens; PR-1.1 + PR-1.2 ship declared scope plus documented CI-greenup absorption."},
        {"lens": "correctness", "position": "reject — 3 must-fix (2 silent-data-corruption hazards in pre-existing streaming sink writer-task lifecycle; 1 BAN violation on row_count cast)."},
        {"lens": "maint", "position": "reject — 4 must-fix (1 BAN violation overlapping with correctness; 2 README doc-drift; 1 silent OnceLock::set error)."},
        {"lens": "arch", "position": "accept — architectural moves are coherent; the issues found are all should-fix quality concerns at the arch lens, not blocking."}
      ],
      "synthesizer_call": "reject — conservative-union semantics. Two of four reviewers reject on their own lens scope; the unified must-fix set is 6 items after dedupe, which is well above any 'accept with caveats' threshold. The two accept lenses (spec, arch) both surface 4+ should-fix items each, so the gap between their verdict and the synthesized reject is narrow — they accept because the issues they personally found are not blockers, not because the issues correctness+maint found are wrong. The phase-1-retroactive-ratification framing is: 4-vote phase-end review is the verification gate, it surfaced 6 must-fix items, those items now go into PR-1.3."
    },
    {
      "topic": "Per-phase review-counts tradeoff (4/4/4/4 vs reducing to 3-vote for Phases 2-3)",
      "positions": [
        {"lens": "spec", "position": "keep — current 4-vote Phase 1 gauntlet is canonical validation."},
        {"lens": "correctness", "position": "keep — cost justified by bug-find rate (this 4-vote review found 3 must-fix items that prior 2-vote per-PR reviews did not surface, including 2 silent-data-corruption bugs)."},
        {"lens": "maint", "position": "keep — this maintainability review found 4 must-fix items earlier 2-vote per-PR reviews missed."},
        {"lens": "arch", "position": "revisit-but-keep — 4-vote appropriate for Phases 1 and 4 (cumulative architecture); for Phases 2 and 3 (focused new work) 4-vote may be over-thorough. Consider 3-vote; not a strong recommendation."}
      ],
      "synthesizer_call": "revisit-but-keep — most-pessimistic enum ordering wins, and arch's framing has merit: the 4-vote at Phase 1 paid off precisely because the artifact was the cumulative 31-commit base + 2 new PRs, where many lenses' coverage compounds. Phases 2 and 3 will deliver narrower, more-bounded artifacts (PR-13 AExpr convertor; PR-8 table_statistics + multi-file). 3-vote for those phases is defensible. But this is a Phase 4 decision — for now the plan stays 4/4/4/4."
    },
    {
      "topic": "Work shape tradeoff (feature-integration prior-art alignment)",
      "positions": [
        {"lens": "spec", "position": "keep — cache_mode dispatch mirrors `parse_parquet_compression` precedent."},
        {"lens": "correctness", "position": "keep — analogous prior art insight directly produced finding 1 (AbortOnDropHandle missing because IPC sink does wrap)."},
        {"lens": "maint", "position": "revisit-but-keep — prior-art alignment isn't fully realized: `pl.read_vortex` is missing parameters present in `pl.read_parquet`; README's Python API surface needs updating; `segment_cache` field naming choice needs cross-referenced docs."},
        {"lens": "arch", "position": "keep — touch points are minimal; each is a discrete branch or single-file insertion."}
      ],
      "synthesizer_call": "revisit-but-keep — most-pessimistic enum ordering wins. Maint's framing is sharp: the work shape is correct, but the prior-art alignment is partial. The cache_mode dispatch IS aligned with `parse_parquet_compression`; the read_vortex / scan_vortex surface is NOT aligned with read_parquet / scan_parquet. Phase 1.3 or Phase 3 should close the read/scan surface asymmetry."
    },
    {
      "topic": "Final phase plan tradeoff (4-phase split)",
      "positions": [
        {"lens": "spec", "position": "revisit-but-keep — Phase 1 expanded substantially; subsequent phases should pre-budget for cross-cutting CI-greenup."},
        {"lens": "correctness", "position": "keep — phase boundaries remain coherent."},
        {"lens": "maint", "position": "keep — Phase 1's actual scope grew but the boundary is clean."},
        {"lens": "arch", "position": "revisit-but-keep — Phase 1's scope crept; one-off due to crates.io unblocking pre-existing CI failures. Keep 4-phase split, note scoping lesson."}
      ],
      "synthesizer_call": "revisit-but-keep — half the reviewers want a scoping lesson captured even though all four agree the 4-phase structure should hold. Phase 2 PR-13 entry should pre-declare any CI-greenup as a separate sub-PR (sub-1.2 sibling) rather than absorbing it."
    }
  ],
  "dropped_re_flags": [
    {
      "topic": "In-memory ScanSourceRef::Buffer zero-copy at scans.rs:324 (`buf.as_slice().to_vec()`)",
      "reason": "covered by Deferred work",
      "reference": "Deferred work:147 — `In-memory ScanSourceRef::Buffer zero-copy (dsl_to_ir/scans.rs:323 does buf.as_slice().to_vec()): considered in PR-1.2, deferred as not-low-effort.`"
    },
    {
      "topic": "Rust dispatch tighten for new_from_vortex match arms (correctness's recommendation that includes the 'tighten the arms' ask)",
      "reason": "covered by Deferred work (tighten part); the coverage half is kept as a unified should-fix",
      "reference": "Deferred work:151 — `PR-1.2 Rust dispatch tighten + Rust unit test (crates/polars-python/src/lazyframe/general.rs:334-359): pyo3 new_from_vortex match silently ignores cache_dedicated_bytes for ('global', _) / ('off', _) arms. Defensive ('dedicated', None) and (other, _) arms are unreachable from Python's _resolve_cache_mode and have no Rust test coverage. Deferred to a follow-up PR.`"
    },
    {
      "topic": "No Rust-level streaming source/sink tests at all (the 'general framing' re-flag — distinct from the producer-error specific test that the unified should-fix retains)",
      "reason": "covered by Deferred work",
      "reference": "Deferred work:148 — `Rust-level tests for the polars-stream Vortex source/sink (currently only Python): deferred to Phase 3.`"
    },
    {
      "topic": "mem::transmute SAFETY comment length at write/array_bridge.rs (maint nit on lines 160-164, framed as 'well-written but borderline')",
      "reason": "covered by Accepted tradeoffs",
      "reference": "Accepted tradeoffs:132 — `mem::transmute in read/array_bridge.rs + write/array_bridge.rs + write/df_to_stream.rs: accepted ... Compile-time size_of + align_of asserts + runtime length check provide safety.` Note: the arch finding at read/array_bridge.rs:126 is RETAINED as a should-fix because it's a different concern (the SAFETY-comment-length BAN, not the underlying mem::transmute approach)."
    },
    {
      "topic": "Hard cache hit/miss counters (Moka doesn't expose stats)",
      "reason": "covered by Deferred work",
      "reference": "Deferred work:149 — `Hard-cached segment-cache hit count test: Moka's Cache doesn't expose hit/miss stats. Deferred.`"
    }
  ],
  "phase_artifacts": {
    "summary": "Phase 1 ratifies the cumulative 31-commit vortex-integration branch and ships two new PRs (PR-1.1 + PR-1.2) plus an absorbed CI-greenup. Organized by concept:\n\n(1) CRATES.IO TRANSITION (PR-1.1, 2 commits, ending `018f2ce43`): workspace Cargo.toml switched from path-dep `vortex = { path = \"../../../../vortex/vortex\", ... }` to `vortex = { version = \"0.70.0\", default-features = false, features = [\"files\", \"tokio\"] }`. Cargo.lock refreshed. Single API-drift fix in `crates/polars-vortex/src/read/schema.rs`: the `DType::Union(_)` arm was removed (Vortex 0.70.0 doesn't expose it; the local-workspace 0.1.0 did). `DType::Variant(_)` was retained with a clear bail-with-message and a new test `variant_dtype_errors_with_clear_message`. The `GIT_CONFIG_GLOBAL=...` env-shim is no longer required after the path-dep removal. Clean, single-arm transition.\n\n(2) PYTHON CACHE-MODE SURFACE (PR-1.2): `pl.scan_vortex(..., cache_mode=...)` and `pl.read_vortex(..., cache_mode=...)` accept `Literal['global', 'off'] | int | None`. Python helper `_resolve_cache_mode` at `py-polars/src/polars/io/vortex/functions.py:186-217` dispatches to a 2-arg pyo3 pair `(cache_mode_kind: &str, cache_dedicated_bytes: Option<u64>)`; pyo3 `new_from_vortex` at `crates/polars-python/src/lazyframe/general.rs:336-372` matches into the Rust `VortexCacheMode::{Global, Off, Dedicated(N)}` enum. Bool/float/u64-overflow rejection paths are tested. Dispatch mirrors the `parse_parquet_compression` precedent — strong prior-art alignment. The Python resolver's return type was narrowed to `tuple[Literal['global', 'off', 'dedicated'], int | None]` post-review.\n\n(3) VISITOR CFG-GATING ROOT-CAUSE FIX (PR-1.2): `crates/polars-python/Cargo.toml:53` moved `serde_json` from `optional = true` (gated on the `json` feature) to non-optional. The visitor `scan_type_to_pyobject` arms for csv/parquet/vortex use `serde_json::to_string` regardless of the `json` feature. Fix at the dep-declaration layer rather than at the source-edit layer originally planned.\n\n(4) CI GREEN-UP ABSORBED (10+ items in PR-1.2): ruff (4 lints), dprint (.big-plans/ exclude + README emphasis + README table alignment), cargo fmt sweep across 19 polars-vortex files, mypy stubs added to `_plr.pyi`, mypy redundant-expr in `_resolve_cache_mode`, clippy `approx_constant` (predicate.rs:293 literal 3.14 → 2.5), cargo deny `0BSD` license + `RUSTSEC-2024-0436` (paste unmaintained — transitive via Vortex), dsl-schema feature wiring + 4 new hashes in `crates/polars-plan/dsl-schema-hashes.json`. Tests added: `test_cache_mode_accepts_all_valid_inputs` (4 valid inputs) and `test_cache_mode_rejects_invalid_inputs` (5 invalid inputs).\n\nWhat's at risk (6 must-fix items):\n- Two silent-data-corruption hazards in the PRE-EXISTING streaming sink writer-task lifecycle (AbortOnDropHandle missing; producer error not forwarded).\n- A `u64 as usize` BAN violation at `dsl_to_ir/scans.rs:345` inconsistent with the matching streaming-source pattern at `io_sources/vortex/mod.rs:217`.\n- README documentation drift: field name `cache` vs actual `segment_cache`; documented `scan_vortex` signature missing the new `cache_mode=` parameter.\n- One silent error swallow `let _ = self.io_metrics.set(...)` in pre-existing streaming-source builder code.\n\nThese 6 items concentrate in 4 areas (streaming-sink lifecycle; row_count cast; README docs; silent error swallow) and are all fixable in a focused PR-1.3.",
    "surprises": [
      {"what": "Vortex streaming sink's `write_handle = ASYNC.spawn(...)` is NOT wrapped in `tokio_handle_ext::AbortOnDropHandle`, divergent from the IPC sink (ipc/mod.rs:101-112) which DOES wrap. On outer-task failure, the writer task continues, sees the closed chunk channel as clean EOS, and produces a truncated-but-valid-looking Vortex file.", "how_handled": "Not yet — surfaced in this Phase-1 review. Goes into PR-1.3.", "amend_plan": "yes"},
      {"what": "Producer task at `vortex/mod.rs:113-128` uses `?` to propagate errors from `dataframe_to_vortex_chunks(&df)`. On Err, `tx` is dropped without sending an Err item; the writer task sees the channel close as clean EOS and writes the Vortex footer on truncated data. The channel type `VortexResult<VortexArrayRef>` CAN carry errors but the producer never uses the Err path.", "how_handled": "Not yet — surfaced in this Phase-1 review. Goes into PR-1.3.", "amend_plan": "yes"},
      {"what": "`crates/polars-plan/src/plans/conversion/dsl_to_ir/scans.rs:345` uses `vxf.row_count() as usize` while the matching site at `crates/polars-stream/src/nodes/io_sources/vortex/mod.rs:217` correctly uses the BAN-compliant `usize::try_from(...).unwrap_or(usize::MAX)`. Inconsistent treatment of the row-count clamp across the two sites that read `vxf.row_count()`.", "how_handled": "Not yet — surfaced in this Phase-1 review. Goes into PR-1.3.", "amend_plan": "yes"},
      {"what": "README documents `pub cache: VortexCacheMode` but the actual field is `pub segment_cache: VortexCacheMode`. Rename was intentional (collision avoidance with `ScanArgsVortex::cache: bool`) but the canonical reference doc lags.", "how_handled": "Not yet — surfaced in this Phase-1 review. Goes into PR-1.3.", "amend_plan": "yes"},
      {"what": "README's documented Python `scan_vortex(...)` signature is missing the new `cache_mode=` parameter that is the headline new public-API surface of PR-1.2.", "how_handled": "Not yet — surfaced in this Phase-1 review. Goes into PR-1.3.", "amend_plan": "yes"},
      {"what": "`crates/polars-stream/src/nodes/io_sources/vortex/builder.rs:49` does `let _ = self.io_metrics.set(io_metrics);` — silently swallows the `Err(io_metrics)` returned by `OnceLock::set` on double-init, with no comment.", "how_handled": "Not yet — surfaced in this Phase-1 review. Goes into PR-1.3.", "amend_plan": "yes"},
      {"what": "`vortex_file_info` at `dsl_to_ir/scans.rs:336` hardcodes `session::segment_cache()` for schema discovery, ignoring `VortexScanOptions::segment_cache`. Users passing `cache_mode='off'` still hit the global cache during the postscript read. Streaming-source path correctly threads the option through.", "how_handled": "Not yet — surfaced in this Phase-1 review.", "amend_plan": "yes"},
      {"what": "Write-side (`crates/polars-vortex/src/write/{array_bridge,df_to_stream,strategy,sink_writer,writer}.rs`) has zero direct unit tests — only the roundtrip integration catches regressions.", "how_handled": "Not yet — surfaced in this review.", "amend_plan": "no"},
      {"what": "Test `time_extension_days_unit_errors` in `schema.rs:429-441` asserts nothing — constructs a Nanoseconds Time, discards it, exits. Anti-help test.", "how_handled": "Not yet — surfaced in this review.", "amend_plan": "no"},
      {"what": "PR-1.2's planned scope was ~3-4 polish items; actual scope grew to 10+ items because PR-1.1's crates.io transition unblocked CI which surfaced layered pre-existing failures.", "how_handled": "Absorbed into PR-1.2; impl-status self-flags. Plan-row PR-1.2 retroactively amended.", "amend_plan": "already-done"},
      {"what": "The 'visitor feature-gating fix' was originally framed as a `visitor/nodes.rs` source edit; actual fix lives at the Cargo.toml level (`serde_json` non-optional).", "how_handled": "Plan-row corrected at commit `c42274a6a`.", "amend_plan": "already-done"},
      {"what": "Vortex 0.70.0 has `DType::Variant` (retained with bail-arm) but lacks `DType::Union` (which the local-workspace 0.1.0 had).", "how_handled": "Single-arm removal; new test for Variant bail.", "amend_plan": "already-done"},
      {"what": "Phase 1 exit criteria say '65 Rust tests' / '8 Python tests' but actual is 66 / 10 (PR-1.1 added 1 Rust test; PR-1.2 added 2 Python tests).", "how_handled": "Impl-status records the correct counts; phase-row text wasn't updated.", "amend_plan": "yes"},
      {"what": "Deny.toml gained an undocumented `RUSTSEC-2024-0436` (paste) ignore alongside the planned 0BSD addition.", "how_handled": "Commit `9dbb23c62` added the ignore; plan PR-1.2 row didn't enumerate.", "amend_plan": "yes"},
      {"what": "In-memory `ScanSourceRef::Buffer` zero-copy assessed and properly deferred (not-low-effort).", "how_handled": "Plan-commit `2c9abdf4e`.", "amend_plan": "already-done"},
      {"what": "`set_global_cache_bytes` is a public Python API (via `set_vortex_cache_bytes`) with zero functional tests. Concurrent invocation against in-flight scans has undocumented transient-memory semantics.", "how_handled": "Not yet — surfaced in this review.", "amend_plan": "no"},
      {"what": "`pl.read_vortex` is missing parameters that `pl.scan_vortex` exposes. Prior-art-alignment with `pl.read_parquet` / `pl.scan_parquet` is partial.", "how_handled": "Not yet — surfaced in this review.", "amend_plan": "no"},
      {"what": "Scaffolding markers are largely absent (predicate.rs is scheduled for PR-2.6 deletion but no inline marker; read/mod.rs back-compat shim has no deletion date; write/mod.rs:4 references PR-10 with no link).", "how_handled": "Not yet — surfaced in this review.", "amend_plan": "no"}
    ],
    "coverage": {
      "tested_cases": [
        {"case": "`DType::Variant` produces clear bail error", "test_location": "crates/polars-vortex/src/read/schema.rs:243-258", "confidence": "high"},
        {"case": "`cache_mode=None`/'global'/'off'/positive int accepted", "test_location": "py-polars/tests/unit/io/test_vortex.py:135-148", "confidence": "high"},
        {"case": "`cache_mode=0`/`-5` raise ValueError", "test_location": "py-polars/tests/unit/io/test_vortex.py:157-160", "confidence": "high"},
        {"case": "`cache_mode='invalid'` raises TypeError", "test_location": "py-polars/tests/unit/io/test_vortex.py:163-164", "confidence": "high"},
        {"case": "`cache_mode=True/False` raise TypeError (bool guard)", "test_location": "py-polars/tests/unit/io/test_vortex.py:170-173", "confidence": "high"},
        {"case": "`cache_mode=1.5` raises TypeError", "test_location": "py-polars/tests/unit/io/test_vortex.py:176-177", "confidence": "high"},
        {"case": "`cache_mode=2**64` raises OverflowError", "test_location": "py-polars/tests/unit/io/test_vortex.py:181-182", "confidence": "high"},
        {"case": "VortexCacheMode::Global/Off/Dedicated resolve semantics", "test_location": "crates/polars-vortex/src/read/options.rs:81-131", "confidence": "high"},
        {"case": "C-ABI bridge size_of + align_of asserts (both directions) + runtime length parity", "test_location": "crates/polars-vortex/src/read/array_bridge.rs:143-144,175 + write/array_bridge.rs:40-43", "confidence": "high"},
        {"case": "Predicate convertor for Equal/Between/EqualOneOf/StartsWith/EndsWith/RegexMatch and primitives/temporal/decimal", "test_location": "crates/polars-vortex/src/read/predicate.rs:286-500", "confidence": "high"},
        {"case": "Decimal precision/scale overflow + negative-scale rejection", "test_location": "crates/polars-vortex/src/read/predicate.rs:391-398 + schema.rs:311-323", "confidence": "high"},
        {"case": "DType→ArrowDataType mapping for all supported dtypes", "test_location": "crates/polars-vortex/src/read/schema.rs:178-505", "confidence": "high"},
        {"case": "Roundtrip read/write for primitives, nullable, utf8, binary, datetime+tz, decimal, lists, structs", "test_location": "crates/polars-vortex/tests/roundtrip.rs", "confidence": "high"},
        {"case": "Python read/write roundtrip + filter / projection-pushdown / negative-slice", "test_location": "py-polars/tests/unit/io/test_vortex.py:55-132", "confidence": "high"},
        {"case": "`PolarsInstrumentedVortexReadAt` forwards reads + records IOMetrics", "test_location": "crates/polars-vortex/src/read/read_at.rs:221-280", "confidence": "medium"}
      ],
      "untested_cases": [
        {"case": "Vortex streaming sink producer-error silent-truncation (the must-fix bug at mod.rs:113-128)", "priority": "high", "why_untested": "No Rust-level streaming-sink tests; no Python test asserts file completeness or non-existence on producer-error paths. The two must-fix concurrency bugs would not be caught by any existing test."},
        {"case": "Vortex sink writer-task abort propagation when outer task fails (must-fix at mod.rs:135-146)", "priority": "high", "why_untested": "Same Rust-test gap."},
        {"case": "Vortex sink with non-bridgeable column type", "priority": "high", "why_untested": "Natural integration test to catch the must-fix silent-truncation bug — none exists."},
        {"case": "`set_global_cache_bytes` functional behavior (Arc-swap semantics + transient-memory)", "priority": "high", "why_untested": "Public Python API with zero test scaffolding."},
        {"case": "Concurrent `set_global_cache_bytes` + in-flight scan (old-cache lifetime)", "priority": "high", "why_untested": "No race-test scaffolding; semantics undocumented."},
        {"case": "`vortex_file_info` honors `VortexScanOptions::segment_cache` (currently doesn't — should-fix arch finding)", "priority": "high", "why_untested": "Discovered during this review."},
        {"case": "`as usize` cast at scans.rs:345 on a >2^32-row file (32-bit truncation)", "priority": "medium", "why_untested": "32-bit platform tests not run in CI."},
        {"case": "Rust-level `new_from_vortex` dispatch arms (`('global', None)`, `('off', None)`, `('dedicated', Some(N))`, defensive `('dedicated', None)` + `(other, _)`)", "priority": "medium", "why_untested": "Deferred per plan; pyo3 dispatch is sole entry-point from Python."},
        {"case": "Visitor cfg-gating regression test (vortex built without `json` feature)", "priority": "medium", "why_untested": "No feature-matrix CI step added."},
        {"case": "Empty DataFrame (0-row, 0-column) write/read round-trip", "priority": "medium", "why_untested": "`dataframe_to_vortex_chunks` has a 0-chunk early-return path that is uncovered."},
        {"case": "Categorical/Enum → UTF-8 write fallback (README claim at line 374-375)", "priority": "medium", "why_untested": "README documents the behavior but no test verifies it."},
        {"case": "Write-side `polars_chunk_to_upstream_record_batch` field-metadata preservation in isolation", "priority": "medium", "why_untested": "Write-side unit tests entirely missing."},
        {"case": "Multi-file scan with mixed local + cloud sources + shared cache", "priority": "medium", "why_untested": "Deferred to Phase 3."},
        {"case": "Rust-level streaming source/sink integration tests (general)", "priority": "medium", "why_untested": "Deferred to Phase 3."},
        {"case": "set_vortex_cache_bytes(True), (1.5), (-1) Python-side rejection (inconsistent with cache_mode validator)", "priority": "low", "why_untested": "No validation exists in Python at this site."},
        {"case": "Hard cache hit/miss counters", "priority": "low", "why_untested": "Moka's Cache doesn't expose stats. Deferred."},
        {"case": "In-memory Buffer zero-copy semantics (scans.rs:324, io_sources/vortex/mod.rs:95)", "priority": "low", "why_untested": "Deferred per 'if low-effort' clause; not-low-effort assessment recorded."}
      ],
      "recommendations": "Highest-leverage additions for PR-1.3 (Phase 1 must-fix fixup PR): (1) a Rust integration test for the streaming Vortex sink simulating producer error — this single test would catch both must-fix concurrency bugs once they're fixed; (2) a unit test for `vortex_file_info` honoring the user's `segment_cache` choice (covers the should-fix arch finding); (3) a unit test for `set_global_cache_bytes` Arc-swap semantics; (4) replace the assertion-free `time_extension_days_unit_errors` test; (5) empty-DataFrame round-trip. Defer to Phase 3: multi-file scan, broader Rust streaming-sink coverage, Categorical/Enum fallback. Defer to Phase 4: 32-bit platform regression tests, deny.toml audit at merge prep."
    },
    "tradeoffs": [
      {"decision": "Reuse existing 31 commits vs rewrite from scratch", "original": "Reuse — 73 tests passing locally; two prior gauntlet review passes already surfaced + fixed substantive bugs", "verdict": "keep", "rationale": "All 4 reviewers agree. The 4-vote Phase 1 review IS the verification gate for the reused base, and it surfaced 6 must-fix items concentrated in pre-existing code — exactly the value the gate is meant to deliver. A rewrite would have burned weeks; the must-fix list is bounded and fixable in PR-1.3."},
      {"decision": "Work shape — feature-integration", "original": "feature-integration; Adding Vortex into existing Polars systems with many touch points. Highest-leverage insight: Analogous prior art.", "verdict": "revisit-but-keep", "rationale": "Most-pessimistic across lenses. Spec/correctness/arch say keep; maint flags partial prior-art-alignment realization. The cache_mode dispatch IS aligned with `parse_parquet_compression`; the `read_vortex` / `scan_vortex` surface is NOT aligned with `read_parquet` / `scan_parquet` (asymmetric parameter list). Correctness's first finding (AbortOnDropHandle missing because IPC sink does wrap) is ALSO produced by the analogous-prior-art insight — vindicating the work shape. Keep, but close the read/scan-surface asymmetry in Phase 3."},
      {"decision": "CI green-up approach — crates.io `vortex = \"0.70.0\"`", "original": "Replace workspace path-dep with version = '0.70.0'. Strictly better than git-rev/CI-clone/feature-gating: cleanest dep, no release blocker, version-pinned.", "verdict": "keep", "rationale": "All 4 reviewers agree. PR-1.1 transitioned cleanly with a single API-drift fix (DType::Union removal). The transitive arrow-* major version coupling (polars-vortex pinned to `arrow-* = \"58\"` to match Vortex's transitive) deserves an upgrade-runbook comment (captured as should-fix), but the decision itself is sound."},
      {"decision": "Final phase plan — 4 phases as drafted", "original": "(1) Ratify + crates.io transition, (2) PR-13 AExpr pushdown, (3) PR-8 file-stats + PR-6 multi-file/nested, (4) PR-14 benches + final polish.", "verdict": "revisit-but-keep", "rationale": "Most-pessimistic across lenses. Spec/arch want a scoping lesson captured; correctness/maint say keep. The 4-phase structure should hold; Phase 2 entry should pre-declare any CI-greenup as a separate sub-PR rather than absorbing it into a polish PR mid-phase. Phase 1's actual scope grew 3x but the boundary is clean — the lesson is about sub-PR pre-declaration, not about the phase split."},
      {"decision": "PR-13 architecture — Option B → A via PR-2.6 cutover", "original": "Parallel paths in PR-2.2/.3/.4/.5; PR-2.6 deletes the SpecializedColumnPredicate fast path.", "verdict": "keep", "rationale": "All 4 reviewers agree. Strategy is mechanically tractable; SpecializedColumnPredicate extraction is contained to a single file (predicate.rs). Captured a should-fix scaffolding-marker finding (predicate.rs needs an inline 'scheduled for PR-2.6 deletion' marker) but the decision itself stands."},
      {"decision": "PR-8 leads Parquet on `table_statistics`", "original": "Lead the pattern, no API change (Phase 3 scope)", "verdict": "keep", "rationale": "3 of 4 reviewers say keep; maint says `no-longer-applicable` (out of scope to evaluate from this phase). Per most-pessimistic enum ordering, `no-longer-applicable` is treated as a non-verdict (lens can't evaluate) and falls behind `keep` from the other 3, so the synthesized verdict is `keep`. No architectural blocker for Phase 3 to populate `UnifiedScanArgs::table_statistics` from the Vortex footer."},
      {"decision": "Per-phase review-counts — 4 / 4 / 4 / 4", "original": "All four phase-end reviews use the 4-vote phase-4 preset. Max thoroughness.", "verdict": "revisit-but-keep", "rationale": "Most-pessimistic across lenses. Spec/correctness/maint say keep (citing this 4-vote review's bug-find rate as direct validation); arch says revisit-but-keep (4-vote may be over-thorough for Phases 2 and 3, which are focused new work rather than cumulative artifacts). The current 4-vote Phase 1 review found 6 must-fix items that prior 2-vote per-PR reviews did not surface — that's the empirical case for keeping the cost. But arch's framing has merit: revisit at Phase 4 entry whether Phases 2 and 3 actually need 4-vote or whether 3-vote would have caught the same items."},
      {"decision": "Single Tokio runtime (Polars' global `ASYNC`) for Vortex async work", "original": "Avoid doubling thread-pool overhead", "verdict": "keep", "rationale": "Unchanged by Phase 1. Sound; session.rs implements correctly via Handle::new(Arc::downgrade(Arc::new(ASYNC.handle()) as Arc<dyn Executor>))."},
      {"decision": "`mem::transmute` between Arrow FFI structs with size+align asserts + runtime length check", "original": "Compile-time asserts + runtime length check provide safety; the clean alternative (from_ffi_parts upstream API) is a separate polars contribution", "verdict": "keep", "rationale": "Phase 1 didn't touch the transmute call-sites. SAFETY comments are well-written; the should-fix arch finding on read/array_bridge.rs:126 is about the comment LENGTH violating the BAN spirit, not the underlying approach. Compress the comment but keep the approach."},
      {"decision": "`SpecializedColumnPredicate` fast path preserved during PR-13 transitional phases (Option B)", "original": "Option B trajectory: parallel path first, delete fast path last", "verdict": "keep", "rationale": "Phase 2 scope; unchanged."},
      {"decision": "`create_skip_batch_predicate = false` for Vortex", "original": "Vortex's pruning_evaluation handles it; explicit branch at polars-mem-engine/src/planner/lp.rs:448-459", "verdict": "keep", "rationale": "Branch is present and well-documented at lp.rs:448-459. Phase 1 didn't touch this."},
      {"decision": "`hashbrown 0.16` (Polars) + `0.17` (Vortex transitive) coexistence", "original": "BAN against bare PlHashMap::new() is the established workaround", "verdict": "keep", "rationale": "Phase 1 BAN compliance verified across the diff."},
      {"decision": "`PolarsInstrumentedVortexReadAt` mandatory wrapping", "original": "Local/cloud/in-memory all route through it", "verdict": "keep", "rationale": "Verified at all three sites; Phase 1 didn't change the wrapping."},
      {"decision": "`row_count: usize::try_from(u64).unwrap_or(usize::MAX)` clamp", "original": "Accepted: 32-bit edge case clamp at nodes/io_sources/vortex/mod.rs:221", "verdict": "revisit-but-keep", "rationale": "The clamp DECISION is correct at the streaming-source site. Application is INCONSISTENT: the matching IR-build-time site at dsl_to_ir/scans.rs:345 uses `as usize` directly — a BAN violation captured as must-fix in the unified findings. Keep the decision (clamp pattern); revise by applying it consistently to both row-count read sites in PR-1.3."},
      {"decision": "Sorting `ColumnPredicates::predicates` by column name before AND-collect", "original": "Deterministic ordering is a reproducibility guarantee", "verdict": "keep", "rationale": "Confirmed at predicate.rs:50. Phase 1 didn't change this."}
    ]
  },
  "executive_summary": "Phase 1 ('Ratify + crates.io transition') of the polars-vortex integration is the cumulative 31-commit base plus PR-1.1 (path-dep → crates.io `vortex = \"0.70.0\"`), PR-1.2 (Python `cache_mode=` surface + visitor cfg-gating root-cause fix + CI greenup absorption — 10+ items including ruff/dprint/mypy stubs/cargo fmt sweep across 19 files/cargo deny 0BSD + RUSTSEC-2024-0436/dsl-schema hashes), and the implicit ratification of the four settled architectural moves (mem-engine delegates to streaming; single Tokio runtime via Polars' global ASYNC; C-ABI bridge via mem::transmute with size+align asserts + runtime length check; SpecializedColumnPredicate fast path preserved). Reviewed by 4 lenses (spec, correctness, maint, arch) per the phase-4 preset.\n\nThe review surfaces 6 must-fix items concentrated in 4 areas:\n(a) silent data-corruption hazards in the PRE-EXISTING streaming sink writer-task lifecycle: `write_handle = ASYNC.spawn(...)` is not wrapped in `AbortOnDropHandle` (divergent from IPC sink which does wrap), and the producer task drops `tx` on error rather than forwarding the Err — both paths produce valid-looking-but-truncated Vortex files on disk;\n(b) a `u64 as usize` BAN violation at `dsl_to_ir/scans.rs:345` inconsistent with the BAN-compliant clamp at the matching streaming-source site `io_sources/vortex/mod.rs:217`;\n(c) README documentation drift: documented field name `cache` vs actual `segment_cache`, and the documented `scan_vortex` signature missing the new `cache_mode=` parameter (the headline new API surface of this phase);\n(d) one silent error swallow `let _ = self.io_metrics.set(...)` at `builder.rs:49` without rationale comment.\n\nNotable surprises also include: `vortex_file_info` at `scans.rs:336` hardcodes the global segment cache for schema discovery (user's `cache_mode='off'` is silently ignored at that read — captured as should-fix); write-side has zero direct unit tests; `time_extension_days_unit_errors` asserts nothing; `set_global_cache_bytes` (public Python API via `set_vortex_cache_bytes`) has zero functional tests and undocumented mid-scan transient-memory semantics.\n\nKey tradeoffs revisited: (1) per-phase review-counts (4/4/4/4) revisit-but-keep — this 4-vote Phase-1 review's bug-find rate justifies the cost retrospectively, but arch's case that Phases 2-3 (focused new work) could use 3-vote is worth re-evaluating at Phase 4 entry. (2) Work shape (feature-integration) revisit-but-keep — the prior-art-alignment insight directly produced correctness finding 1 (AbortOnDropHandle missing because IPC sink does wrap), vindicating the shape; but `pl.read_vortex` vs `pl.scan_vortex` surface is asymmetric vs the `read_parquet`/`scan_parquet` template. (3) Final phase plan (4-phase) revisit-but-keep — boundaries hold; future phases should pre-declare any CI-greenup as a separate sub-PR rather than absorbing.\n\nVerdict split: spec and arch ACCEPT on their own lens scope (no must-fix from their lenses); correctness and maint REJECT (3 + 4 must-fix from their lenses, with 1 overlap). Conservative-union synthesis is REJECT with 6 must-fix items going into PR-1.3, plus 28 should-fix items and 13 nits captured for Phase 2-4 absorption.",
  "overall": "reject",
  "must_fix_count": 6,
  "should_fix_count": 28,
  "nit_count": 13
}

```

</details>

## Phase 1: Ratify + crates.io transition — end-of-phase review (cycle 2) — accepted (4-vote)

**Synthesizer output from `/spiral:gauntlet` (`preset=phase-4`, lenses=`spec`+`correctness`+`maint`+`arch`); full Synthesizer Output JSON in the `<details>` block at the end of this section.**

### Executive summary

Phase 1 ('Ratify + crates.io transition') ACCEPTS at cycle 2. All 4 reviewers (spec, correctness, maint, arch) reach `accept`. 0 must-fix items, 11 should-fix, 6 nits across 17 unified findings (after dedupe). The cumulative Phase-1 artifact — 31-commit base + PR-1.1 (crates.io transition) + PR-1.2 (Python cache_mode= surface + visitor cfg-gating + 10+ CI-greenup items) + PR-1.3 (6 phase-end must-fix items + 2 inner-loop must-fix + 2 inner-loop should-fix across 10 work commits) — is architecturally coherent and correctness-sound. All 6 cycle-1 phase-end must-fix items are verified resolved at source by all 4 reviewers (Resolved phase-end must-fix items table has all rows marked [x]). The four settled architectural moves (mem-engine delegates to streaming; single Polars ASYNC Tokio runtime; mem::transmute C-ABI bridge with size+align+length safety triplet; SpecializedColumnPredicate fast path preserved) are intact. Cross-bundle consistency on cache_mode is tight. The MF-001/MF-002 silent-truncation hazards are closed; correctness lens traced the producer/writer error-path lifecycle exhaustively and verified deterministic behavior (producer error always wins via await ordering — incidentally revealing that the Deferred work entry overstates the race, captured as a should-fix doc-update).

**Key surprises**: (a) cycle-1 reward-hacking on MF-006 was caught and routed to a strictly-better fix in cycle-2 inner loop — process lesson recorded; (b) cycle-1 verification gap on the `vortex_err!` build break revealed that `cargo check -p polars` (umbrella) does NOT activate polars-stream's vortex code without `new_streaming` — recommend adding `-p polars-stream --features vortex,cloud` to the verification checklist; (c) the `pub use ::vortex;` re-export WIDENED via the macro dependency but the 5-sub-module narrowing still works (macros re-export by module).

**Critical tradeoff verdicts**: REUSE 31 COMMITS → firm KEEP (8 must-fix items emerging from cycle-1+2 is the most-skeptical case for rewrite and even that favors reuse). 4/4/4/4 REVIEW COUNTS → firm KEEP (cycle-2's adversarial second pass caught the MF-006 reward-hack + the build break — neither would have surfaced under 3-vote). FEATURE-INTEGRATION WORK SHAPE → KEEP (prior-art alignment tightened; the read_vortex/scan_vortex asymmetry remains as one Phase 3 carry-forward). `pub use ::vortex;` → REVISIT-BUT-KEEP (narrow to 5 sub-modules in Phase 2 entry).

**Top should-fix recommendations for Phase 2 entry housekeeping**: (1) thread `VortexScanOptions::segment_cache` through `vortex_file_info` at scans.rs:336 (5-line fix; closes the divergent IR-time vs streaming-time semantics that partial-honors `cache_mode='off'`); (2) narrow `pub use ::vortex;` to 5 sub-modules (mechanical, tightens layering BAN's machine-checkability); (3) update Deferred work entry to reflect actual error-path determinism; (4) update Plan-row 184 stale 65/8 test count to actual 66/10; (5) inline `read::metadata` back-compat shim (one caller; 2-minute fix); (6) add `cargo check -p polars-stream --features vortex,cloud` to verification checklist.

The phase-end checkpoint is the right moment for the user to decide between (a) proceeding to Phase 2 (PR-13 AExpr pushdown) directly, (b) inserting a Phase 1.4 cleanup PR for the top should-fix items, (c) amending Phase 1 to absorb the cleanup before moving on, or (d) pausing and resuming in a fresh session.

### Summary of changes

Phase 1 ('Ratify + crates.io transition') of polars-vortex closes coherently at the architectural level after cycle-2 phase-4 retroactive ratification. The cumulative diff covers the 31-commit base + PR-1.1 (crates.io transition + DType::Union arm removal + DType::Variant bail test) + PR-1.2 (Python cache_mode= surface + visitor cfg-gating root-cause fix + 10+ CI-greenup items absorbed) + PR-1.3 (6 phase-end must-fix items + 2 inner-loop must-fix + 2 inner-loop should-fix across 10 work commits in 2 inner-loop cycles). All 6 cycle-1 phase-end must-fix items are verified resolved: MF-001 wraps Vortex sink writer in tokio_handle_ext::AbortOnDropHandle (mirroring IPC sink at ipc/mod.rs:101 — closes outer-task-failure→silent-footer hole); MF-002 forwards producer errors through chunk_tx as Err(vortex_err!(...)) BEFORE bailing (closes producer-error→silent-footer hole); MF-003 replaces u64-as-usize cast with usize::try_from(...).unwrap_or(usize::MAX) at scans.rs:349 mirroring the established clamp at io_sources/vortex/mod.rs:217; MF-004 corrects README field name to segment_cache; MF-005 documents cache_mode= in the README scan_vortex signature; MF-006 aligns set_io_metrics with CSV/IPC/NDJSON 3-of-4 sibling consensus via .ok().unwrap() (cycle-2 inner-loop reroute from cycle-1's reward-hacking comment-only fix). The four settled architectural moves — mem-engine delegates to streaming, single Polars ASYNC Tokio runtime, mem::transmute C-ABI bridge with size+align+length safety triplet, SpecializedColumnPredicate fast path preserved for PR-13 transition — are coherently implemented and unchanged. Cross-bundle consistency on the cache_mode/cache_mode_kind/cache_dedicated_bytes 2-arg pyo3 pair is tight across Python/Rust/.pyi/README. 0 must-fix items in cycle-2. 16 should-fix items (highest-priority: vortex_file_info segment_cache hardcode at scans.rs:336 — divergent IR-time vs streaming-time semantics; pub use ::vortex; can narrow to 5 sub-modules covering the 8 actual paths; producer-error comment overstates the race — actually deterministic per await ordering; scaffolding markers absent on PR-2.6 deletion target + back-compat shim + PR-10 stale reference; .ok().unwrap() BAN-vs-consensus tension worth codifying; long justification comments at sink/builder/array_bridge approaching BAN thresholds). 6 nits. Cumulative deferred items: 6. The integration is coherent, exit criteria all pass, and Phase 2 (PR-13 AExpr pushdown) is unblocked.

### Surprises and discoveries

- **All 6 cycle-1 phase-end must-fix items were in PRE-EXISTING 31-commit code, not in PR-1.1 or PR-1.2 directly. Two of those (MF-001, MF-002) were silent-data-corruption hazards in the streaming sink writer-task lifecycle that two prior hand-prompted gauntlet passes + per-PR inner-loop reviews missed. Phase 1's retroactive ratification framing delivered as designed.**
  - How handled: Resolved in PR-1.3. Pattern recorded: prior-art-alignment insight (maint lens) produced MF-001; the feature-integration work-shape's highest-leverage insight was vindicated.
  - Amend plan: `already-done`
- **PR-1.3's cycle-1 MF-006 fix was a reward-hacking comment-add. Cycle-2 inner-loop caught it via specific code-trace through multi_scan/mod.rs:185-200 + pipeline/initialization.rs:366 + physical_plan/lower_ir.rs:769-775 (single-call-site reality) and routed to the strictly-better .ok().unwrap() fix aligned with CSV/IPC/NDJSON 3-of-4 sibling consensus. Lesson: when adding a justification comment to a silent-error swallow, audit the underlying pattern.**
  - How handled: Resolved cycle-2; documented in PR-1.3 surprises.
  - Amend plan: `already-done`
- **PR-1.3's cycle-1 MF-002 fix introduced a build break (`vortex::error::vortex_err!` doesn't resolve in polars-stream). The umbrella `cargo check -p polars --features vortex,cloud,parquet,dtype-full` doesn't include `new_streaming`, so polars-stream's vortex sink code wasn't compiled. Verification gap: the right check is `-p polars-stream --features vortex,cloud`.**
  - How handled: Resolved cycle-2 via `polars_vortex::vortex::error::vortex_err!` through the broad re-export. Recommend adding `cargo check -p polars-stream --features vortex,cloud` to the verification checklist for future PRs touching polars-stream.
  - Amend plan: `already-done`
- **Cycle-2 surfaced a pre-existing E0004 build break under `polars-stream --features vortex` (without `cloud`): the Vortex sink Writeable match's `Writeable::Cloud(_)` arm is `#[cfg(feature = "cloud")]` on polars-stream's own `cloud` feature, but the underlying enum's Cloud variant remains visible because `polars-io`'s `file_cache` feature transitively enables `polars-io/cloud`. CI doesn't catch this combo. Predates PR-1.3.**
  - How handled: Deferred (cumulative deferred 6). Tracked for Phase 2 cleanup or follow-up PR.
  - Amend plan: `already-done`
- **Correctness lens traced the producer/writer error-path race exhaustively and verified the Deferred work entry 'Vortex sink producer/writer error-path determinism polish' MISCHARACTERIZES the behavior. The producer's `.await?` ALWAYS sees the producer's terminal value before `write_handle.await` runs, so the producer's PolarsError always propagates first; the writer's VortexError-wrapped form is silently discarded via AbortOnDrop. Producer-Err path is deterministic; only error-message-WORDING is what the Deferred entry was trying to characterize as 'race-y' but it's not race-y in user-visible behavior.**
  - How handled: Not yet — captured as should-fix above to update both the inline comment and the Deferred work entry. Trivial doc-only correction.
  - Amend plan: `yes`
- **`pub use ::vortex;` re-export WIDENED via PR-1.3's MF-002 fix introducing a macro dependency (`vortex::error::vortex_err!`). Macros from external crates have looser stability contracts than function signatures, so this is a regression on the layering concern cycle-1 already flagged. But the narrowing recommendation still works WITH the macro because macros are re-exported by module — `pub mod vortex { pub use ::vortex::{array, error, file, io, session}; }` covers every actual use including the macro.**
  - How handled: Captured as should-fix above with concrete narrowing recommendation.
  - Amend plan: `no`
- **Plan-row PR-1.2 'Files touched (expected)' field still lists 4 files; actual is ~30. Cycle-1 should-fix #1 noted this; PR-1.3 didn't address (must-fix-only). Implementation status carries the truth; the 'expected' field is documentation-completeness drift.**
  - How handled: Captured as nit above. Low priority.
  - Amend plan: `no`
- **Phase 1 exit criteria row 184 still reads '65 Rust tests' / '8 Python tests' — actual is 66 / 10. Cycle-1 should-fix with `Amend plan: yes`; not updated across two cycles. Stale numbers will carry into Phase 2 planning unless fixed.**
  - How handled: Captured as should-fix above. Plan-edit before Phase 2 entry.
  - Amend plan: `yes`


### Testing coverage assessment

**Tested cases:**

| Case | Test location | Confidence |
|------|---------------|------------|
| All 6 cycle-1 phase-end must-fix items verified resolved at source by all 4 reviewers | `Resolved phase-end must-fix items table — Phase 1 cycle 1 (rows 1-6 all marked [x])` | high |
| Cache_mode dispatch consistency across Python/Rust/.pyi/README | `Cross-bundle grep confirms identical naming and types (cache_mode_kind: &str, cache_dedicated_bytes: Option<u64>) at general.rs:343-365 + functions.py:186-217 + _plr.pyi + README.md:325-332` | high |
| VortexCacheMode::{Global, Off, Dedicated} resolve semantics | `crates/polars-vortex/src/read/options.rs:81-131 (three unit tests asserting Arc::ptr_eq behaviors)` | high |
| C-ABI bridge size_of + align_of asserts + runtime length parity (both directions) | `crates/polars-vortex/src/read/array_bridge.rs:143-144,175 + write/array_bridge.rs:40-43` | high |
| Channel-Err capacity bound (no deadlock for tx.send(Err(...))) | `Verified via futures::channel::mpsc::channel(n) capacity semantics: n + num_senders slots` | high |
| No-second-runtime invariant | `session.rs:40-49 (Handle from ASYNC); sink mod.rs:163,111 (ASYNC.spawn + async_executor::spawn)` | high |
| All-reads-via-PolarsInstrumentedVortexReadAt invariant | `crates/polars-vortex/src/read/read_at.rs:106-164 (three factory functions: local/cloud/in-memory all wrap)` | high |
| row_count clamp is symmetric across read sites + write strategy | `scans.rs:349 + io_sources/vortex/mod.rs:217 + write/strategy.rs:26-29 (three sites converged on the same shape)` | high |
| Roundtrip read/write + filter/projection-pushdown + negative-slice | `py-polars/tests/unit/io/test_vortex.py:55-132 + crates/polars-vortex/tests/roundtrip.rs` | high |

**Untested cases (priority-ranked):**

| Case | Priority | Why untested |
|------|----------|--------------|
| Vortex sink producer-error end-to-end regression (Python-level) | high | Deferred. Python-level test (~30 LoC) would catch MF-001/MF-002 against future refactor regression; smaller scope than the Rust-level harness also deferred. |
| vortex_file_info honors VortexScanOptions::segment_cache | high | Carry-forward should-fix; PR-1.3 scoped out. Phase 2 prerequisite or Phase 1.4 cleanup. |
| set_vortex_cache_bytes(True/1.5/-1) Python validation | low | Carry-forward cycle-1 should-fix; inconsistent with cache_mode= validator hygiene. |
| Empty DataFrame (0-col) write/read roundtrip exercising n_chunks==0 early-return | medium | Carry-forward; trivial scope but no test added. |
| 32-bit platform regression on row_count + rbs clamps | low | CI doesn't run 32-bit targets; defensive only. |
| Multi-file scan + schema-evolution + nested-type + small-int dtype coverage | medium | Phase 3 scope per plan. |
| polars-stream --features vortex (without cloud) E0004 build break | low | Pre-existing; deferred. Phase 2 cleanup. |
| Hard cache hit/miss counters (Moka API limitation) | low | Moka doesn't expose stats; deferred. |

**Recommendations:** Phase 2 entry should land a 'Phase 2.0 housekeeping' sub-PR addressing the top 3-4 should-fix items: (1) vortex_file_info segment_cache thread (5-line fix; closes the divergent semantics); (2) narrow `pub use ::vortex;` to 5 sub-modules (mechanical; tightens the layering BAN's machine-checkability); (3) inline the `read::metadata` back-compat shim (2-minute fix, one caller); (4) update plan-row 184 stale exit-criteria numbers (durable plan-edit). Then add the Python-level Vortex sink regression test for MF-001/MF-002 as part of PR-2.1's prerequisite hygiene. Phase 3 takes the broader test-coverage push (Rust-level streaming harness, multi-file, schema-evolution, nested-type, small-int dtypes, read_vortex parameter symmetry). Phase 4 polish: 32-bit overflow regression tests, deny.toml audit, set_global_cache_bytes Arc-swap semantics tests, SAFETY-comment trims.


### Tradeoffs re-evaluation

| Decision | Original choice | Verdict | Rationale |
|----------|-----------------|---------|----------|
| Reuse existing 31 commits vs rewrite from scratch | Reuse — 73 tests passing locally; two prior gauntlet review passes already surfaced + fixed substantive bugs | `keep` | Cycle 2 firms up keep. Net of 6 cycle-1 + 2 cycle-2-inner-loop must-fix items emerging from the 4-vote review IS the most-skeptical case for rewrite — and even that math favors reuse: 8 bounded mechanical fixes (~10 commits in PR-1.3) vs. weeks of feature-parity work in a rewrite. Architectural coherence of the inherited foundation is sound; the four settled moves are well-implemented at the chosen seams. A rewrite of this size would generate a similar 8-must-fix curve under the same review discipline. |
| Work shape — feature-integration; Analogous prior art | Many touch points; analogous prior art as highest-leverage insight | `keep` | Cycle-2 maint review found cross-bundle consistency TIGHTENED post-PR-1.3 (README↔Rust↔Python on cache_mode = aligned). The prior-art-alignment insight produced MF-001 (IPC sink AbortOnDropHandle) and the .ok().unwrap() sibling-consensus fix. Cycle-1's revisit-but-keep verdict can now firmly transition to keep — the prior-art-alignment gap that prompted the revisit (read_vortex/scan_vortex asymmetry) is the one outstanding work-shape concern, deferred to Phase 3 with concrete fix path. |
| CI green-up approach — crates.io vortex = '0.70.0' | Replace path-dep with version-pinned crates.io | `keep` | Unchanged. PR-1.1 transitioned cleanly; cycle-2 sees no regressions. The transitive arrow-* major version coupling deserves an upgrade-runbook comment (cycle-1 should-fix carries forward) but the decision itself is sound. |
| Final phase plan — 4 phases as drafted | (1) Ratify, (2) PR-13 AExpr pushdown, (3) PR-8 + multi-file, (4) benches/polish | `keep` | Phase 1's boundaries held cleanly through reject+fix cycle. PR-1.2's scope-creep absorption and PR-1.3's 33% inner-loop scope growth are sub-PR-declaration lessons, not phase-split lessons. Phase 2 entry should pre-budget for cross-cutting CI-greenup AND for fixup-PR inner-loop discovery (~30% scope-creep observed pattern). |
| PR-13 architecture — Option B → A via PR-2.6 cutover | Parallel paths first, delete fast path last | `keep` | Unchanged by Phase 1; Phase 2 scope. SpecializedColumnPredicate fast path at predicate.rs (498 LoC) is contained; PR-2.6 deletion mechanics tractable. Scaffolding-marker should-fix on predicate.rs is the natural Phase 2 entry housekeeping. |
| PR-8 leads Parquet on table_statistics | Lead the pattern, no API change (Phase 3 scope) | `keep` | Unchanged. The mem-engine Vortex branch at lp.rs:448-459 will be REFINED in PR-3.1 (file-level table_statistics pruning fires; internal pruning stays disabled) — design unchanged, just extended. |
| Per-phase review-counts — 4 / 4 / 4 / 4 | Max thoroughness; phase-4 preset on every phase-end | `keep` | Cycle-1's revisit-but-keep is REVERSED to firm keep by cycle-2 evidence. Cycle-2's adversarial pass over PR-1.3 caught the cycle-1 MF-006 comment-add reward-hack AND the vortex_err! build break under non-umbrella feature combos — both findings emerged from the second adversarial pass over the same artifact. The 25%-extra cost over 3-vote is small compared to catching arch-vs-impl drift at every checkpoint. Phases 2-3 will deliver NEW code with novel architectural shapes; 4-vote is the calibrated cost. |
| Single Tokio runtime (Polars ASYNC) for Vortex async work | Avoid doubling thread-pool overhead | `keep` | Verified at cycle 2 — sound; session.rs implements correctly with LazyLock-wrapped EXECUTOR + VortexSession::default().with_handle(...) pattern. No second runtime detected anywhere in the diff. |
| mem::transmute between Arrow FFI structs with size+align+length triplet | Compile-time asserts + runtime length check provide safety | `keep` | Phase 1 didn't touch transmute call-sites. The comment-length should-fix (cycle-1 + cycle-2 arch) is about prose, not approach — compress comments, keep the approach. |
| Sorting ColumnPredicates::predicates by column name for deterministic AND-collect | Deterministic ordering is a reproducibility guarantee | `keep` | Confirmed at predicate.rs:50 via sort_by_key(\|(name, _)\| name.as_str()). |
| create_skip_batch_predicate = false for Vortex at planner/lp.rs:448-459 | Vortex's LayoutReader::pruning_evaluation handles zone-level pruning | `keep` | Branch present and well-documented. Phase 3 PR-8 will REFINE (add file-level table_statistics pruning while keeping internal pruning disabled) — design extension, not change. |
| hashbrown 0.16 (Polars) + 0.17 (Vortex transitive) coexistence | BAN against bare PlHashMap::new() is established workaround | `keep` | Cycle-2 BAN compliance verified across the diff. The #[allow(clippy::disallowed_types)] on polars_chunk_to_upstream_record_batch for the upstream-arrow boundary is the right complementary pattern. |
| PolarsInstrumentedVortexReadAt mandatory wrapping | Local/cloud/in-memory all route through it | `keep` | Verified at all three sites (local_file_read_at, cloud_read_at, in_memory_read_at). |
| row_count: usize::try_from(u64).unwrap_or(usize::MAX) clamp pattern | 32-bit edge case clamp | `keep` | Three sites now converged on the same shape (scans.rs:349 + io_sources/vortex/mod.rs:217 + write/strategy.rs:26-29). Cycle-1's revisit-but-keep verdict is now firmly keep. Note: the write-time application at strategy.rs has the cycle-1 should-fix recommendation to error loudly (vs the read-time clamp); captured as should-fix above for Phase 2 re-evaluation. |
| `pub use ::vortex;` (broad upstream re-export at lib.rs:18) | Re-export entire upstream Vortex API so downstream Polars crates don't need a direct vortex dep | `revisit-but-keep` | Cycle-1 arch should-fix; cycle-2 WIDENED via MF-002's macro dependency. Audit shows actual surface is 8 paths across 5 sub-modules — narrowing covers every use including the macro. Re-export shape stays (downstream stable surface); width narrows. Mechanical 1-commit change suitable for Phase 2 entry. |


### Disagreements

_(none — all 4 reviewers agreed on accept verdict)_


### Dropped re-flags (carry-forward items reviewers re-surfaced)

- **MF-001 through MF-006 cycle-1 phase-end must-fix items** — reason: all resolved in PR-1.3 cycle-1 + cycle-2 inner-loop; verified at source by all 4 reviewers. Marked `[x]` in Resolved phase-end must-fix items table.; reference: `Resolved phase-end must-fix items — Phase 1: Ratify + crates.io transition — cycle 1`
- **In-memory ScanSourceRef::Buffer zero-copy (scans.rs:324)** — reason: covered by Deferred work — assessed not-low-effort in PR-1.2; reference: `Deferred work entry`
- **mem::transmute approach in array_bridge.rs** — reason: covered by Accepted tradeoffs — approach is sound; only the comment-length question remains as a should-fix (retained above); reference: `Accepted tradeoffs / r1 traps`
- **RUSTSEC-2024-0436 + 0BSD in deny.toml** — reason: covered as cycle-1 should-fix already absorbed into Surprises with plan-row amendment; reference: `Phase 1 cycle 1 review`
- **PR-13 architecture (Option B → A via PR-2.6)** — reason: Phase 2 scope; out of scope to revisit until Phase 2 begins; reference: `Key decisions row 5`


<details><summary>Full Synthesizer Output JSON (gauntlet schema_version: 1)</summary>

```json
{
  "schema_version": 1,
  "preset": "phase-4",
  "lenses_used": ["spec", "correctness", "maint", "arch"],
  "review_count": 4,
  "review_cycles_this_invocation": 1,
  "prior_cycle_dropped_re_flags": [],
  "unified_findings": [
    {
      "severity": "should-fix",
      "kind": "architecture",
      "file_line": "crates/polars-plan/src/plans/conversion/dsl_to_ir/scans.rs:336",
      "description": "vortex_file_info hardcodes `with_segment_cache(segment_cache())` (the global cache), ignoring `VortexScanOptions::segment_cache`. The streaming-source path at io_sources/vortex/mod.rs:138 correctly threads `self.options.segment_cache.resolve()`. The two code paths now have DIVERGENT semantics for the same scan: the IR-build-time postscript read uses the global cache, but the actual data-decode reads use the user's requested mode. A user passing `cache_mode='off'` still hits the global cache during schema discovery. Cycle-1 arch flagged; still unfixed.",
      "recommended_fix": "Thread `options.segment_cache.resolve()` through vortex_file_info — change signature to accept `&VortexScanOptions` (or the resolved cache), then call `.with_segment_cache(options.segment_cache.resolve())` at line 336. Mirror the streaming-source pattern at io_sources/vortex/mod.rs:138. 5-line mechanical fix. Surface as Phase 2 prerequisite or Phase 1.4 cleanup.",
      "found_by": ["arch"]
    },
    {
      "severity": "should-fix",
      "kind": "layering",
      "file_line": "crates/polars-vortex/src/lib.rs:18",
      "description": "`pub use ::vortex;` re-exports the entire upstream Vortex crate. Cycle-1 arch flagged; cycle-2 confirms PR-1.3 WIDENED the surface by introducing a dependency on `vortex::error::vortex_err!` (a macro). Audit of cross-crate uses shows the actual surface is exactly 8 paths spanning 5 sub-modules: `array::{ArrayRef, VortexSessionExecute, arrow::ArrowArrayExecutor, stream::ArrayStreamAdapter}`, `error::{vortex_err!, VortexResult}`, `file::{Footer, OpenOptionsSessionExt, VortexFile}`, `io::{VortexReadAt, std_file::FileReadAt}`. A 5-sub-module narrowed re-export covers every call site INCLUDING the macro (macros are re-exported by module).",
      "recommended_fix": "Replace `pub use ::vortex;` with `pub mod vortex { pub use ::vortex::{array, error, file, io, session}; }`. Audit existing call sites to verify all 8 paths resolve. The BAN 'No new direct vortex-internal symbol use outside the bridge files' becomes machine-checkable: anything outside the 5 sub-modules fails to compile.",
      "found_by": ["maint", "arch"]
    },
    {
      "severity": "should-fix",
      "kind": "doc-quality",
      "file_line": "crates/polars-stream/src/nodes/io_sinks/writers/vortex/mod.rs:127-137",
      "description": "MF-002 producer-error forwarding comment is 11 lines (~370 chars) — borderline on the project BAN against >100-char justifications. Two distinct concerns entangled: WHY-send-Err-first AND WHY-let_-on-the-send. Plus correctness lens traced the producer/writer await sequence and verified that `producer.await?` ALWAYS sees the producer's terminal value before `write_handle.await` runs — so the producer's PolarsError always wins; the writer's VortexError-wrapped form is silently discarded via AbortOnDrop. The comment's 'whichever fires first' framing AND the Deferred work entry overstate the race.",
      "recommended_fix": "Trim to 3-4 lines: `// Forward the producer error so ArrayStreamAdapter bails instead of finalizing a truncated footer (silent-data-corruption hazard). The producer's original Err always propagates via producer.await? below; the channel-Err only forces the writer to bail before finalize. tx.send failing means writer already shut down; benign.` Also update the Deferred work entry `Vortex sink producer/writer error-path determinism polish` to reflect that the producer-Err path is DETERMINISTIC (producer always wins via await ordering), and the polish opportunity is comment clarity, not error message wording.",
      "found_by": ["correctness", "maint"]
    },
    {
      "severity": "should-fix",
      "kind": "doc-quality",
      "file_line": "crates/polars-stream/src/nodes/io_sinks/writers/vortex/mod.rs:157-162",
      "description": "MF-001 AbortOnDropHandle wrap comment is 6 lines (~360 chars), citing the silent-truncation hazard, the IPC sink at ipc/mod.rs:101, and bare-JoinHandle behavior. The IPC sink itself has ZERO explanatory comment on its analogous wrap. The asymmetric prose burden is firmly on Vortex.",
      "recommended_fix": "Trim to 1-2 lines: `// AbortOnDropHandle so the writer task is aborted on outer-task failure rather than orphaned (would finalize a truncated footer). Mirrors `ipc/mod.rs:101`.` Detailed rationale belongs in a module-level doc-comment, not duplicated inline.",
      "found_by": ["maint", "arch"]
    },
    {
      "severity": "should-fix",
      "kind": "convention",
      "file_line": "crates/polars-stream/src/nodes/io_sources/vortex/builder.rs:54",
      "description": "MF-006's `.ok().unwrap()` resolves the silent-error-swallow BAN but introduces `.unwrap()` in production-path per strict BAN reading. Mitigated by 3-of-4 CSV/IPC/NDJSON sibling consensus; the consensus is the stronger signal. Plus the comment at builder.rs:49-54 (~315 chars) is the verbose outlier — sibling builders use the same pattern with ZERO comment.",
      "recommended_fix": "Either (a) codify the BAN-vs-consensus tension as an Accepted tradeoff in the plan, OR (b) consider a `debug_assert!(self.io_metrics.get().is_none())` belt-and-suspenders so the load-bearing 'multi-scan calls once' claim is enforced inline. Trim the comment to 1-2 lines referencing one canonical sibling.",
      "found_by": ["correctness", "maint"]
    },
    {
      "severity": "should-fix",
      "kind": "overflow",
      "file_line": "crates/polars-vortex/src/write/strategy.rs:26-29",
      "description": "Cycle-1 should-fix #2 specifically recommended ERRORING loudly on `row_block_size` overflow because it's a write-time CONFIGURATION knob (user-provided), not a read-time OBSERVED value. PR-1.3 chose pattern consistency (`usize::try_from(rbs).unwrap_or(usize::MAX)`) over the original recommendation. Defensible by pattern-match but worth re-evaluating: a misconfigured rbs > usize::MAX silently clamps rather than erroring at the call site.",
      "recommended_fix": "Replace with `let rbs_usize = usize::try_from(rbs).map_err(|_| polars_err!(ComputeError: \"row_block_size {} exceeds usize::MAX on this platform\", rbs))?;` so misconfigured u64 row_block_size errors loudly at the write call. Or accept the consistency-over-loudness tradeoff explicitly.",
      "found_by": ["correctness"]
    },
    {
      "severity": "should-fix",
      "kind": "scaffolding",
      "file_line": "crates/polars-vortex/src/read/predicate.rs:1, crates/polars-vortex/src/read/mod.rs:18-22, crates/polars-vortex/src/write/mod.rs:4",
      "description": "Three scaffolding-marker gaps from cycle-1 maint, unfixed in cycle-2 (PR-1.3 scoped must-fix-only): (1) predicate.rs is scheduled for PR-2.6 deletion per the plan; no inline marker. (2) read/mod.rs::metadata is a back-compat shim with one caller (polars-plan/src/dsl/file_scan/mod.rs:21) — inlineable now in ~2 minutes. (3) write/mod.rs:4 references 'PR-10' with no link. Fresh engineers reading these files cold have zero indication they're transitional.",
      "recommended_fix": "Phase 2 entry housekeeping: (a) add module-level scaffolding comment to predicate.rs pointing at PR-2.6; (b) inline `metadata` shim into read/mod.rs proper and delete the shim; (c) strip 'PR-10' reference or replace with upstream URL.",
      "found_by": ["maint", "arch"]
    },
    {
      "severity": "should-fix",
      "kind": "doc-quality",
      "file_line": "crates/polars-vortex/src/read/array_bridge.rs:160-164",
      "description": "Cycle-1 arch flagged the SAFETY comment block as 120+ chars (over BAN spirit). PR-1.3 scoped out; cycle-2 confirms still present (~340 chars across 5 lines). Two distinct claims interleaved: struct layout identity + release-callback ownership semantics. Both load-bearing for soundness, but the BAN threshold flags exactly this length. Similar concern at write/array_bridge.rs:51.",
      "recommended_fix": "Move detailed reasoning into the module-level `//!` doc-comment block at the top of the file. Trim the inline SAFETY to one sentence: `// SAFETY: layout verified by the compile-time asserts at lines 143-144; release callback ownership documented in the module doc-comment.`",
      "found_by": ["maint", "arch"]
    },
    {
      "severity": "should-fix",
      "kind": "boundary",
      "file_line": "py-polars/src/polars/io/vortex/functions.py:242-276",
      "description": "Cycle-1 maint finding (prior-art-alignment gap): `pl.read_vortex` accepts ~10 of `pl.scan_vortex`'s ~22 parameters; docstring says 'See scan_vortex for the full parameter list' but read_vortex silently doesn't forward 12 of them. Fresh engineers following the pointer hit `TypeError: unexpected keyword argument`. Asymmetric with `pl.read_parquet` / `pl.scan_parquet`. PR-1.3 scoped out.",
      "recommended_fix": "Phase 3 (per cycle-1 work-shape revisit-but-keep): either (a) `**kwargs` forwarding pattern matching `read_parquet`, or (b) enumerate the supported subset explicitly in the docstring and explain why others aren't forwarded. Option (a) is prior-art-aligned.",
      "found_by": ["maint"]
    },
    {
      "severity": "should-fix",
      "kind": "coverage",
      "file_line": "crates/polars-vortex/src/read/schema.rs:429-441",
      "description": "`time_extension_days_unit_errors` test asserts nothing — constructs a Nanoseconds `Time` then discards it. Cycle-1 maint flagged; PR-1.3 scoped out. The bail-arm at schema.rs:142-143 is structurally unreachable from public API (Vortex's `Time::new(Days)` panics during construction). Assertion-free test is anti-help.",
      "recommended_fix": "Phase 2-3 absorption: either (a) delete the test and move the documentation into a module-level comment near the bail arm, or (b) construct a Days-unit Time via `DType::Extension` directly (bypassing the panicking constructor) and assert the bail-message contents.",
      "found_by": ["maint"]
    },
    {
      "severity": "should-fix",
      "kind": "weak-exit-criteria",
      "file_line": ".big-plans/vortex-integration.md:184",
      "description": "Phase 1 exit criteria row at line 184 reads '65 Rust tests pass' / '8 Python tests pass' but actual shipped state is 66 Rust + 10 Python (PR-1.1 added 1 Rust; PR-1.2 added 2 Python). Cycle-1 spec flagged with `Amend plan: yes`. Plan-row was not updated across two cycles. Stale numbers will carry into Phase 2 planning.",
      "recommended_fix": "Plan-edit commit: either reframe the criteria as 'at least N tests pass' (durable across phases) or update counts to 66 / 10 to match shipped state.",
      "found_by": ["spec"]
    },
    {
      "severity": "nit",
      "kind": "doc-quality",
      "file_line": ".big-plans/vortex-integration.md:196",
      "description": "Plan-row PR-1.2 'Files touched (expected)' lists 4 files; actual is ~30 (including 19-file polars-vortex cargo fmt sweep + various CI greenup files). Implementation status row at line 333-345 documents the scope expansion as accepted self-flagged scope-creep, so this is documentation-completeness gap not a re-flag.",
      "recommended_fix": "Low-priority cleanup; optionally amend 'Files touched (expected)' to enumerate the broader actual set or add a `(+ ~25 CI-greenup files — see Implementation status)` parenthetical.",
      "found_by": ["spec"]
    },
    {
      "severity": "nit",
      "kind": "scope-creep",
      "file_line": "crates/polars-vortex/README.md",
      "description": "README received heavy whitespace / table-alignment reformatting from dprint sweep — 283 line delta on a documentation file. Substantive content changes (MF-004 + MF-005) are ~10 lines; remaining ~270 lines are cosmetic re-wrap. Each substantive change is harder to bisect against cosmetic churn.",
      "recommended_fix": "For Phase 2 entry: pre-declare dprint table-alignment + markdown re-wrap as a separate mechanical-formatting sub-PR. Don't retroactively unbundle PR-1.3.",
      "found_by": ["spec"]
    },
    {
      "severity": "nit",
      "kind": "overflow",
      "file_line": "crates/polars-vortex/src/read/array_bridge.rs:175 and write/array_bridge.rs:45,64",
      "description": "`if imported.len() as i64 != expected_len` casts `usize → i64` unguarded. For an array length exceeding i64::MAX (~9.2 EB items, theoretical), wraps to negative and bug-detection silently fails. Carry-forward from cycle-1 should-fix; cycle-2 confirms still present.",
      "recommended_fix": "Use `i64::try_from(imported.len()).map_err(|_| polars_err!(ComputeError: \"imported array length {} exceeds i64\", imported.len()))?`. Defer to Phase 2 or 3 — theoretical only.",
      "found_by": ["correctness"]
    },
    {
      "severity": "nit",
      "kind": "overflow",
      "file_line": "crates/polars-vortex/src/read/schema.rs:110",
      "description": "`ArrowDataType::FixedSizeList(Box::new(inner), *size as usize)` casts the Vortex FixedSizeList size field to `usize` unguarded. Hypothetical at current Vortex API (size is u32 today); worth a guard for consistency with row_count/rbs clamps.",
      "recommended_fix": "Replace with `usize::try_from(*size).map_err(|_| polars_err!(ComputeError: \"FixedSizeList size {} exceeds usize::MAX\", size))?` to satisfy the same overflow-discipline. Defer.",
      "found_by": ["correctness"]
    },
    {
      "severity": "nit",
      "kind": "convention",
      "file_line": "crates/polars-vortex/src/write/array_bridge.rs:107",
      "description": "Read- and write-side C-ABI bridges operate at different granularities: read transmutes whole arrays; write builds Field-by-Field with explicit `with_metadata`. Asymmetry is real (RecordBatch::try_new requires explicit schema-array alignment) but undocumented — a reader expecting mirror-image bridges will be surprised.",
      "recommended_fix": "Add a one-line comment at the top of write/array_bridge.rs noting the asymmetry: `// Note: write path builds Field-by-Field because RecordBatch::try_new validates schema-array consistency upfront. Read path transmutes whole arrays because Vortex's to_arrow_schema() carries field metadata already.`",
      "found_by": ["arch"]
    },
    {
      "severity": "nit",
      "kind": "architecture",
      "file_line": "crates/polars-vortex/src/write/options.rs:21",
      "description": "`include_dtype: bool` defaults to `true`. The `false` case is a write-time footgun: produces files unreadable without out-of-band schema. No call-site guard; doc-comment doesn't warn loudly.",
      "recommended_fix": "Expand doc-comment to explain that `false` requires out-of-band schema on read; consider whether to drop the knob from the public surface entirely.",
      "found_by": ["arch"]
    }
  ],
  "disagreements": [],
  "dropped_re_flags": [
    {"topic": "MF-001 through MF-006 cycle-1 phase-end must-fix items", "reason": "all resolved in PR-1.3 cycle-1 + cycle-2 inner-loop; verified at source by all 4 reviewers. Marked `[x]` in Resolved phase-end must-fix items table.", "reference": "Resolved phase-end must-fix items — Phase 1: Ratify + crates.io transition — cycle 1"},
    {"topic": "In-memory ScanSourceRef::Buffer zero-copy (scans.rs:324)", "reason": "covered by Deferred work — assessed not-low-effort in PR-1.2", "reference": "Deferred work entry"},
    {"topic": "mem::transmute approach in array_bridge.rs", "reason": "covered by Accepted tradeoffs — approach is sound; only the comment-length question remains as a should-fix (retained above)", "reference": "Accepted tradeoffs / r1 traps"},
    {"topic": "RUSTSEC-2024-0436 + 0BSD in deny.toml", "reason": "covered as cycle-1 should-fix already absorbed into Surprises with plan-row amendment", "reference": "Phase 1 cycle 1 review"},
    {"topic": "PR-13 architecture (Option B → A via PR-2.6)", "reason": "Phase 2 scope; out of scope to revisit until Phase 2 begins", "reference": "Key decisions row 5"}
  ],
  "phase_artifacts": {
    "summary": "Phase 1 ('Ratify + crates.io transition') of polars-vortex closes coherently at the architectural level after cycle-2 phase-4 retroactive ratification. The cumulative diff covers the 31-commit base + PR-1.1 (crates.io transition + DType::Union arm removal + DType::Variant bail test) + PR-1.2 (Python cache_mode= surface + visitor cfg-gating root-cause fix + 10+ CI-greenup items absorbed) + PR-1.3 (6 phase-end must-fix items + 2 inner-loop must-fix + 2 inner-loop should-fix across 10 work commits in 2 inner-loop cycles). All 6 cycle-1 phase-end must-fix items are verified resolved: MF-001 wraps Vortex sink writer in tokio_handle_ext::AbortOnDropHandle (mirroring IPC sink at ipc/mod.rs:101 — closes outer-task-failure→silent-footer hole); MF-002 forwards producer errors through chunk_tx as Err(vortex_err!(...)) BEFORE bailing (closes producer-error→silent-footer hole); MF-003 replaces u64-as-usize cast with usize::try_from(...).unwrap_or(usize::MAX) at scans.rs:349 mirroring the established clamp at io_sources/vortex/mod.rs:217; MF-004 corrects README field name to segment_cache; MF-005 documents cache_mode= in the README scan_vortex signature; MF-006 aligns set_io_metrics with CSV/IPC/NDJSON 3-of-4 sibling consensus via .ok().unwrap() (cycle-2 inner-loop reroute from cycle-1's reward-hacking comment-only fix). The four settled architectural moves — mem-engine delegates to streaming, single Polars ASYNC Tokio runtime, mem::transmute C-ABI bridge with size+align+length safety triplet, SpecializedColumnPredicate fast path preserved for PR-13 transition — are coherently implemented and unchanged. Cross-bundle consistency on the cache_mode/cache_mode_kind/cache_dedicated_bytes 2-arg pyo3 pair is tight across Python/Rust/.pyi/README. 0 must-fix items in cycle-2. 16 should-fix items (highest-priority: vortex_file_info segment_cache hardcode at scans.rs:336 — divergent IR-time vs streaming-time semantics; pub use ::vortex; can narrow to 5 sub-modules covering the 8 actual paths; producer-error comment overstates the race — actually deterministic per await ordering; scaffolding markers absent on PR-2.6 deletion target + back-compat shim + PR-10 stale reference; .ok().unwrap() BAN-vs-consensus tension worth codifying; long justification comments at sink/builder/array_bridge approaching BAN thresholds). 6 nits. Cumulative deferred items: 6. The integration is coherent, exit criteria all pass, and Phase 2 (PR-13 AExpr pushdown) is unblocked.",
    "surprises": [
      {"what": "All 6 cycle-1 phase-end must-fix items were in PRE-EXISTING 31-commit code, not in PR-1.1 or PR-1.2 directly. Two of those (MF-001, MF-002) were silent-data-corruption hazards in the streaming sink writer-task lifecycle that two prior hand-prompted gauntlet passes + per-PR inner-loop reviews missed. Phase 1's retroactive ratification framing delivered as designed.", "how_handled": "Resolved in PR-1.3. Pattern recorded: prior-art-alignment insight (maint lens) produced MF-001; the feature-integration work-shape's highest-leverage insight was vindicated.", "amend_plan": "already-done"},
      {"what": "PR-1.3's cycle-1 MF-006 fix was a reward-hacking comment-add. Cycle-2 inner-loop caught it via specific code-trace through multi_scan/mod.rs:185-200 + pipeline/initialization.rs:366 + physical_plan/lower_ir.rs:769-775 (single-call-site reality) and routed to the strictly-better .ok().unwrap() fix aligned with CSV/IPC/NDJSON 3-of-4 sibling consensus. Lesson: when adding a justification comment to a silent-error swallow, audit the underlying pattern.", "how_handled": "Resolved cycle-2; documented in PR-1.3 surprises.", "amend_plan": "already-done"},
      {"what": "PR-1.3's cycle-1 MF-002 fix introduced a build break (`vortex::error::vortex_err!` doesn't resolve in polars-stream). The umbrella `cargo check -p polars --features vortex,cloud,parquet,dtype-full` doesn't include `new_streaming`, so polars-stream's vortex sink code wasn't compiled. Verification gap: the right check is `-p polars-stream --features vortex,cloud`.", "how_handled": "Resolved cycle-2 via `polars_vortex::vortex::error::vortex_err!` through the broad re-export. Recommend adding `cargo check -p polars-stream --features vortex,cloud` to the verification checklist for future PRs touching polars-stream.", "amend_plan": "already-done"},
      {"what": "Cycle-2 surfaced a pre-existing E0004 build break under `polars-stream --features vortex` (without `cloud`): the Vortex sink Writeable match's `Writeable::Cloud(_)` arm is `#[cfg(feature = \"cloud\")]` on polars-stream's own `cloud` feature, but the underlying enum's Cloud variant remains visible because `polars-io`'s `file_cache` feature transitively enables `polars-io/cloud`. CI doesn't catch this combo. Predates PR-1.3.", "how_handled": "Deferred (cumulative deferred 6). Tracked for Phase 2 cleanup or follow-up PR.", "amend_plan": "already-done"},
      {"what": "Correctness lens traced the producer/writer error-path race exhaustively and verified the Deferred work entry 'Vortex sink producer/writer error-path determinism polish' MISCHARACTERIZES the behavior. The producer's `.await?` ALWAYS sees the producer's terminal value before `write_handle.await` runs, so the producer's PolarsError always propagates first; the writer's VortexError-wrapped form is silently discarded via AbortOnDrop. Producer-Err path is deterministic; only error-message-WORDING is what the Deferred entry was trying to characterize as 'race-y' but it's not race-y in user-visible behavior.", "how_handled": "Not yet — captured as should-fix above to update both the inline comment and the Deferred work entry. Trivial doc-only correction.", "amend_plan": "yes"},
      {"what": "`pub use ::vortex;` re-export WIDENED via PR-1.3's MF-002 fix introducing a macro dependency (`vortex::error::vortex_err!`). Macros from external crates have looser stability contracts than function signatures, so this is a regression on the layering concern cycle-1 already flagged. But the narrowing recommendation still works WITH the macro because macros are re-exported by module — `pub mod vortex { pub use ::vortex::{array, error, file, io, session}; }` covers every actual use including the macro.", "how_handled": "Captured as should-fix above with concrete narrowing recommendation.", "amend_plan": "no"},
      {"what": "Plan-row PR-1.2 'Files touched (expected)' field still lists 4 files; actual is ~30. Cycle-1 should-fix #1 noted this; PR-1.3 didn't address (must-fix-only). Implementation status carries the truth; the 'expected' field is documentation-completeness drift.", "how_handled": "Captured as nit above. Low priority.", "amend_plan": "no"},
      {"what": "Phase 1 exit criteria row 184 still reads '65 Rust tests' / '8 Python tests' — actual is 66 / 10. Cycle-1 should-fix with `Amend plan: yes`; not updated across two cycles. Stale numbers will carry into Phase 2 planning unless fixed.", "how_handled": "Captured as should-fix above. Plan-edit before Phase 2 entry.", "amend_plan": "yes"}
    ],
    "coverage": {
      "tested_cases": [
        {"case": "All 6 cycle-1 phase-end must-fix items verified resolved at source by all 4 reviewers", "test_location": "Resolved phase-end must-fix items table — Phase 1 cycle 1 (rows 1-6 all marked [x])", "confidence": "high"},
        {"case": "Cache_mode dispatch consistency across Python/Rust/.pyi/README", "test_location": "Cross-bundle grep confirms identical naming and types (cache_mode_kind: &str, cache_dedicated_bytes: Option<u64>) at general.rs:343-365 + functions.py:186-217 + _plr.pyi + README.md:325-332", "confidence": "high"},
        {"case": "VortexCacheMode::{Global, Off, Dedicated} resolve semantics", "test_location": "crates/polars-vortex/src/read/options.rs:81-131 (three unit tests asserting Arc::ptr_eq behaviors)", "confidence": "high"},
        {"case": "C-ABI bridge size_of + align_of asserts + runtime length parity (both directions)", "test_location": "crates/polars-vortex/src/read/array_bridge.rs:143-144,175 + write/array_bridge.rs:40-43", "confidence": "high"},
        {"case": "Channel-Err capacity bound (no deadlock for tx.send(Err(...)))", "test_location": "Verified via futures::channel::mpsc::channel(n) capacity semantics: n + num_senders slots", "confidence": "high"},
        {"case": "No-second-runtime invariant", "test_location": "session.rs:40-49 (Handle from ASYNC); sink mod.rs:163,111 (ASYNC.spawn + async_executor::spawn)", "confidence": "high"},
        {"case": "All-reads-via-PolarsInstrumentedVortexReadAt invariant", "test_location": "crates/polars-vortex/src/read/read_at.rs:106-164 (three factory functions: local/cloud/in-memory all wrap)", "confidence": "high"},
        {"case": "row_count clamp is symmetric across read sites + write strategy", "test_location": "scans.rs:349 + io_sources/vortex/mod.rs:217 + write/strategy.rs:26-29 (three sites converged on the same shape)", "confidence": "high"},
        {"case": "Roundtrip read/write + filter/projection-pushdown + negative-slice", "test_location": "py-polars/tests/unit/io/test_vortex.py:55-132 + crates/polars-vortex/tests/roundtrip.rs", "confidence": "high"}
      ],
      "untested_cases": [
        {"case": "Vortex sink producer-error end-to-end regression (Python-level)", "priority": "high", "why_untested": "Deferred. Python-level test (~30 LoC) would catch MF-001/MF-002 against future refactor regression; smaller scope than the Rust-level harness also deferred."},
        {"case": "vortex_file_info honors VortexScanOptions::segment_cache", "priority": "high", "why_untested": "Carry-forward should-fix; PR-1.3 scoped out. Phase 2 prerequisite or Phase 1.4 cleanup."},
        {"case": "set_vortex_cache_bytes(True/1.5/-1) Python validation", "priority": "low", "why_untested": "Carry-forward cycle-1 should-fix; inconsistent with cache_mode= validator hygiene."},
        {"case": "Empty DataFrame (0-col) write/read roundtrip exercising n_chunks==0 early-return", "priority": "medium", "why_untested": "Carry-forward; trivial scope but no test added."},
        {"case": "32-bit platform regression on row_count + rbs clamps", "priority": "low", "why_untested": "CI doesn't run 32-bit targets; defensive only."},
        {"case": "Multi-file scan + schema-evolution + nested-type + small-int dtype coverage", "priority": "medium", "why_untested": "Phase 3 scope per plan."},
        {"case": "polars-stream --features vortex (without cloud) E0004 build break", "priority": "low", "why_untested": "Pre-existing; deferred. Phase 2 cleanup."},
        {"case": "Hard cache hit/miss counters (Moka API limitation)", "priority": "low", "why_untested": "Moka doesn't expose stats; deferred."}
      ],
      "recommendations": "Phase 2 entry should land a 'Phase 2.0 housekeeping' sub-PR addressing the top 3-4 should-fix items: (1) vortex_file_info segment_cache thread (5-line fix; closes the divergent semantics); (2) narrow `pub use ::vortex;` to 5 sub-modules (mechanical; tightens the layering BAN's machine-checkability); (3) inline the `read::metadata` back-compat shim (2-minute fix, one caller); (4) update plan-row 184 stale exit-criteria numbers (durable plan-edit). Then add the Python-level Vortex sink regression test for MF-001/MF-002 as part of PR-2.1's prerequisite hygiene. Phase 3 takes the broader test-coverage push (Rust-level streaming harness, multi-file, schema-evolution, nested-type, small-int dtypes, read_vortex parameter symmetry). Phase 4 polish: 32-bit overflow regression tests, deny.toml audit, set_global_cache_bytes Arc-swap semantics tests, SAFETY-comment trims."
    },
    "tradeoffs": [
      {"decision": "Reuse existing 31 commits vs rewrite from scratch", "original": "Reuse — 73 tests passing locally; two prior gauntlet review passes already surfaced + fixed substantive bugs", "verdict": "keep", "rationale": "Cycle 2 firms up keep. Net of 6 cycle-1 + 2 cycle-2-inner-loop must-fix items emerging from the 4-vote review IS the most-skeptical case for rewrite — and even that math favors reuse: 8 bounded mechanical fixes (~10 commits in PR-1.3) vs. weeks of feature-parity work in a rewrite. Architectural coherence of the inherited foundation is sound; the four settled moves are well-implemented at the chosen seams. A rewrite of this size would generate a similar 8-must-fix curve under the same review discipline."},
      {"decision": "Work shape — feature-integration; Analogous prior art", "original": "Many touch points; analogous prior art as highest-leverage insight", "verdict": "keep", "rationale": "Cycle-2 maint review found cross-bundle consistency TIGHTENED post-PR-1.3 (README↔Rust↔Python on cache_mode = aligned). The prior-art-alignment insight produced MF-001 (IPC sink AbortOnDropHandle) and the .ok().unwrap() sibling-consensus fix. Cycle-1's revisit-but-keep verdict can now firmly transition to keep — the prior-art-alignment gap that prompted the revisit (read_vortex/scan_vortex asymmetry) is the one outstanding work-shape concern, deferred to Phase 3 with concrete fix path."},
      {"decision": "CI green-up approach — crates.io vortex = '0.70.0'", "original": "Replace path-dep with version-pinned crates.io", "verdict": "keep", "rationale": "Unchanged. PR-1.1 transitioned cleanly; cycle-2 sees no regressions. The transitive arrow-* major version coupling deserves an upgrade-runbook comment (cycle-1 should-fix carries forward) but the decision itself is sound."},
      {"decision": "Final phase plan — 4 phases as drafted", "original": "(1) Ratify, (2) PR-13 AExpr pushdown, (3) PR-8 + multi-file, (4) benches/polish", "verdict": "keep", "rationale": "Phase 1's boundaries held cleanly through reject+fix cycle. PR-1.2's scope-creep absorption and PR-1.3's 33% inner-loop scope growth are sub-PR-declaration lessons, not phase-split lessons. Phase 2 entry should pre-budget for cross-cutting CI-greenup AND for fixup-PR inner-loop discovery (~30% scope-creep observed pattern)."},
      {"decision": "PR-13 architecture — Option B → A via PR-2.6 cutover", "original": "Parallel paths first, delete fast path last", "verdict": "keep", "rationale": "Unchanged by Phase 1; Phase 2 scope. SpecializedColumnPredicate fast path at predicate.rs (498 LoC) is contained; PR-2.6 deletion mechanics tractable. Scaffolding-marker should-fix on predicate.rs is the natural Phase 2 entry housekeeping."},
      {"decision": "PR-8 leads Parquet on table_statistics", "original": "Lead the pattern, no API change (Phase 3 scope)", "verdict": "keep", "rationale": "Unchanged. The mem-engine Vortex branch at lp.rs:448-459 will be REFINED in PR-3.1 (file-level table_statistics pruning fires; internal pruning stays disabled) — design unchanged, just extended."},
      {"decision": "Per-phase review-counts — 4 / 4 / 4 / 4", "original": "Max thoroughness; phase-4 preset on every phase-end", "verdict": "keep", "rationale": "Cycle-1's revisit-but-keep is REVERSED to firm keep by cycle-2 evidence. Cycle-2's adversarial pass over PR-1.3 caught the cycle-1 MF-006 comment-add reward-hack AND the vortex_err! build break under non-umbrella feature combos — both findings emerged from the second adversarial pass over the same artifact. The 25%-extra cost over 3-vote is small compared to catching arch-vs-impl drift at every checkpoint. Phases 2-3 will deliver NEW code with novel architectural shapes; 4-vote is the calibrated cost."},
      {"decision": "Single Tokio runtime (Polars ASYNC) for Vortex async work", "original": "Avoid doubling thread-pool overhead", "verdict": "keep", "rationale": "Verified at cycle 2 — sound; session.rs implements correctly with LazyLock-wrapped EXECUTOR + VortexSession::default().with_handle(...) pattern. No second runtime detected anywhere in the diff."},
      {"decision": "mem::transmute between Arrow FFI structs with size+align+length triplet", "original": "Compile-time asserts + runtime length check provide safety", "verdict": "keep", "rationale": "Phase 1 didn't touch transmute call-sites. The comment-length should-fix (cycle-1 + cycle-2 arch) is about prose, not approach — compress comments, keep the approach."},
      {"decision": "Sorting ColumnPredicates::predicates by column name for deterministic AND-collect", "original": "Deterministic ordering is a reproducibility guarantee", "verdict": "keep", "rationale": "Confirmed at predicate.rs:50 via sort_by_key(|(name, _)| name.as_str())."},
      {"decision": "create_skip_batch_predicate = false for Vortex at planner/lp.rs:448-459", "original": "Vortex's LayoutReader::pruning_evaluation handles zone-level pruning", "verdict": "keep", "rationale": "Branch present and well-documented. Phase 3 PR-8 will REFINE (add file-level table_statistics pruning while keeping internal pruning disabled) — design extension, not change."},
      {"decision": "hashbrown 0.16 (Polars) + 0.17 (Vortex transitive) coexistence", "original": "BAN against bare PlHashMap::new() is established workaround", "verdict": "keep", "rationale": "Cycle-2 BAN compliance verified across the diff. The #[allow(clippy::disallowed_types)] on polars_chunk_to_upstream_record_batch for the upstream-arrow boundary is the right complementary pattern."},
      {"decision": "PolarsInstrumentedVortexReadAt mandatory wrapping", "original": "Local/cloud/in-memory all route through it", "verdict": "keep", "rationale": "Verified at all three sites (local_file_read_at, cloud_read_at, in_memory_read_at)."},
      {"decision": "row_count: usize::try_from(u64).unwrap_or(usize::MAX) clamp pattern", "original": "32-bit edge case clamp", "verdict": "keep", "rationale": "Three sites now converged on the same shape (scans.rs:349 + io_sources/vortex/mod.rs:217 + write/strategy.rs:26-29). Cycle-1's revisit-but-keep verdict is now firmly keep. Note: the write-time application at strategy.rs has the cycle-1 should-fix recommendation to error loudly (vs the read-time clamp); captured as should-fix above for Phase 2 re-evaluation."},
      {"decision": "`pub use ::vortex;` (broad upstream re-export at lib.rs:18)", "original": "Re-export entire upstream Vortex API so downstream Polars crates don't need a direct vortex dep", "verdict": "revisit-but-keep", "rationale": "Cycle-1 arch should-fix; cycle-2 WIDENED via MF-002's macro dependency. Audit shows actual surface is 8 paths across 5 sub-modules — narrowing covers every use including the macro. Re-export shape stays (downstream stable surface); width narrows. Mechanical 1-commit change suitable for Phase 2 entry."}
    ]
  },
  "executive_summary": "Phase 1 ('Ratify + crates.io transition') ACCEPTS at cycle 2. All 4 reviewers (spec, correctness, maint, arch) reach `accept`. 0 must-fix items, 11 should-fix, 6 nits across 17 unified findings (after dedupe). The cumulative Phase-1 artifact — 31-commit base + PR-1.1 (crates.io transition) + PR-1.2 (Python cache_mode= surface + visitor cfg-gating + 10+ CI-greenup items) + PR-1.3 (6 phase-end must-fix items + 2 inner-loop must-fix + 2 inner-loop should-fix across 10 work commits) — is architecturally coherent and correctness-sound. All 6 cycle-1 phase-end must-fix items are verified resolved at source by all 4 reviewers (Resolved phase-end must-fix items table has all rows marked [x]). The four settled architectural moves (mem-engine delegates to streaming; single Polars ASYNC Tokio runtime; mem::transmute C-ABI bridge with size+align+length safety triplet; SpecializedColumnPredicate fast path preserved) are intact. Cross-bundle consistency on cache_mode is tight. The MF-001/MF-002 silent-truncation hazards are closed; correctness lens traced the producer/writer error-path lifecycle exhaustively and verified deterministic behavior (producer error always wins via await ordering — incidentally revealing that the Deferred work entry overstates the race, captured as a should-fix doc-update).\n\n**Key surprises**: (a) cycle-1 reward-hacking on MF-006 was caught and routed to a strictly-better fix in cycle-2 inner loop — process lesson recorded; (b) cycle-1 verification gap on the `vortex_err!` build break revealed that `cargo check -p polars` (umbrella) does NOT activate polars-stream's vortex code without `new_streaming` — recommend adding `-p polars-stream --features vortex,cloud` to the verification checklist; (c) the `pub use ::vortex;` re-export WIDENED via the macro dependency but the 5-sub-module narrowing still works (macros re-export by module).\n\n**Critical tradeoff verdicts**: REUSE 31 COMMITS → firm KEEP (8 must-fix items emerging from cycle-1+2 is the most-skeptical case for rewrite and even that favors reuse). 4/4/4/4 REVIEW COUNTS → firm KEEP (cycle-2's adversarial second pass caught the MF-006 reward-hack + the build break — neither would have surfaced under 3-vote). FEATURE-INTEGRATION WORK SHAPE → KEEP (prior-art alignment tightened; the read_vortex/scan_vortex asymmetry remains as one Phase 3 carry-forward). `pub use ::vortex;` → REVISIT-BUT-KEEP (narrow to 5 sub-modules in Phase 2 entry).\n\n**Top should-fix recommendations for Phase 2 entry housekeeping**: (1) thread `VortexScanOptions::segment_cache` through `vortex_file_info` at scans.rs:336 (5-line fix; closes the divergent IR-time vs streaming-time semantics that partial-honors `cache_mode='off'`); (2) narrow `pub use ::vortex;` to 5 sub-modules (mechanical, tightens layering BAN's machine-checkability); (3) update Deferred work entry to reflect actual error-path determinism; (4) update Plan-row 184 stale 65/8 test count to actual 66/10; (5) inline `read::metadata` back-compat shim (one caller; 2-minute fix); (6) add `cargo check -p polars-stream --features vortex,cloud` to verification checklist.\n\nThe phase-end checkpoint is the right moment for the user to decide between (a) proceeding to Phase 2 (PR-13 AExpr pushdown) directly, (b) inserting a Phase 1.4 cleanup PR for the top should-fix items, (c) amending Phase 1 to absorb the cleanup before moving on, or (d) pausing and resuming in a fresh session.",
  "overall": "accept",
  "must_fix_count": 0,
  "should_fix_count": 11,
  "nit_count": 6
}

```

</details>

## Pending phase-end must-fix items — Phase 1: Ratify + crates.io transition — cycle 3

| Severity | File:line | Description | Implicated PR | Resolved |
|----------|-----------|-------------|---------------|----------|
| must-fix | `.big-plans/vortex-integration.md:404` | PR-1.4 CI-reopen narrative at plan lines 404+408 re-introduces 3 occurrences of the typos-banned token while explaining the typo fix, recreating the exact CI failure. `gh pr checks 1` shows `main` Spell Check FAIL on HEAD `1a244a0f7`. Phase 1 exit criterion (e) not met. | PR-1.4 | [ ] |
| must-fix | `crates/polars-vortex/README.md:244` | README crate-layout block describes `mod.rs` as `VortexFooterRef alias + back-compat metadata re-export` — but PR-1.4 commit `3479c6bdb` inlined the metadata re-export. README is canonical reference doc; fresh engineers will hit `not found` compile error on `use polars_vortex::read::metadata::VortexFooterRef`. | PR-1.4 | [ ] |

## Phase 1: Ratify + crates.io transition — end-of-phase review (cycle 3) — rejected (4-vote)

**Synthesizer output from `/spiral:gauntlet` (`preset=phase-4`, lenses=`spec`+`correctness`+`maint`+`arch`); full Synthesizer Output JSON in the `<details>` block at the end of this section.**

### Executive summary

Overall verdict: REJECT. Two must-fix items prevent Phase 1 close-out, both doc-only and both mechanical to apply. (1) Spec lens caught a typos-CI regression at `.big-plans/vortex-integration.md:404,408` where the narrative explaining the typos fix reintroduces three occurrences of the banned token, recreating the exact CI failure the CI-reopen was meant to close. `gh pr checks 1` shows the `main` Spell Check job still failing on HEAD `1a244a0f7`. The cycle-3 inner-loop CI-reopen 2-vote accepted at confidence:high without verifying actual post-push CI state, which is the procedural surprise driving this reject. (2) Maint lens caught `crates/polars-vortex/README.md:244` still describing a `metadata` back-compat re-export shim that PR-1.4 commit `3479c6bdb` deleted — fresh engineers following the README will write `use polars_vortex::read::metadata::VortexFooterRef` and hit a not-found compile error. README is the canonical reference doc; same severity class as cycle-1's MF-004 and MF-005.

The verdict surfaces a 2-vs-2 split: spec + maint reject on the must-fix items above; correctness (0 findings, all cycle-2 verifications hold) + arch (0 must-fix, 3 should-fix) accept. Synthesizer applies conservative-union — any must-fix forces reject. Notable surprises beyond the must-fix items: the PR-1.4 plan-doc sweep updated only 1 of 5 stale test-count rows (lines 77, 96, 196, 205 still say '65 Rust + 8 Python'); the EXECUTOR Arc pattern at plan:108 mis-specifies what `session.rs:43-49` actually implements (plan-vs-source drift in the opposite direction from usual); the dedicated double-resolve pattern allocates two independent segment caches per scan (flagged from two distinct angles by maint + arch); `morsel_rx.recv()` returning Err is silently treated as clean EOS in vortex `io_sources/mod.rs:173-204` with no documented assumption; and RUSTSEC-2024-0436 enumeration is missing from the PR-1.2 impl-status bullet. The producer-error inline comment finding at `sink/mod.rs:127-137` — flagged independently by spec and maint — drops to `dropped_re_flags` because it's already a Deferred work entry at plan line 1515. Counts: 2 must-fix, 10 should-fix, 6 nits.

### Summary of changes

Phase 1 of polars-vortex integration lands 78 commits / 3834 lines covering: PR-1.1 (path-dep → crates.io `vortex = "0.70.0"`; DType::Union arm removal; DType::Variant bail-test); PR-1.2 (Python `cache_mode=` surface + serde_json non-optional root-cause + 10+ CI-greenup items including ruff/dprint/mypy stubs/cargo fmt sweep/deny.toml 0BSD+RUSTSEC-2024-0436/dsl-schema hashes); PR-1.3 (6 phase-end cycle-1 must-fix items resolved: AbortOnDropHandle wrap, producer-Err forwarding, u64→try_from-clamp at scans.rs:351, README field-name + cache_mode signature, OnceLock::set silent-discard alignment); PR-1.4 (4 cycle-2 cleanup items: segment_cache thread-through, `pub use ::vortex;` narrowed to 5 sub-modules {array, error, file, io, layout}, metadata shim inlined, producer/writer Deferred entry corrected); PR-1.4 CI-reopen (cargo fmt restoration + typo word replacement). Four settled architectural moves intact: mem-engine delegates to streaming with explicit Vortex branch at `lp.rs:448-459`; single Polars ASYNC Tokio runtime; mem::transmute C-ABI bridge with size+align+length triplet; SpecializedColumnPredicate fast path preserved for PR-13 transition. 66 Rust + 10 Python tests pass at source; cycle-3 delta vs cycle-2 is small (PR-1.4 cleanup + CI fix).

### Surprises and discoveries

- **CI-reopen cycle accepted at confidence:high without verifying actual post-push CI state**. `gh pr checks 1` shows `main` Spell Check Typos still failing on HEAD `1a244a0f7` because the typos-fix narrative reintroduces the banned token. (amend_plan: yes)
- **PR-1.4 declared "row-184 stale test counts" fix only updated row 185**. Four sibling locations (lines 77, 96, 196, 205) remain stale. Partial scope shipment. (amend_plan: yes)
- **README.md:244 still references the back-compat `metadata` re-export shim** that PR-1.4 commit `3479c6bdb` deleted. README-vs-source drift. (amend_plan: no — source fix needed)
- **Dedicated double-resolve allocates two independent segment caches per scan**. Flagged by maint (options.rs:67-74 hidden-assumption) + arch (scans.rs:293-302+1033-1039 fragmentation). Cycle-1 PR-1.4 should-fix carry-forward, but reframed now as semantic-divergence not just perf. (amend_plan: yes)
- **Plan spec at line 108 mis-specifies the EXECUTOR Arc pattern**. Spec would dangle Weak immediately; source at `session.rs:40-48` correctly keeps a `LazyLock<Arc<dyn Executor>>` strong ref. Plan-doc fix needed. (amend_plan: yes)
- **morsel_rx.recv() Err-as-EOS is undocumented in vortex io_sources/mod.rs:173-204**. Silent-swallowing risk under upstream behavior change. (amend_plan: no — source comment)
- **RUSTSEC-2024-0436 enumeration missing from PR-1.2 impl-status bullet at plan:339** despite advisory completeness being recurring phase-end expectation. (amend_plan: yes)
- **lib.rs narrowing comment claims "8 paths" but enumerates 12**. Comment-vs-content drift. (amend_plan: no — source comment)
- **Cycle-1 deferred items in plan-commit JSON bodies rather than canonical Deferred work bullets**. Discovery friction. Phase 2.0 housekeeping candidate. (amend_plan: no — Phase 2 entry)
- **2-vs-2 reviewer split on overall verdict**. Both reject votes are doc-only must-fix items — code-clean / doc-drifted phase-end signature. (amend_plan: no — process observation)

### Testing coverage assessment

| Tested case | Test location | Confidence |
|---|---|---|
| All 6 cycle-1 phase-end must-fix items resolved | Resolved phase-end must-fix items table | high |
| segment_cache threaded through both IR-build + streaming | scans.rs:1029-1039 + io_sources/vortex/mod.rs:138 | high |
| pub use ::vortex narrowed to {array, error, file, io, layout} | lib.rs:23 + grep audit 0 violations | high |
| 66 Rust + 10 Python tests pass | grep count | high |
| AbortOnDropHandle wraps sink writer | mod.rs:157-162 | high |
| Producer-Err forwarded via channel-Err | mod.rs:124-145 | high |
| row_count clamp uniformity across 3 sites | scans.rs:349 + io_sources/vortex/mod.rs:217 + write/strategy.rs:28 | high |
| C-ABI bridge size+align+length triplet | read/array_bridge.rs:143-144,175 + write/array_bridge.rs:40-43 | high |

| Untested case | Priority | Why untested |
|---|---|---|
| `gh pr checks 1` green on typos check (exit criterion e) | high | FAIL on HEAD `1a244a0f7` — must-fix #1 blocks this |
| README.md module-path claims compile correctly | high | README:244 stale — must-fix #2 blocks this |
| Vortex sink producer-error Python regression | high | Deferred work entry |
| Rust-level streaming source/sink tests | high | Phase 3 scope |
| vortex_file_info segment_cache regression | high | Carry-forward should-fix |
| Dedicated double-resolve dual-cache behavior | medium | No test |
| morsel_rx.recv() Err-as-EOS contract | medium | No test |
| 32-bit platform row_count >2^32 truncation | low | CI doesn't run 32-bit |
| polars-stream --features vortex (no cloud) E0004 | low | Pre-existing; Deferred |

Recommendations: Repair the 2 must-fix items first (plan:404+408 typo-narrative + README:244 back-compat phrase), verify CI green locally, push, then accept cycle 4. Should-fix items roll into a Phase 2.0 housekeeping sub-PR.

### Tradeoffs re-evaluation

| Decision | Verdict | Rationale |
|---|---|---|
| Reuse existing 31 commits vs rewrite | keep | Cycle-3 firms cycle-2; no new must-fix in source delta. |
| Work shape — feature-integration; Analogous prior art | keep | PR-1.4 pub-use-vortex audit + .ok().unwrap() sibling-consensus alignment held. |
| CI green-up — crates.io vortex = "0.70.0" | keep | Source clean; cycle-3 failure is doc-only. |
| 4 phases as drafted | keep | Boundary held through PR-1.4 + CI-reopen. |
| PR-13 architecture — Option B → A via PR-2.6 | keep | Phase 2 scope. |
| PR-8 leads Parquet on table_statistics | keep | Phase 3 scope. |
| Per-phase review-counts — 4/4/4/4 | revisit-but-keep | Cycle-3 caught what cycle-3 inner-loop missed (CI green verify). Cost-benefit case for 4-vote on cosmetic CI-reopens is marginal. Phase 4 retrospective. |
| Single Tokio runtime (Polars ASYNC) | keep | Source correct; plan:108 spec wrong (surfaced as should-fix architecture-vs-plan-drift). |
| mem::transmute Arrow FFI with size+align+length | keep | Phase 1 didn't touch transmute sites. |
| SpecializedColumnPredicate preserved | keep | Phase 2 scope; scaffolding marker should-fix added. |
| Sorting ColumnPredicates by name | keep | Phase 1 maintained invariant. |
| create_skip_batch_predicate=false for Vortex | keep | Branch unchanged. |
| hashbrown 0.16/0.17 coexistence | keep | BAN compliance verified. |
| PolarsInstrumentedVortexReadAt mandatory | keep | Three factories all wrap. |
| row_count clamp `try_from(u64).unwrap_or(MAX)` | keep | Three sites converged. |
| pub use ::vortex narrowed to 5 sub-modules | keep | PR-1.4 shipped {array, error, file, io, layout}; machine-checkable. |

### Disagreements

| Topic | Synthesizer call |
|---|---|
| Overall verdict — 2 reject (spec, maint) vs 2 accept (correctness, arch) | **reject** per conservative-union: any must-fix forces reject. Two reviewers found distinct must-fix items at distinct file_lines; both doc-only fixes that are quick to apply. |

### Dropped re-flags (carry-forward items reviewers re-surfaced)

| Topic | Reason | Reference |
|---|---|---|
| Vortex sink producer-error inline comment trim (mod.rs:127-137) | covered by Deferred work — flagged by both spec (scope-drift) and maint (should-fix), but the inline comment trim is already captured as "Vortex sink producer-error inline comment trim + minor polish" in Deferred work. | Deferred work:1515 |

<details><summary>Full Synthesizer Output JSON (gauntlet schema_version: 1)</summary>

See archive section `## Phase 1 raw gauntlet responses (archive) — ### Cycle 3 — preset=phase-4 — reject` above (committed in `d687da096`) for the full Synthesizer Output JSON. Inline copy elided to avoid plan-file bloat; archive is the durability anchor per Step 3.2.5.

</details>

## Deferred work

Items deferred from inner-loop or phase-end reviews. Each row is fed into subsequent review prompts as "known accepted tradeoff — don't re-flag."

Seeded with carry-forward items from the existing plan's §13 that may surface as should-fix during reviews:

- **`polars-arrow::ffi::ArrowArray::from_ffi_parts(...)` upstream API**: would let polars-vortex drop the `mem::transmute` in `read/array_bridge.rs` + `write/array_bridge.rs`. Deferred: scope is a separate polars upstream contribution; current safety triplet (size+align asserts + runtime length check) is accepted (see `Accepted tradeoffs`).
- **Vortex `Duration` extension dtype** (upstream Vortex addition): would unblock Polars `Duration` pushdown. Deferred to a future Vortex release.
- **`takeable_rows_provider` morsel size**: defaults to IPC's 122,880 rows; Vortex's natural zone block is 8,192. Deferred — low ROI, may surface during Phase 4 benches if measurement shows it matters.
- **~~Workspace `vortex = { path = "..." }` → crates.io version~~**: **RESOLVED in PR-1.1.** Vortex 0.70.0 is published; the migration is in Phase 1 scope, not deferred.
- **~~Pre-existing visitor feature-gating~~ in `polars-python::lazyframe::visitor::nodes.rs::scan_type_to_pyobject`**: **RESOLVED in PR-1.2 commit `6f0fe06a9`** at the Cargo.toml level (making `serde_json` non-optional in `polars-python`) rather than in `visitor/nodes.rs` itself. The actual file `visitor/nodes.rs` was unchanged because the cfg-gating issue lived in the dep declaration, not the call sites. Plan-vs-reality drift was caught by PR-1.2 gauntlet cycle 1 nit #8 (lens=fresh, scope-drift).
- **In-memory `ScanSourceRef::Buffer` zero-copy** (`dsl_to_ir/scans.rs:323` does `buf.as_slice().to_vec()` then hands the owned `Vec<u8>` to `in_memory_read_at`): considered in PR-1.2, deferred as not-low-effort. The fix requires changing `in_memory_read_at`'s signature to accept a refcounted slice (e.g., `Arc<[u8]>` or polars' `MemSlice`) and threading the change through Vortex's `ByteBuffer::from` constructor. Modest effort, modest ROI (only the in-memory scan path; cloud/local already zero-copy). Revisit in PR-14 benches if measurement shows the copy matters; otherwise leave permanently deferred.
- **Rust-level tests for the polars-stream Vortex source/sink** (currently only Python): deferred to Phase 3 or follow-up.
- **Hard-cached segment-cache hit count test**: Moka's `Cache` doesn't expose hit/miss stats; would need wrapping in `InstrumentedSegmentCache` from upstream Vortex. Deferred.
- **PR-1.1 Python local-verification** (criterion b "8 Python tests pass locally"): this worktree's env has no `maturin` / `pytest` / installed py-polars wheel. Subsumed by Phase 1 exit criterion (e) which runs `gh pr checks 1` (the spiraldb/polars CI runs the full `test-python.yml` matrix against the pushed PR). Resolution: CI verification IS the Python test execution; no further local work required. (Deferred from PR-1.1 gauntlet cycle 1 should-fix #2, 2026-05-15.)
- **PR-1.2 Rust dispatch tighten + Rust unit test** (`crates/polars-python/src/lazyframe/general.rs:334-359`): pyo3 `new_from_vortex` match silently ignores `cache_dedicated_bytes` for `('global', _)` / `('off', _)` arms. Defensive `('dedicated', None)` and `(other, _)` arms are unreachable from Python's `_resolve_cache_mode` and have no Rust test coverage. Two paths: (a) tighten arms to require `None` for non-dedicated kinds with explicit error, (b) downgrade unreachable arms to `debug_assert!` / `unreachable!`. Plus add a Rust `#[test]` exercising all four arms via direct `PyLazyFrame::new_from_vortex` construction. Modest scope (~30 LoC + test infrastructure); deferred to a follow-up PR because PR-1.2 already grew significantly beyond planned scope absorbing CI-greenup. (Deferred from PR-1.2 gauntlet cycle 1 should-fix #3, 2026-05-15.)
- **Vortex sink producer-error inline comment trim + minor polish** (`crates/polars-stream/src/nodes/io_sinks/writers/vortex/mod.rs:127-145`): the inline comment introduced with MF-002 (~370 chars across 11 lines) is on the borderline of the project BAN against >100-char justifications. CORRECTNESS-LENS NOTE (cycle-2 phase-end review): the comment's "whichever fires first" framing is inaccurate — the producer/writer error path is actually DETERMINISTIC under all error scenarios. Tracing the await sequence: `producer.await?` at the join site polls the producer's JoinHandle to completion before `write_handle.await` runs. When the producer returns `Err(e)`, the user-visible error is always the original PolarsError; the VortexError-wrapped form sent through the channel is silently discarded when `?` unwinds and AbortOnDrop fires on `write_handle`. The channel-Err's role is to force the writer to bail before finalizing the footer (defense in depth), NOT to provide an alternate user-facing error path. Polish opportunity: trim the comment to 3-4 lines and correct the framing. Modest doc-only effort; out of PR-1.4 scope. (Deferred from PR-1.3 inner-loop cycle 1 should-fix #5 + corrected by PR-1.4 cycle-2 review, 2026-05-16.)
- **Vortex sink producer-error regression test** (Python-level): MF-001 + MF-002 in PR-1.3 alter observable error-path behavior of `sink_vortex` (now produces a non-OK error on producer failure instead of a truncated-but-valid Vortex file). The existing Deferred work entry "Rust-level tests for the polars-stream Vortex source/sink" covers Rust-level harness construction. A NARROWER Python-level test (e.g., `py-polars/tests/unit/io/test_vortex.py` triggering producer error mid-stream via misaligned chunks or unsupported dtype and asserting `pl.exceptions.ComputeError` containing 'vortex sink producer' + invalid/absent on-disk file) would protect the MF-001/MF-002 guarantees without the Rust harness. Modest scope (~30 LoC test); deferred because this worktree has no installed py-polars wheel for iterative test development. (Deferred from PR-1.3 inner-loop cycle 1 should-fix #6, 2026-05-15.)
- **Vortex sink Writeable match non-exhaustive under `polars-stream --features vortex` (without `cloud`)** (`crates/polars-stream/src/nodes/io_sinks/writers/vortex/mod.rs:91`): `Writeable::Cloud(_)` arm is `#[cfg(feature = "cloud")]` on polars-stream's own `cloud` feature, but the underlying enum's Cloud variant remains visible because `polars-io` (a non-optional polars-stream dep with `features = ["async", "file_cache"]`) enables `polars-io/cloud` transitively via `file_cache`. `cargo check -p polars-stream --features vortex` fails with E0004; `cargo check -p polars --features vortex,cloud,parquet,dtype-full` is clean because that combo keeps `polars-stream/cloud` on. Predates PR-1.3 (commit `bbe16b34a` introduced the cloud sink). Two fix paths: (a) make polars-stream's `vortex` feature transitively enable `cloud` (mirroring how `parquet` may handle it), or (b) add a fall-through wildcard arm in the Vortex sink match. Out of PR-1.3 scope (a 6-must-fix patch); track for Phase 2 cleanup or a follow-up PR. (Deferred from PR-1.3 inner-loop cycle 2 should-fix #1, 2026-05-15.)

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
