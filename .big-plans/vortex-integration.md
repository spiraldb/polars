# Vortex Integration into Polars — big-plans plan

> Continuation of [spiraldb/polars#1](https://github.com/spiraldb/polars/pull/1) (`vortex-integration`, 31 existing commits, +6,497/-80, 73 tests). big-plans takes over the remaining work — retroactive ratification + CI green-up + PR-13 aggressive AExpr pushdown + PR-8 file-stats + PR-6 multi-file/nested coverage + PR-14 benches — and lands as one squash-merged PR onto `spiraldb:main`.

## Current State

```yaml
status: executing
branch: vortex-integration
planning_sub_flow: null
current_phase: "Ratify + crates.io transition"
phase_index: 1
current_pr: PR-1.3
pr_index: 3
outstanding_must_fix: 5
deferred_items_total: 3
last_user_touchpoint: 2026-05-15T21:00:00Z
last_user_touchpoint_what: "phase-end reject — re-open PR-1.3 to address 6 must-fix items"
subagent_invocations_this_pr: 0
subagent_invocations_total: 10
review_cycles_this_pr: 0
phase_entry_sha: 657c78c97
phase_end_cycle: 1
phase_end_reject_cycles: 0
last_phase_end_verdict: reject
last_commit: 530c963b0
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
| 1 | Ratify + crates.io transition | Retroactive 4-vote gauntlet of cumulative diff vs `main` + path-dep → crates.io migration + cheap polish | (a) 4-vote phase-end review accepts. (b) `cargo check -p polars --features vortex,cloud,parquet,dtype-full` clean. (c) `cargo test -p polars-vortex --features dtype-date,dtype-datetime,dtype-time,dtype-decimal` → 65 Rust tests pass. (d) `pytest py-polars/tests/unit/io/test_vortex.py` → 8 tests pass. (e) `gh pr checks 1 --repo spiraldb/polars` shows green for Rust + Python core checks. | 3-4 | **4-vote** |
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

## Pending phase-end must-fix items — Phase 1: Ratify + crates.io transition — cycle 1

| Severity | File:line | Description | Implicated PR | Resolved |
|----------|-----------|-------------|---------------|----------|
| must-fix | `crates/polars-stream/src/nodes/io_sinks/writers/vortex/mod.rs:135-146` | Vortex sink's `write_handle = ASYNC.spawn(...)` returns a bare tokio::task::JoinHandle, not wrapped in `tokio_handle_ext::AbortOnDropHandle`. Divergent from the IPC sink (ipc/mod.rs:101-112) which DOES wrap. If the ou... | PR-1.3 | [ ] |
| must-fix | `crates/polars-stream/src/nodes/io_sinks/writers/vortex/mod.rs:113-128` | Silent data corruption: when `dataframe_to_vortex_chunks(&df)?` returns Err inside the producer task, `?` propagates the error and drops `tx` (the chunk channel). The writer task's `ArrayStreamAdapter` sees channel cl... | PR-1.3 | [ ] |
| must-fix | `crates/polars-plan/src/plans/conversion/dsl_to_ir/scans.rs:345` | `let row_count = vxf.row_count() as usize;` violates the explicit project BAN: 'No `as`-cast `u64 → usize` on row counts. Use `usize::try_from(row_count).unwrap_or(usize::MAX)`'. On 32-bit platforms with Vortex files ... | PR-1.3 | [ ] |
| must-fix | `crates/polars-vortex/README.md:297` | README documents the Rust struct field as `pub cache: VortexCacheMode` but the actual field is `pub segment_cache: VortexCacheMode` (read/options.rs:28). The rename was intentional (read/options.rs:26-27 comment expla... | PR-1.3 | [ ] |
| must-fix | `crates/polars-vortex/README.md:325-332` | The documented `pl.scan_vortex(...)` signature in the README is missing the `cache_mode=` parameter that PR-1.2 shipped — the headline new public-API surface of this phase. README is the canonical reference doc and is... | PR-1.3 | [ ] |
| must-fix | `crates/polars-stream/src/nodes/io_sources/vortex/builder.rs:49` | `let _ = self.io_metrics.set(io_metrics);` silently swallows the `Err(io_metrics)` returned by `OnceLock::set` when called twice. Violates the project BAN: 'Silent error swallowing: `let _ = fallible_op()` ... without... | PR-1.3 | [ ] |

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
