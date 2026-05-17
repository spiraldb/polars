# polars-vortex

Native [Vortex](https://vortex.dev) file-format support for [Polars](https://pola.rs) — a
first-class peer of `polars-parquet`.

```toml
# Cargo.toml
[dependencies]
polars = { version = "...", features = ["vortex"] } # local files
polars = { version = "...", features = ["vortex", "cloud"] } # + s3:// / gs:// / az://
```

```python
import polars as pl

# Read
lf = pl.scan_vortex("data.vortex")
lf = pl.scan_vortex("s3://bucket/data.vortex", storage_options={...})
df = (
    lf
    .filter(pl.col("user_id") == 42)         # → pushed to Vortex
    .filter(pl.col("name").str.starts_with("ada"))  # → pushed (LIKE)
    .slice(-100, 50)                         # → pushed (negative slice)
    .select(["user_id", "name", "score"])    # → pushed (projection)
    .collect()
)

# Write
df.write_vortex("out.vortex")                # eager
lf.sink_vortex("out.vortex")                 # streaming
lf.sink_vortex(pl.PartitionBy("base/", by=["year", "month"]))  # partitioned

# Tune the process-global decompressed-segment cache (default 512 MiB)
pl.set_vortex_cache_bytes(2 * 1024**3)       # 2 GiB
```

## Why Vortex

Vortex is a columnar file format designed for **rich pushdown**: an expression IR, layout-aware zone
pruning, BtrBlocks-style adaptive compression, and a cross-query segment cache that holds
_decompressed_ data between scans of the same file. The integration is structured so that Polars
uses _all_ of these properties — not just "Vortex as another Parquet".

What this looks like in practice:

- **Filter pushdown**: an AExpr-direct convertor walks the predicate's `Arena<AExpr>` and emits Vortex
  `Expression`s for the shapes Vortex can represent — column references, scalar literals, the six
  comparison operators (`Eq`/`NotEq`/`Lt`/`LtEq`/`Gt`/`GtEq`), boolean combinators (`And`/`Or`/`Not`),
  null checks (`IsNull`/`IsNotNull`), numeric addition (`Plus → checked_add`), same-kind `CAST`
  (`Primitive↔Primitive` / `Bool↔Bool` / `Utf8↔Utf8` under `Strict` options), and struct field access
  (`col.struct.field("inner")`). The Expression is handed to `ScanBuilder::with_filter`; inside
  Vortex, `LayoutReader::pruning_evaluation` consults per-zone statistics and skips chunks that can't
  satisfy the predicate — _without decompressing them_. Multiple type-safety gates (bitwise-vs-logical,
  numeric-only Plus, kind-compatible CAST, same-PType comparison) refuse pushdown for shapes Vortex
  would scan-time-error on; the multi-scan layer always re-applies the full predicate post-decode so
  partial pushdown is always _safe_.
- **Negative slice pushdown**: `lf.tail(N)` becomes `ScanBuilder::with_row_range(...)`, not "decode
  everything and take the last N". The file's row count comes from the footer (free).
- **Segment cache reuse**: second and subsequent scans of the same file skip decompression for any
  zone already in the cache. Process-global, tunable.

## How it plugs into Polars

The integration runs entirely on Polars' existing infrastructure — no second Tokio runtime, no
shadow scheduler, no extra binding layer beyond Polars' standard `FileScanIR` / `FileWriteFormat` /
`FileReaderBuilder` / `FileWriterStarter` traits.

```
LazyFrame::scan_vortex(path, args)
  → DslBuilder::scan_vortex
    → FileScanDsl::Vortex { options }
      → vortex_file_info (DSL → IR: open file, derive schema, cache Footer)
        → FileScanIR::Vortex { options, metadata: Option<Arc<Footer>> }
          → polars-mem-engine delegates to polars-stream (lp.rs:1206)
            → lower_ir.rs builds a VortexReaderBuilder
              → VortexFileReader (impl FileReader)
                ├─ initialize(): VortexOpenOptions::open(read_at).await
                │   read_at = PolarsInstrumentedVortexReadAt
                │     (wraps vortex_io::FileReadAt / ObjectStoreReadAt
                │      with IOMetrics + with_concurrency_budget)
                └─ begin_read(args):
                    projection  → vortex::expr::pack(get_item(...))
                    predicate   → builder.aexpr_filter (AExpr-direct convertor result
                                  computed at IR-build time in lower_ir.rs; PR-2.6
                                  cutover deleted the legacy
                                  SpecializedColumnPredicate path)
                    pre_slice   → ScanBuilder::with_row_range
                    .into_array_stream() → Stream<ArrayRef>
                    for each chunk:
                      ArrowArrayExecutor::execute_record_batch
                        → upstream arrow_array::RecordBatch
                      → C-ABI mem::transmute (read/array_bridge.rs)
                        → polars-arrow chunks
                      → DataFrame → Morsel → FileReaderOutputSend::send_morsel
```

Sink mirrors the same shape: `LazyFrame::sink_vortex(path)` →
`FileWriteFormat::Vortex(Arc<VortexWriteOptions>)` → `VortexWriterStarter` (`FileWriterStarter`
impl) → morsels are converted to Vortex `ArrayRef`s by the reverse C-ABI bridge in
`write/array_bridge.rs`, streamed through a `futures::channel::mpsc<ArrayRef>`, wrapped in
`ArrayStreamAdapter`, and handed to `VortexWriteOptions::write(tokio_file, stream).await` on Polars'
`ASYNC` runtime.

## The four key design decisions

1. **One Tokio runtime, not two.** All Vortex async I/O runs on Polars' global `ASYNC` runtime via
   Vortex's `Handle` adapter built from `ASYNC.handle()` once at startup in [`session.rs`]. We
   deliberately avoid:
   - `CurrentThreadRuntime` (would bottleneck multi-pipeline streaming).
   - Spawning a second multi-thread Tokio (doubles thread count, confuses `object_store`'s
     ambient-runtime selection, no win).
   - A custom `Executor` impl over Polars' `polars-stream::async_executor` (no blocking pool, so
     cloud reads would hang).

2. **Arrow C Data Interface as a zero-copy bridge.** Both `polars-arrow::ffi::ArrowArray` and
   upstream `arrow_array::ffi::FFI_ArrowArray` are `#[repr(C)]` with the standard 9-field Arrow C
   ABI layout. We `mem::transmute` between them — moves the ~80-byte struct, leaves the buffers in
   place. Compile-time `size_of` assertions enforce struct identity. Same trick `vortex-duckdb` uses
   with DuckDB.

   Read-side ([`read/array_bridge.rs`]):
   ```
   vortex ArrayRef
   → ArrowArrayExecutor::execute_record_batch(&upstream_schema)
     → upstream arrow_array::RecordBatch
       → arrow_array::ffi::to_ffi(&ArrayData) → FFI_ArrowArray
         → mem::transmute → polars_arrow::ffi::ArrowArray
           → polars_arrow::ffi::import_array_from_c(array, dtype)
             → polars-arrow Box<dyn Array>
               → Series → Column → DataFrame
   ```

   Write-side ([`write/array_bridge.rs`]):
   ```
   polars-arrow Box<dyn Array>
   → polars_arrow::ffi::export_array_to_c → polars_arrow::ffi::ArrowArray
     → mem::transmute → FFI_ArrowArray
       → arrow_array::ffi::from_ffi(array, &schema) → ArrayData
         → make_array → upstream arrow_array::ArrayRef
           → StructArray → vortex ArrayRef (via FromArrowArray)
   ```

3. **`PolarsInstrumentedVortexReadAt` as a thin decorator** ([`read/read_at.rs`]) — wraps Vortex's
   native `FileReadAt` (local) and `ObjectStoreReadAt` (cloud) and splices in:
   - `polars_io::pl_async::with_concurrency_budget(1, ...)` around every `read_at`, so Vortex reads
     share Polars' global cross-format concurrency cap with Parquet et al.
   - `OptIOMetrics::record_io_read(len, fut)` for the same `bytes_requested` / `bytes_received` /
     `io_timer` instrumentation that `pl.explain()` and verbose mode surface for other formats.

   For cloud, the `Arc<dyn ObjectStore>` is constructed via `polars_io::cloud::build_object_store`,
   so all `CloudOptions` semantics (auth, retry, region overrides, credential providers) are honored
   — the same way Parquet's cloud reads work. No buffer copy.

4. **AExpr-direct filter pushdown** (
   [`polars-plan/src/plans/aexpr/predicates/vortex_convertor.rs`](../polars-plan/src/plans/aexpr/predicates/vortex_convertor.rs))
   — walks the predicate's `Arena<AExpr>` at IR-build time in
   [`polars-stream/src/physical_plan/lower_ir.rs`](../polars-stream/src/physical_plan/lower_ir.rs)
   (the `FileScanIR::Vortex` arm). The resulting Vortex `Expression` (or `None`) is attached to
   `VortexReaderBuilder.aexpr_filter` and consumed by `VortexFileReader::begin_read` →
   `ScanBuilder::with_filter`. Coverage:

   | Polars `AExpr` shape                                | Vortex `Expression`                       | Gates                              |
   | --------------------------------------------------- | ----------------------------------------- | ---------------------------------- |
   | `Column(name)`                                      | `get_item(name, root())`                  | virtual-column refuse in lower_ir |
   | `Literal(Scalar)`                                   | `lit(polars_scalar_to_vortex(...))`       | none                               |
   | comparisons (Eq/NotEq/Lt/LtEq/Gt/GtEq)              | `eq`/`not_eq`/`lt`/`lt_eq`/`gt`/`gt_eq`   | pairwise-equal-PType + schema       |
   | logical AND/OR (And/Or/LogicalAnd/LogicalOr)        | `and`/`or`                                | bitwise-vs-logical schema gate      |
   | numeric addition (Plus)                             | `checked_add`                             | numeric + pairwise-equal-PType      |
   | same-kind CAST (Strict only)                        | `cast(child, vortex_dtype)`               | source-kind + Strict-options gates  |
   | struct field access (`col.struct.field("inner")`)   | `get_item(field_name, struct_expr)`       | schema-membership gate              |
   | `IsNull` / `IsNotNull`                              | `is_null` / `is_not_null`                 | none                                |
   | `Not`                                               | `not`                                     | boolean-only schema gate            |

   Pushdown is refused (returns `None` → residual) for unhandled shapes (Sort, Gather, Filter,
   Agg, Ternary, AnonymousFunction, Over, Rolling, temporal extracts, etc.), for non-Strict
   `CastOptions`, for cross-kind CAST, for cross-PType arithmetic/comparison, and for predicates
   referencing hive partition columns or virtual columns (row_index, include_file_paths) at the
   `lower_ir.rs` guard. The reader advertises `ReaderCapabilities::PARTIAL_FILTER`, so Polars'
   multi-scan layer always re-applies the original full predicate post-decode. Result: pushdown
   is always _safe_, just sometimes _partial_. Historical note: PR-2.6 deleted the previous
   `SpecializedColumnPredicate`-derived path (a parallel fast path during PR-13.1–.5).

## Cargo features

| Feature      | Default | Effect                                                                                                                                                                    |
| ------------ | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cloud`      | off     | Enables `s3://` / `gs://` / `az://` / `http(s)://` reads. Pulls `vortex/object_store` and `polars-io/cloud`. Cloud _sink_ is not yet wired (errors with a clear message). |
| `serde`      | off     | Crate-wide `Serialize` / `Deserialize` on the option types — required when `polars-plan/serde` is enabled.                                                                |
| `dsl-schema` | off     | `schemars::JsonSchema` derives on option types — required when `polars-plan/dsl-schema` is enabled.                                                                       |

The umbrella `polars` crate's `vortex` feature pulls in `polars-lazy/vortex` and turns on
`new_streaming` (Vortex scans go through the streaming engine).

## Configuration

### Process-global segment cache

Vortex's segment cache stores _decompressed_ columnar segments across queries on the same file — one
of the biggest perf wins over Parquet's "decompress every time" model. The cache is process-global;
size controlled via:

```python
pl.set_vortex_cache_bytes(2 * 1024**3)   # 2 GiB
pl.set_vortex_cache_bytes(0)             # disable
```

…or via the `POLARS_VORTEX_CACHE_BYTES` environment variable (default 512 MiB). Eviction is tiny-LFU
(good fit for "read the same file many times" workloads).

Memory note: the 512 MiB default is meaningful resident memory. If you're tight on RAM, lower it
explicitly (or `set_vortex_cache_bytes(0)`).

### Concurrency

`PolarsInstrumentedVortexReadAt` routes every read through Polars' `with_concurrency_budget`,
controlled by `POLARS_CONCURRENCY_BUDGET` (same as other Polars I/O). The per-scan parallelism is
controlled by `VortexScanOptions::scan_concurrency` — set to `Some(n)` to call
`ScanBuilder::with_concurrency(n)`, or leave as `None` to let Vortex pick a default based on the
layout's natural splits.

### Cloud auth

Standard Polars `storage_options=` — credentials, retry config, endpoint overrides, all flow through
`polars_io::cloud::build_object_store`.

## Pushdown coverage at a glance

| Pushdown                                               | Status                          | Path                                                                                   |
| ------------------------------------------------------ | ------------------------------- | -------------------------------------------------------------------------------------- |
| Projection (column subset)                             | ✅                              | `polars Projection` → `vortex::expr::pack(get_item(...))`                              |
| Slice (positive)                                       | ✅                              | `Slice::Positive` → `ScanBuilder::with_row_range`                                      |
| Slice (negative, e.g. `.tail(N)`)                      | ✅                              | `restrict_to_bounds(row_count)` (footer-cached row count)                              |
| Filter — `==`, `Between`, `is_in`                      | ✅                              | `SpecializedColumnPredicate` → Vortex `Expression`                                     |
| Filter — `starts_with`, `ends_with`                    | ✅                              | LIKE pattern (wildcard-safe)                                                           |
| Filter — temporal (`Date`, `Datetime`, `Time`) scalars | ✅ (when `dtype-*` features on) | Vortex Date/Time/Timestamp extension scalars                                           |
| Filter — `Decimal` scalars                             | ✅ (when `dtype-decimal` on)    | Vortex `DecimalValue::I128`                                                            |
| Filter — `Duration` scalars                            | ❌ residual                     | Vortex has no Duration extension dtype yet                                             |
| Filter — regex                                         | ❌ residual                     | Vortex `like` doesn't do regex                                                         |
| Filter — arithmetic (`col + 1 > 5`)                    | ❌ residual                     | AExpr traversal not yet wired (PR-13)                                                  |
| Filter — `CAST(col, ...)`                              | ❌ residual                     | AExpr traversal (PR-13)                                                                |
| Filter — struct field access                           | ❌ residual                     | AExpr traversal (PR-13)                                                                |
| Zone-level pruning                                     | ✅                              | Vortex's `LayoutReader::pruning_evaluation` when given any filter                      |
| Hive partitioning                                      | ✅ (free)                       | `UnifiedScanArgs::hive_options`                                                        |
| Schema evolution                                       | ✅ (free)                       | `UnifiedScanArgs::{cast_columns_policy, missing_columns_policy, extra_columns_policy}` |
| Row index                                              | ✅ (free)                       | `UnifiedScanArgs::row_index` (attached post-decode)                                    |

Residual predicates are always re-applied by Polars' multi-scan layer post-decode — partial pushdown
is correct, never _less correct_ than no pushdown.

## Crate layout

```
crates/polars-vortex/
├── README.md
├── Cargo.toml
└── src/
    ├── lib.rs                       # re-exports + `vortex` umbrella passthrough
    ├── session.rs                   # global VortexSession + global Moka segment cache
    ├── read/
    │   ├── mod.rs                   # VortexFooterRef = Arc<Footer> type alias
    │   ├── options.rs               # VortexScanOptions, VortexCacheMode (with resolve())
    │   ├── schema.rs                # Vortex DType → polars-arrow ArrowSchema walker
    │   ├── read_at.rs               # PolarsInstrumentedVortexReadAt decorator
    │   ├── predicate.rs             # ColumnPredicates → Vortex Expression
    │   └── array_bridge.rs          # upstream RecordBatch → DataFrame via C-ABI
    └── write/
        ├── mod.rs
        ├── options.rs               # VortexWriteOptions + VortexCompression
        ├── strategy.rs              # build_write_options: public opts → Vortex's WriteOptions
        ├── array_bridge.rs          # polars-arrow Array → upstream ArrayRef via C-ABI
        ├── df_to_stream.rs          # DataFrame → Vec<ArrayRef> + DType derivation
        ├── sink_writer.rs           # VortexSink enum (Local + Cloud → unified VortexWrite)
        └── writer.rs                # eager write_vortex(&df, path, &options)
```

The streaming source/sink nodes live in `polars-stream` (so they can see the streaming engine's
internals):

```
crates/polars-stream/src/nodes/
├── io_sources/vortex/
│   ├── mod.rs                       # VortexFileReader (FileReader impl)
│   └── builder.rs                   # VortexReaderBuilder (FileReaderBuilder impl)
└── io_sinks/writers/vortex/
    └── mod.rs                       # VortexWriterStarter (FileWriterStarter impl)
```

And the IR / DSL touchpoints sit alongside the existing format variants:

- `polars-plan/src/dsl/file_scan/mod.rs` — `FileScanDsl::Vortex` + `FileScanIR::Vortex`
- `polars-plan/src/dsl/options/mod.rs` — `FileWriteFormat::Vortex`
- `polars-plan/src/plans/conversion/dsl_to_ir/scans.rs::vortex_file_info`
- `polars-plan/src/dsl/builder_dsl.rs::DslBuilder::scan_vortex`
- `polars-lazy/src/scan/vortex.rs` — `LazyFrame::scan_vortex` + `ScanArgsVortex`
- `polars-python/src/lazyframe/general.rs` — `new_from_vortex`, `sink_vortex`
- `py-polars/src/polars/io/vortex/{__init__,functions}.py` — `pl.scan_vortex` / `pl.read_vortex` /
  `pl.set_vortex_cache_bytes`
- `py-polars/src/polars/{lazyframe,dataframe}/frame.py` — `LazyFrame.sink_vortex` /
  `DataFrame.write_vortex`

## API surface — Rust

The Rust API in this crate is intentionally low-level; for most users, the Polars DSL
(`LazyFrame::scan_vortex`) is the right entry point.

```rust
// Read options — embedded in FileScanIR::Vortex
pub struct VortexScanOptions {
    pub schema: Option<SchemaRef>,
    pub push_predicate: bool,            // default true — translate predicates to Vortex Expressions
    pub initial_read_size: Option<usize>,
    pub scan_concurrency: Option<NonZeroUsize>,
    pub segment_cache: VortexCacheMode,  // Global | Off | Dedicated(bytes). Named `segment_cache` (not `cache`) to avoid collision with `ScanArgsVortex::cache: bool` (LazyFrame query cache).
}

// Write options — embedded in FileWriteFormat::Vortex
pub struct VortexWriteOptions {
    pub compression: VortexCompression, // BtrBlocks (default) | Uncompressed
    pub row_block_size: Option<u64>,    // None → Vortex default (8192)
    pub include_dtype: bool,            // default true (manual Default impl)
}

// Session helpers
pub fn polars_vortex::session::session() -> &'static VortexSession;
pub fn polars_vortex::session::handle() -> vortex::io::runtime::Handle;
pub fn polars_vortex::session::segment_cache() -> Arc<dyn SegmentCache>;
pub fn polars_vortex::session::set_global_cache_bytes(bytes: u64);

// Eager Rust write
pub fn polars_vortex::write::write_vortex(
    df: &DataFrame,
    path: impl AsRef<Path>,
    options: &VortexWriteOptions,
) -> PolarsResult<()>;
```

## API surface — Python

```python
# Read
pl.scan_vortex(source, *, n_rows=None, row_index_name=None, row_index_offset=0,
               push_predicate=True, initial_read_size=None,
               scan_concurrency=None,
               cache_mode=None,        # Literal["global", "off"] | int | None — Vortex segment cache mode (None / "global" → process-global cache; "off" → disabled; positive int → per-scan Dedicated(bytes))
               hive_partitioning=None, glob=True,
               hidden_file_prefix=None, schema=None, hive_schema=None,
               try_parse_hive_dates=True, rechunk=False, cache=True,
               storage_options=None, credential_provider="auto",
               include_file_paths=None,
               missing_columns="raise", extra_columns="raise") -> LazyFrame

pl.read_vortex(source, ...) -> DataFrame    # eager: scan_vortex(...).collect()

# Write
LazyFrame.sink_vortex(path, *, compression="btrblocks", row_block_size=None,
                      include_dtype=True, maintain_order=True, storage_options=None,
                      credential_provider="auto", sync_on_close=None, mkdir=False,
                      lazy=False, engine="auto", optimizations=...)
DataFrame.write_vortex(file, *, compression="btrblocks", row_block_size=None,
                       include_dtype=True, storage_options=None,
                       credential_provider="auto")

# Config
pl.set_vortex_cache_bytes(byte_budget: int)  # 0 = disable; default 512 MiB
```

## What works today

- ✅ `pl.scan_vortex(local_path).filter(...).slice(...).collect()`
- ✅ `pl.scan_vortex("s3://...", storage_options={...}).collect()` — cloud read AND cloud schema
  discovery (no `schema=` required)
- ✅ `pl.scan_vortex(...).tail(N).collect()` — negative slice pushed via the footer's row count
- ✅ `df.write_vortex(path)` and `lf.sink_vortex(path)` (local AND `s3://`/`gs://`/`az://`)
- ✅ `lf.sink_vortex(pl.PartitionBy("base/", by=[...]))` — partitioned writes
- ✅ Filter pushdown for Equal / Between / EqualOneOf / StartsWith / EndsWith, and (when built with
  `dtype-date` / `dtype-datetime` / `dtype-time` / `dtype-decimal`) for Date / Datetime / Time /
  Decimal scalar literals.
- ✅ Multiple Vortex files in one scan (multi-file glob) via `UnifiedScanArgs`
- ✅ Hive partitioning on read via `UnifiedScanArgs::hive_options`
- ✅ Process-global decompressed-segment cache with `pl.set_vortex_cache_bytes(N)`
- ✅ `scan_concurrency=N` knob → `ScanBuilder::with_concurrency`
- ✅ `cargo check -p polars --features vortex,cloud,parquet` warm in ~5s

## Known limits / pending follow-ups

- **Aggressive predicate pushdown** (arithmetic, CAST, struct field access, temporal extracts) is
  not yet wired — these stay as residual filters. Implementing them requires walking AExpr at
  IR-build time instead of relying on `SpecializedColumnPredicate`.
- **File-level optimizer stats** (whole-file pruning at IR time) are not wired. Vortex's zone-level
  pruning already runs inside the scan, so this is a modest optimization — useful mainly for very
  large multi-file scans where opening every file at IR time is acceptable.
- **Polars `Categorical` / `Enum`** doesn't have a direct Vortex representation; writes fall back to
  UTF-8.

## Test recipes

End-to-end roundtrip from Python:

```python
import polars as pl
df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
df.write_vortex("/tmp/test.vortex")
out = pl.read_vortex("/tmp/test.vortex")
assert out.equals(df)
```

Check filter pushdown is engaged:

```python
lf = pl.scan_vortex("/tmp/test.vortex").filter(pl.col("a") == 2)
print(lf.explain(optimized=True))
# Expect the filter to be absorbed into the Scan node (no separate Filter step
# above), with `selection: ...` reflecting the pushed Vortex expression.
```

Verify the segment cache helps on a second read:

```python
import time
pl.set_vortex_cache_bytes(1 * 1024**3)
t1 = time.time(); pl.read_vortex("/tmp/big.vortex"); print("cold:", time.time() - t1)
t2 = time.time(); pl.read_vortex("/tmp/big.vortex"); print("warm:", time.time() - t2)
```

Run the Vortex Criterion benches (compare filter-pushdown wall-clock across phases):

```sh
# Save a baseline (e.g. on Phase 1's tip):
cargo bench -p polars --features vortex,cloud,parquet,dtype-full,strings \
    --bench io_vortex -- --save-baseline phase-1

# Compare a later commit (e.g. Phase 2's tip) against the saved baseline:
cargo bench -p polars --features vortex,cloud,parquet,dtype-full,strings \
    --bench io_vortex -- --baseline phase-1
```

The harness ships three benches (`vortex_scan/no_filter`, `vortex_scan/filter_lt`,
`vortex_scan/filter_arithmetic`); the third is the key Phase 1 → Phase 2 measurement (`col + 1 == N`
— Phase 1 falls back to residual+post-decode reapply, Phase 2 pushes arithmetic through Vortex's
zone pruning via `checked_add`).

## Pointers — reading the source

- The runtime wiring is the most subtle piece: see [`session.rs`] for how the global `VortexSession`
  is built from `ASYNC.handle()`.
- The C-ABI bridge is the most surprising piece: see the comments in [`read/array_bridge.rs`] and
  [`write/array_bridge.rs`].
- The predicate convertor is the highest-leverage piece for perf: see [`read/predicate.rs`].
- The streaming source's morsel loop is the heart of `begin_read`:
  `crates/polars-stream/src/nodes/io_sources/vortex/mod.rs`.
- The streaming sink's writer task is the inverse:
  `crates/polars-stream/src/nodes/io_sinks/writers/vortex/mod.rs`.

[`session.rs`]: src/session.rs
[`read/array_bridge.rs`]: src/read/array_bridge.rs
[`write/array_bridge.rs`]: src/write/array_bridge.rs
[`read/predicate.rs`]: src/read/predicate.rs
[`read/read_at.rs`]: src/read/read_at.rs
