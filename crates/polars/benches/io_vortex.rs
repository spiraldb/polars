//! Criterion benches for Vortex filter-pushdown wall-clock perf.
//!
//! Phase 1 ("Ratify + crates.io transition") ships Vortex format support with a
//! `SpecializedColumnPredicate`-derived filter-pushdown path that handles a fixed
//! set of shapes (`==` / `Between` / `is_in` / `starts_with` / `ends_with`). Phase 2
//! ("PR-13 aggressive AExpr pushdown") swaps that out for an AExpr-direct convertor
//! that covers a broader shape set including arithmetic (`col + 1 == N`), same-kind
//! `CAST`, and struct field access.
//!
//! These benches give Phase 1 and Phase 2 a comparable wall-clock anchor:
//!
//! 1. `no_filter` — baseline full-scan with no predicate; identical perf on both
//!    phases (no convertor involvement). Useful for normalizing throughput across
//!    machines.
//! 2. `filter_lt` — `col < N`, selective enough to skip most zones. Both phases
//!    push this down (Phase 1 via `SpecializedColumnPredicate::Lt`, Phase 2 via the
//!    AExpr-direct convertor's `lt` arm). Wall-clock should be ~equal across
//!    phases; if Phase 2 regresses here, the convertor's per-shape overhead is
//!    measurable.
//! 3. `filter_arithmetic` — `col + 1 == N`. Phase 1: AExpr-direct path is absent,
//!    convertor returns `None`, scan decodes everything and Polars filters
//!    post-decode. Phase 2: the convertor emits `eq(checked_add(get_item("id", root()),
//!    lit(1)), lit(N))` and Vortex skips zones that can't satisfy. Expected: large
//!    speedup on Phase 2 for selective filters on multi-zone files.
//!
//! Run with:
//! ```sh
//! cargo bench -p polars --features vortex,cloud,parquet,dtype-full,strings \
//!     --bench io_vortex
//! ```
//!
//! The broader `cloud,parquet,dtype-full,strings` set is needed because
//! `polars-stream`'s lower_expr.rs references `IRStringFunction::Strptime` inside
//! a `dtype-date/datetime/time`-gated arm — that arm pulls in `strings` from
//! polars-plan transitively. The minimal feature combo for the bench is therefore
//! the same as Phase 1's documented exit-criterion (a):
//! `cargo check -p polars --features vortex,cloud,parquet,dtype-full,strings`.
//!
//! For the Phase 1 → Phase 2 comparison, run on Phase 1's tip first
//! (save the criterion output), then on Phase 2's tip, and compare via
//! `cargo bench --bench io_vortex -- --save-baseline <name>` / `--baseline <name>`.

use std::path::Path;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use polars::prelude::*;
use polars_utils::pl_path::PlRefPath;
use polars_vortex::VortexWriteOptions;
use polars_vortex::write::write_vortex;
use tempfile::tempdir;

/// Number of rows in the synthetic Vortex test file. 100k is enough to span multiple
/// Vortex zones (default zone size is 8,192 rows) so zone-pruning has something to
/// skip, while keeping per-iteration time small enough that the benches finish in
/// reasonable wall-clock.
const N_ROWS: i64 = 100_000;

/// Target row in the filter (a fixed selective value somewhere mid-file). Both
/// `filter_lt` and `filter_arithmetic` use this so the selectivity is comparable.
const FILTER_TARGET: i64 = 50_000;

/// Build a synthetic DataFrame with `id: Int64` (0..N) and `value: Int64` (id*7 + 3),
/// write it to a Vortex file with default options, and return the file path.
fn make_test_vortex(path: &Path) {
    let ids: Vec<i64> = (0..N_ROWS).collect();
    let values: Vec<i64> = ids
        .iter()
        .map(|&i| i.wrapping_mul(7).wrapping_add(3))
        .collect();
    let df = df!["id" => ids, "value" => values].expect("build df");
    write_vortex(&df, path, &VortexWriteOptions::default()).expect("write vortex");
}

fn bench_vortex_scan(c: &mut Criterion) {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("bench.vortex");
    make_test_vortex(&path);

    let mut group = c.benchmark_group("vortex_scan");

    group.bench_function("no_filter", |b| {
        b.iter(|| {
            let df = LazyFrame::scan_vortex(
                PlRefPath::try_from_path(&path).expect("path -> PlRefPath"),
                ScanArgsVortex::default(),
            )
            .expect("scan_vortex")
            .collect()
            .expect("collect");
            black_box(df);
        });
    });

    group.bench_function("filter_lt", |b| {
        b.iter(|| {
            let df = LazyFrame::scan_vortex(
                PlRefPath::try_from_path(&path).expect("path -> PlRefPath"),
                ScanArgsVortex::default(),
            )
            .expect("scan_vortex")
            .filter(col("id").lt(lit(FILTER_TARGET)))
            .collect()
            .expect("collect");
            black_box(df);
        });
    });

    group.bench_function("filter_arithmetic", |b| {
        b.iter(|| {
            // `col("id") + 1 == FILTER_TARGET` — Phase 1's `SpecializedColumnPredicate`
            // can't represent arithmetic on the column side; Phase 2's AExpr-direct
            // convertor maps Plus → checked_add and pushes down.
            let df = LazyFrame::scan_vortex(
                PlRefPath::try_from_path(&path).expect("path -> PlRefPath"),
                ScanArgsVortex::default(),
            )
            .expect("scan_vortex")
            .filter((col("id") + lit(1i64)).eq(lit(FILTER_TARGET)))
            .collect()
            .expect("collect");
            black_box(df);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_vortex_scan);
criterion_main!(benches);
