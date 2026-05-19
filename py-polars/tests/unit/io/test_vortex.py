"""Smoke tests for the Vortex read/write integration.

These exercise the full Python → Rust → Vortex pipeline (DSL → IR → streaming
engine → polars-vortex → vortex-file) for the most common shapes. They run
only when the Polars binary was built with the ``vortex`` feature; otherwise
``pl.scan_vortex`` raises and the tests are skipped.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

import polars as pl
from polars.testing import assert_frame_equal

if TYPE_CHECKING:
    from pathlib import Path


def _vortex_available() -> bool:
    # `scan_vortex` is always exported from the Python side; whether the
    # underlying Rust binary has the `vortex` feature is what determines
    # support. Probe via a lightweight Rust-side check that's defined only
    # when the feature is on — falls through to a write probe if the helper
    # isn't found, so older builds still work.
    try:
        from polars._plr import set_vortex_cache_bytes  # noqa: F401
    except ImportError:
        return False
    # Real-world check that the write path is wired (catches a partial build
    # where the symbol is present but the sink isn't). We pin on the specific
    # AttributeError/PolarsError types so an unrelated bug doesn't silently
    # skip the suite.
    import tempfile

    from polars.exceptions import ComputeError

    try:
        with tempfile.NamedTemporaryFile(suffix=".vortex") as f:
            pl.DataFrame({"x": [1]}).write_vortex(f.name)
    except (AttributeError, ComputeError):
        return False
    else:
        return True


pytestmark = pytest.mark.skipif(
    not _vortex_available(),
    reason="polars was not built with the 'vortex' feature",
)


def test_roundtrip_basic(tmp_path: Path) -> None:
    path = tmp_path / "basic.vortex"
    df = pl.DataFrame(
        {
            "ints": [1, 2, 3, 4, 5],
            "floats": [1.0, 2.0, 3.0, 4.0, 5.0],
            "strs": ["a", "b", "c", "d", "e"],
        }
    )
    df.write_vortex(path)

    out = pl.read_vortex(path)
    assert_frame_equal(out, df)


def test_roundtrip_nullable(tmp_path: Path) -> None:
    path = tmp_path / "nullable.vortex"
    df = pl.DataFrame(
        {
            "a": [1, None, 3, None, 5],
            "b": ["x", "y", None, "z", None],
        }
    )
    df.write_vortex(path)
    out = pl.read_vortex(path)
    assert_frame_equal(out, df)


def test_roundtrip_uncompressed(tmp_path: Path) -> None:
    path = tmp_path / "uncompressed.vortex"
    df = pl.DataFrame({"x": range(100)})
    df.write_vortex(path, compression="uncompressed")
    out = pl.read_vortex(path)
    assert_frame_equal(out, df)


def test_roundtrip_small_row_block(tmp_path: Path) -> None:
    path = tmp_path / "small_blocks.vortex"
    df = pl.DataFrame({"x": range(20)})
    df.write_vortex(path, row_block_size=4)
    out = pl.read_vortex(path)
    assert_frame_equal(out, df)


def test_invalid_compression_errors(tmp_path: Path) -> None:
    path = tmp_path / "bad.vortex"
    df = pl.DataFrame({"x": [1, 2, 3]})
    with pytest.raises(ValueError, match="invalid vortex compression"):
        df.write_vortex(path, compression="gzip")  # type: ignore[arg-type]


def test_scan_with_filter(tmp_path: Path) -> None:
    path = tmp_path / "filter.vortex"
    df = pl.DataFrame({"a": list(range(20)), "b": [str(i) for i in range(20)]})
    df.write_vortex(path)

    out = pl.scan_vortex(path).filter(pl.col("a") > 14).collect()
    assert out.shape == (5, 2)
    assert out["a"].to_list() == [15, 16, 17, 18, 19]


def test_scan_with_arithmetic_filter(tmp_path: Path) -> None:
    """PR-13.2 acceptance: ``col + 1 == 5`` pushes down via the AExpr convertor.

    The legacy ``SpecializedColumnPredicate``-derived path cannot represent
    arithmetic on a column reference — only literal comparisons / IN-lists /
    range. PR-13.2 wires the convertor at ``physical_plan::lower_ir`` so the
    Vortex source receives a real ``a + 1 == 5`` Vortex expression. We assert
    correctness here (Polars reapplies post-decode regardless, so any drop-rows
    bug would be the *more* dangerous failure mode; a pushdown-not-applied
    regression would manifest as slower but still-correct results).
    """
    path = tmp_path / "arith_filter.vortex"
    df = pl.DataFrame({"a": list(range(20)), "b": [str(i) for i in range(20)]})
    df.write_vortex(path)

    out = pl.scan_vortex(path).filter(pl.col("a") + 1 == 5).collect()
    assert out.shape == (1, 2)
    assert out["a"].to_list() == [4]
    assert out["b"].to_list() == ["4"]


def test_scan_with_cast_filter(tmp_path: Path) -> None:
    """PR-13.3 acceptance: ``col.cast(Int64) > 100`` pushes down via the CAST arm.

    Convertor maps `AExpr::Cast { dtype: Int64, options: Strict }` →
    `vortex::expr::cast(child, DType::Primitive(I64, Nullable))` ONLY when the
    source dtype is in the same Vortex kind (Primitive↔Primitive, here Int32
    → Int64 is Primitive→Primitive). The legacy `SpecializedColumnPredicate`
    fast path cannot represent a CAST on the column side, so without PR-2.3
    this would fall back to no-pushdown.
    """
    path = tmp_path / "cast_filter.vortex"
    df = pl.DataFrame({"a": pl.Series([1, 50, 101, 200], dtype=pl.Int32)})
    df.write_vortex(path)

    out = pl.scan_vortex(path).filter(pl.col("a").cast(pl.Int64) > 100).collect()
    assert out.shape == (2, 1)
    assert out["a"].to_list() == [101, 200]


def test_scan_with_struct_field_filter(tmp_path: Path) -> None:
    """PR-13.4 acceptance: struct field access pushes down via the StructField arm.

    Convertor maps `AExpr::Function { StructExpr(FieldByName("inner")), .. }` →
    `vortex::expr::get_item("inner", inner_struct_expr)`. The legacy
    `SpecializedColumnPredicate` fast path cannot represent struct field access
    on the column side. Schema-membership gate refuses pushdown when the field
    doesn't exist in the struct's dtype (else Vortex's `GetItem.return_dtype`
    `vortex_err!`s at scan-time).
    """
    path = tmp_path / "struct_filter.vortex"
    df = pl.DataFrame(
        {
            "s": [
                {"inner": "a", "count": 1},
                {"inner": "x", "count": 2},
                {"inner": "x", "count": 3},
                {"inner": "z", "count": 4},
            ]
        }
    )
    df.write_vortex(path)

    out = (
        pl.scan_vortex(path).filter(pl.col("s").struct.field("inner") == "x").collect()
    )
    assert out.shape == (2, 1)
    assert out["s"].struct.field("count").to_list() == [2, 3]


def test_scan_with_cross_kind_cast_filter(tmp_path: Path) -> None:
    """PR-2.3 cycle-1 must-fix: cross-kind CAST (Primitive → Utf8) must not crash.

    Pre-fix: convertor emitted `cast(get_item("a", root()), DType::Utf8(...))`,
    which Vortex's `Primitive::CastKernel` doesn't handle (returns
    `Ok(None)` for non-Primitive targets), causing `cast/mod.rs:120` to
    `vortex_bail!("No CastKernel ...")` at scan-time — propagating as a
    hard `ComputeError`.

    Post-fix: `cast_kind_compatible` refuses the convertor pushdown so the
    legacy `polars_to_vortex_predicate` fallback handles the predicate
    (which also can't represent the cast — falls through to no-pushdown).
    Polars post-decode reapply produces the correct results.

    A regression dropping the source-dtype-kind gate would surface here as
    a Vortex scan-time error.
    """
    path = tmp_path / "cross_kind_cast.vortex"
    df = pl.DataFrame({"a": pl.Series([1, 50, 101, 200], dtype=pl.Int32)})
    df.write_vortex(path)

    # Int32 → String CAST then string equality. Must not crash; Polars
    # post-decode handles correctly.
    out = pl.scan_vortex(path).filter(pl.col("a").cast(pl.String) == "101").collect()
    assert out.shape == (1, 1)
    assert out["a"].to_list() == [101]


def test_scan_with_hive_partitioning_and_filter(tmp_path: Path) -> None:
    """PR-2.2 cycle-1 M1 regression (mechanism updated by PR-2.8): hive-partitioned
    scans with a hive-only-column filter must not crash.

    Without protection, the convertor at ``lower_ir.rs`` would emit a Vortex
    ``get_item('year', root())`` reference to a column that doesn't exist in
    the per-file Vortex data (``year`` is a HIVE virtual column, synthesized
    after decode from the directory structure).

    Protection mechanism (updated by PR-2.8): ``aexpr_file_minterms_to_vortex_expression``
    walks top-level conjuncts via ``MintermIter`` and drops minterms whose
    leaves are in ``virtual_cols`` (built from ``hive_parts.schema()`` +
    ``row_index.name`` + ``include_file_paths``). The single minterm
    ``year == 2024`` references only ``year`` (a hive virtual col), so the
    helper drops it; ``and_collect(vec![])`` returns ``None``; pushdown is
    refused; Polars' hive-partition pruning + multi-scan ``PARTIAL_FILTER``
    reapply produces the correct result.

    Pre-PR-2.8 mechanism: ``lower_ir.rs`` had an all-or-nothing virtual-col
    guard that refused the WHOLE predicate when ``hive_parts.is_some()``.
    The guard was REPLACED by PR-2.8's per-minterm split (which is
    strictly-better — see ``test_scan_with_hive_and_file_col_mixed_filter``
    for the mixed-shape case it now handles).

    A regression where the per-column split mis-classified ``year`` as a
    file column would surface as a ``ComputeError`` from Vortex bailing on
    a missing column 'year'.
    """
    (tmp_path / "year=2024").mkdir()
    (tmp_path / "year=2025").mkdir()
    pl.DataFrame({"x": [1, 2, 3]}).write_vortex(tmp_path / "year=2024" / "data.vortex")
    pl.DataFrame({"x": [4, 5, 6]}).write_vortex(tmp_path / "year=2025" / "data.vortex")

    out = (
        pl.scan_vortex(tmp_path / "**/*.vortex", hive_partitioning=True)
        .filter(pl.col("year") == 2024)
        .collect()
    )
    assert out.shape == (3, 2)
    assert sorted(out["x"].to_list()) == [1, 2, 3]
    assert out["year"].unique().to_list() == [2024]


def test_scan_with_row_index_and_filter(tmp_path: Path) -> None:
    """PR-2.2 cycle-2 C2-001 regression (mechanism updated by PR-2.8): row_index
    virtual col + row_index-only filter must not crash.

    ``row_index_name`` synthesizes ``ri`` as a virtual column after decode;
    ``ri`` is not present in the Vortex file's data. Without protection,
    the convertor would emit a Vortex ``get_item('ri', root())`` reference
    that Vortex can't resolve.

    Protection mechanism (updated by PR-2.8): the single minterm
    ``ri > 10`` references only ``ri`` (a row_index virtual col, included
    in ``virtual_cols`` at ``lower_ir.rs``); ``aexpr_file_minterms_to_vortex_expression``
    drops the minterm; ``and_collect(vec![])`` returns ``None``; pushdown
    is refused; Polars' row-index materialization + multi-scan
    ``PARTIAL_FILTER`` reapply produces the correct result.

    Pre-PR-2.8 mechanism: ``lower_ir.rs`` had an extended all-or-nothing
    guard that refused the WHOLE predicate when ``row_index.is_some()`` OR
    ``include_file_paths.is_some()``. The guard was REPLACED by PR-2.8's
    per-minterm split (see ``test_scan_with_row_index_and_file_col_mixed_filter``
    for the mixed-shape case it now handles).

    A regression where the per-column split mis-classified ``ri`` as a file
    column would surface as a ``ComputeError`` from Vortex bailing on
    missing column 'ri'.
    """
    path = tmp_path / "ri.vortex"
    pl.DataFrame({"x": list(range(20))}).write_vortex(path)

    out = pl.scan_vortex(path, row_index_name="ri").filter(pl.col("ri") > 10).collect()
    assert out["ri"].to_list() == list(range(11, 20))


def test_scan_with_projection(tmp_path: Path) -> None:
    path = tmp_path / "proj.vortex"
    df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
    df.write_vortex(path)

    out = pl.scan_vortex(path).select(["a", "c"]).collect()
    assert out.columns == ["a", "c"]
    assert out.shape == (3, 2)


def test_scan_with_negative_slice(tmp_path: Path) -> None:
    path = tmp_path / "neg_slice.vortex"
    df = pl.DataFrame({"x": list(range(100))})
    df.write_vortex(path)

    out = pl.scan_vortex(path).tail(5).collect()
    assert out["x"].to_list() == [95, 96, 97, 98, 99]


def test_cache_mode_accepts_all_valid_inputs(tmp_path: Path) -> None:
    """The cache_mode dispatch accepts None / "global" / "off" / positive int.

    The cache is a perf knob, not a correctness one, so all four modes should
    return identical data. (Moka's Cache doesn't expose hit/miss stats, so a
    hit-ratio assertion would require wrapping in an InstrumentedSegmentCache
    — deferred per plan.)
    """
    path = tmp_path / "cache_modes.vortex"
    pl.DataFrame({"x": list(range(10))}).write_vortex(path)

    for mode in (None, "global", "off", 4 * 1024 * 1024):
        out = pl.scan_vortex(path, cache_mode=mode).collect()  # type: ignore[arg-type]
        assert out["x"].to_list() == list(range(10))


def test_cache_mode_rejects_invalid_inputs(tmp_path: Path) -> None:
    """The cache_mode dispatch rejects bad input cleanly at scan-construction time."""
    path = tmp_path / "cache_invalid.vortex"
    pl.DataFrame({"x": [1]}).write_vortex(path)

    # Non-positive ints -> ValueError (the resolver's <= 0 guard).
    with pytest.raises(ValueError, match="positive byte count"):
        pl.scan_vortex(path, cache_mode=0)
    with pytest.raises(ValueError, match="positive byte count"):
        pl.scan_vortex(path, cache_mode=-5)

    # Unknown strings -> TypeError.
    with pytest.raises(TypeError, match="cache_mode must be"):
        pl.scan_vortex(path, cache_mode="invalid")  # type: ignore[arg-type]

    # bool inputs -> TypeError. `isinstance(True, int)` is True in Python, so
    # the resolver's explicit `isinstance(cache_mode, bool)` guard is the
    # only thing preventing True from being silently treated as a 1-byte
    # dedicated cache (and False as 0-byte). Regression-test the guard.
    with pytest.raises(TypeError, match="got bool"):
        pl.scan_vortex(path, cache_mode=True)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="got bool"):
        pl.scan_vortex(path, cache_mode=False)  # type: ignore[arg-type]

    # Float inputs -> TypeError (rejected by the resolver's int-isinstance check).
    with pytest.raises(TypeError, match="cache_mode must be"):
        pl.scan_vortex(path, cache_mode=1.5)  # type: ignore[arg-type]

    # Python int larger than u64::MAX -> OverflowError at the pyo3 boundary.
    # (The resolver returns ('dedicated', 2**64); pyo3's u64 conversion raises.)
    with pytest.raises(OverflowError):
        pl.scan_vortex(path, cache_mode=2**64)


def test_scan_with_file_stats_smoke(tmp_path: Path) -> None:
    """Scan + filter with file-level table_statistics populated.

    ``vortex_file_info`` populates ``UnifiedScanArgs::table_statistics`` from
    the Vortex footer's ``FileStatistics``. The mem-engine's
    ``skip_batch_predicate`` evaluates the predicate against the per-file
    min/max/null-count stats DataFrame and prunes the file when the predicate
    can never match.

    A regression dropping the table_statistics populate would still yield
    correct results (file gets opened, scanned, post-decode filter applies);
    the visible regression would be slower scan time. Smoke test verifies the
    populate path doesn't crash and returns the right rows.
    """
    path = tmp_path / "file_stats_smoke.vortex"
    pl.DataFrame({"a": list(range(100))}).write_vortex(path)

    out = pl.scan_vortex(path).filter(pl.col("a") > 90).collect()
    assert out.shape == (9, 1)
    assert out["a"].to_list() == list(range(91, 100))


def test_scan_with_file_stats_multifile_does_not_panic(tmp_path: Path) -> None:
    """Multi-file scan + filter must not panic.

    The populate path gates on ``n_sources == 1`` because the
    ``vortex_file_info`` only reads the first source's footer, producing a
    single-row stats DataFrame. Without the gate, multi-file scans would hit
    ``polars-mem-engine/src/scan_predicate/functions.rs:397``
    (``assert_eq!(skip_files_mask.len(), sources.len())``) and panic.

    A regression where the gate is dropped would surface here as a hard
    ``assertion failed`` panic during ``collect()``.
    """
    a = tmp_path / "stats_multi_a.vortex"
    b = tmp_path / "stats_multi_b.vortex"
    pl.DataFrame({"x": [1, 3, 5, 7, 9]}).write_vortex(a)
    pl.DataFrame({"x": [2, 4, 6, 8, 10]}).write_vortex(b)

    out = pl.scan_vortex([a, b]).filter(pl.col("x") > 5).collect()
    # Combined sorted: a's {7, 9} + b's {6, 8, 10} = 5 rows.
    assert out.shape == (5, 1)
    assert sorted(out["x"].to_list()) == [6, 7, 8, 9, 10]


def test_multifile_scan_shape_and_ordering(tmp_path: Path) -> None:
    """``pl.scan_vortex([a, b])`` yields right shape + path-list order.

    Multi-file scans should: (a) read each file in path-list order, (b) concat
    results with the same schema, (c) produce a DataFrame whose row count is
    the sum of per-file row counts.
    """
    a = tmp_path / "multi_a.vortex"
    b = tmp_path / "multi_b.vortex"
    pl.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]}).write_vortex(a)
    pl.DataFrame({"x": [4, 5], "y": ["d", "e"]}).write_vortex(b)

    out = pl.scan_vortex([a, b]).collect()
    assert out.shape == (5, 2)
    assert out["x"].to_list() == [1, 2, 3, 4, 5]
    assert out["y"].to_list() == ["a", "b", "c", "d", "e"]


def test_multifile_scan_missing_columns_insert(tmp_path: Path) -> None:
    """missing_columns='insert' synthesizes typed nulls for absent columns.

    File a has {x, y}; file b has only {x}. With missing_columns='insert',
    file b's rows get NULL in the y column.

    Verifies Vortex respects the missing_columns parameter at multi-scan
    layer (parameter already exposed via scan_vortex's signature; handled
    generically in apply_extra_ops).
    """
    a = tmp_path / "miss_a.vortex"
    b = tmp_path / "miss_b.vortex"
    pl.DataFrame({"x": [1, 2], "y": ["a", "b"]}).write_vortex(a)
    pl.DataFrame({"x": [3, 4]}).write_vortex(b)

    out = pl.scan_vortex([a, b], missing_columns="insert").collect()
    assert out.shape == (4, 2)
    assert out["x"].to_list() == [1, 2, 3, 4]
    assert out["y"].to_list() == ["a", "b", None, None]


def test_multifile_scan_missing_columns_raise(tmp_path: Path) -> None:
    """missing_columns='raise' (default) errors when a file lacks a column."""
    a = tmp_path / "miss_r_a.vortex"
    b = tmp_path / "miss_r_b.vortex"
    pl.DataFrame({"x": [1, 2], "y": ["a", "b"]}).write_vortex(a)
    pl.DataFrame({"x": [3, 4]}).write_vortex(b)

    with pytest.raises(pl.exceptions.PolarsError):
        pl.scan_vortex([a, b], missing_columns="raise").collect()
# === PR-2.7 amend: cutover-lost pushdown shapes ===
#
# Each test exercises a shape that the PR-2.6 cutover removed from the
# pushdown path. Correctness via Polars's post-decode reapply is invariant
# (PARTIAL_FILTER capability), so a "pushdown not applied" regression would
# manifest as slower-but-correct results — these tests catch the more
# dangerous failure mode: pushdown emitting a Vortex expression that
# silently drops or duplicates rows. Engagement verification is implicit
# (the bench harness from PR-1.5 measures wall-clock).


def test_scan_with_is_between_filter(tmp_path: Path) -> None:
    """PR-2.7 cycle 1: ``col.is_between(lo, hi)`` pushes down via the
    is_between arm, decomposed to ``(col >= lo) AND (col <= hi)``.

    The legacy SpecializedColumnPredicate path handled this via
    ``SpecializedColumnPredicate::Between``; PR-2.6 cutover removed it.
    The new convertor arm re-establishes pushdown by decomposing to a
    Vortex ``and(gt_eq, lt_eq)`` (or strict variants per ``closed``).
    """
    path = tmp_path / "between_filter.vortex"
    df = pl.DataFrame({"a": list(range(20))})
    df.write_vortex(path)

    out = pl.scan_vortex(path).filter(pl.col("a").is_between(5, 10)).collect()
    # Default closed="both" → 5, 6, 7, 8, 9, 10
    assert out["a"].to_list() == [5, 6, 7, 8, 9, 10]


def test_scan_with_is_between_left_closed_filter(tmp_path: Path) -> None:
    """PR-2.7 cycle 1: ``is_between`` with non-default closed kwarg.

    ClosedInterval::Left → ``(col >= lo) AND (col < hi)``. Verifies the
    closed-interval-variant mapping isn't off-by-one.
    """
    path = tmp_path / "between_left_filter.vortex"
    df = pl.DataFrame({"a": list(range(20))})
    df.write_vortex(path)

    out = (
        pl.scan_vortex(path)
        .filter(pl.col("a").is_between(5, 10, closed="left"))
        .collect()
    )
    # closed="left" → 5, 6, 7, 8, 9 (10 excluded)
    assert out["a"].to_list() == [5, 6, 7, 8, 9]


def test_scan_with_is_in_filter(tmp_path: Path) -> None:
    """PR-2.7 cycle 1: ``col.is_in([...])`` pushes down via the is_in arm,
    decomposed to ``(col == v1) OR (col == v2) OR ...``.

    Legacy ``SpecializedColumnPredicate::EqualOneOf`` handled this; PR-2.6
    cutover removed it. The new convertor arm reuses the polars-plan-internal
    ``try_extract_is_in_haystack`` helper for haystack extraction (same code
    path as the deleted SpecializedColumnPredicate route) so the
    constant-eval / list-dispatch / null-drop logic stays consistent.
    """
    path = tmp_path / "is_in_filter.vortex"
    df = pl.DataFrame({"a": list(range(20))})
    df.write_vortex(path)

    out = pl.scan_vortex(path).filter(pl.col("a").is_in([1, 3, 5, 7])).collect()
    assert out["a"].to_list() == [1, 3, 5, 7]


def test_scan_with_starts_with_filter(tmp_path: Path) -> None:
    """PR-2.7 cycle 1: ``col.str.starts_with("prefix")`` pushes down via the
    StringExpr arm as ``like(col, lit("prefix%"))``.

    Legacy ``SpecializedColumnPredicate::StartsWith`` handled this; PR-2.6
    cutover removed it. The needle is escaped via ``bytes_to_like_literal``
    which refuses pushdown if the prefix contains LIKE wildcards (%, _, \\).
    """
    path = tmp_path / "starts_with_filter.vortex"
    df = pl.DataFrame({"s": ["apple", "apricot", "banana", "blueberry", "cherry"]})
    df.write_vortex(path)

    out = pl.scan_vortex(path).filter(pl.col("s").str.starts_with("ap")).collect()
    assert out["s"].to_list() == ["apple", "apricot"]


def test_scan_with_ends_with_filter(tmp_path: Path) -> None:
    """PR-2.7 cycle 1: ``col.str.ends_with("suffix")`` pushes down via the
    StringExpr arm as ``like(col, lit("%suffix"))``."""
    path = tmp_path / "ends_with_filter.vortex"
    df = pl.DataFrame({"s": ["apple", "pineapple", "banana", "grape"]})
    df.write_vortex(path)

    out = pl.scan_vortex(path).filter(pl.col("s").str.ends_with("apple")).collect()
    # Both "apple" and "pineapple" end with "apple"
    assert out["s"].to_list() == ["apple", "pineapple"]


def test_scan_with_contains_literal_filter(tmp_path: Path) -> None:
    """PR-2.7 cycle 1: ``col.str.contains("sub", literal=True)`` pushes down
    via the StringExpr arm as ``like(col, lit("%sub%"))``.

    ``literal=False`` (regex mode) is REFUSED — Vortex's LIKE doesn't
    support regex; the residual filter reapplies post-decode for correctness.
    Tested implicitly by the unit-level
    ``shape_contains_literal_false_returns_none`` test.
    """
    path = tmp_path / "contains_filter.vortex"
    df = pl.DataFrame(
        {"s": ["hello world", "good morning", "world peace", "morning sun"]}
    )
    df.write_vortex(path)

    out = (
        pl.scan_vortex(path)
        .filter(pl.col("s").str.contains("world", literal=True))
        .collect()
    )
    assert out["s"].to_list() == ["hello world", "world peace"]


def test_scan_with_ternary_filter(tmp_path: Path) -> None:
    """PR-2.7 cycle 1: ``pl.when(...).then(...).otherwise(...)`` inside a
    filter pushes down via the Ternary arm as Vortex
    ``case_when(condition, then_value, else_value)``.

    The Ternary returns a Boolean expression usable as a filter predicate.
    Here: when ``a > 5``, push down ``a < 15``; otherwise emit False.
    Effective predicate: ``5 < a < 15``.
    """
    path = tmp_path / "ternary_filter.vortex"
    df = pl.DataFrame({"a": list(range(20))})
    df.write_vortex(path)

    out = (
        pl.scan_vortex(path)
        .filter(
            pl.when(pl.col("a") > 5).then(pl.col("a") < 15).otherwise(False)  # noqa: FBT003
        )
        .collect()
    )
    # 5 < a < 15 → 6, 7, 8, 9, 10, 11, 12, 13, 14
    assert out["a"].to_list() == [6, 7, 8, 9, 10, 11, 12, 13, 14]


def test_scan_with_hive_and_file_col_mixed_filter(tmp_path: Path) -> None:
    """PR-2.8: hive-partitioned scan with a mixed file-col + hive-col filter
    splits the predicate per-column and pushes the file-col conjunct to Vortex
    while leaving the hive-col conjunct for Polars' multi-scan reapply.

    Pre-PR-2.8: the convertor's virtual-column guard at lower_ir.rs:801-815
    refused convertor pushdown ENTIRELY when hive_parts.is_some(); correctness
    held via PARTIAL_FILTER reapply but the file-col part missed Vortex zone
    pruning.

    Post-PR-2.8: `aexpr_file_minterms_to_vortex_expression` walks top-level
    conjuncts via MintermIter and converts only the file-only ones. The
    `x > 5` minterm pushes to Vortex; the `year == 2024` minterm stays
    residual and Polars' hive-partition pruning + multi-scan reapply handles
    it. Result correctness is preserved either way; the test confirms the
    pipeline doesn't crash and returns the right rows.

    A regression where the per-column split mis-classified a hive col as
    file (and tried to push it to Vortex) would surface as a ``ComputeError``
    from Vortex bailing on missing column 'year'.
    """
    (tmp_path / "year=2024").mkdir()
    (tmp_path / "year=2025").mkdir()
    pl.DataFrame({"x": [1, 3, 5, 7, 9]}).write_vortex(tmp_path / "year=2024" / "data.vortex")
    pl.DataFrame({"x": [2, 4, 6, 8, 10]}).write_vortex(tmp_path / "year=2025" / "data.vortex")

    out = (
        pl.scan_vortex(tmp_path / "**/*.vortex", hive_partitioning=True)
        .filter((pl.col("x") > 5) & (pl.col("year") == 2024))
        .collect()
    )
    # x > 5 AND year == 2024 → from year=2024 dir: 7, 9 (1, 3, 5 are filtered)
    assert out.shape == (2, 2)
    assert sorted(out["x"].to_list()) == [7, 9]
    assert out["year"].unique().to_list() == [2024]


def test_scan_with_row_index_and_file_col_mixed_filter(tmp_path: Path) -> None:
    """PR-2.8 cycle 2: row_index virtual col + file col mixed filter splits the
    predicate per-minterm. The ``x > 5`` minterm pushes to Vortex; the
    ``ri > 10`` minterm stays residual and Polars' row-index materialization
    + multi-scan reapply handles it.

    Pre-PR-2.8 behavior: virtual-column guard refused the WHOLE predicate
    when ``row_index.is_some()``; correctness held via PARTIAL_FILTER but the
    file-col part missed Vortex zone pruning.

    A regression where the per-column split mis-classified ``ri`` as a file
    column (and tried to push it to Vortex) would surface as a
    ``ComputeError`` from Vortex bailing on missing column 'ri'.
    """
    path = tmp_path / "ri_mixed.vortex"
    pl.DataFrame({"x": list(range(20))}).write_vortex(path)

    out = (
        pl.scan_vortex(path, row_index_name="ri")
        .filter((pl.col("x") > 5) & (pl.col("ri") > 10))
        .collect()
    )
    # x: 0..20; ri: 0..20 (1:1 mapping). x > 5 → x ∈ {6..19}; ri > 10 → ri ∈ {11..19}.
    # Intersection: x ∈ {11..19}, ri ∈ {11..19}, both 9 rows.
    assert out.shape == (9, 2)
    assert out["x"].to_list() == list(range(11, 20))
    assert out["ri"].to_list() == list(range(11, 20))


def test_scan_with_include_file_paths_and_file_col_mixed_filter(tmp_path: Path) -> None:
    """PR-2.8 cycle 2: include_file_paths virtual col + file col mixed filter
    splits the predicate per-minterm. The ``x > 5`` minterm pushes to Vortex;
    the ``pl.col("src").str.ends_with("a.vortex")`` minterm stays residual.

    Note: ``include_file_paths`` populates ``src`` with the FULL path (per
    ``ScanSourceRef::to_include_path_name`` at
    ``crates/polars-plan/src/dsl/scan_sources.rs``), not the basename. The
    discriminator must therefore anchor on a substring that does NOT appear in
    pytest's ``tmp_path`` parent directory; ``str.ends_with("a.vortex")``
    anchors on the basename suffix and is robust across CI environments.

    A regression where the per-column split mis-classified ``src`` as a file
    column would surface as a ``ComputeError`` from Vortex bailing on missing
    column 'src'.
    """
    a = tmp_path / "a.vortex"
    b = tmp_path / "b.vortex"
    pl.DataFrame({"x": [1, 3, 5, 7, 9]}).write_vortex(a)
    pl.DataFrame({"x": [2, 4, 6, 8, 10]}).write_vortex(b)

    out = (
        pl.scan_vortex([a, b], include_file_paths="src")
        .filter((pl.col("x") > 5) & pl.col("src").str.ends_with("a.vortex"))
        .collect()
    )
    # a.vortex's x: 1,3,5,7,9 → x > 5 → 7, 9 (src ends with "a.vortex" → match)
    # b.vortex's x: 2,4,6,8,10 → x > 5 → 6, 8, 10 (src ends with "b.vortex" → no match)
    assert out.shape == (2, 2)
    assert sorted(out["x"].to_list()) == [7, 9]
    assert all(s.endswith("a.vortex") for s in out["src"].to_list())


def test_scan_with_starts_with_wildcard_in_needle(tmp_path: Path) -> None:
    """PR-2.7 cycle 1 (negative path): a wildcard ('%') in the needle refuses
    pushdown via ``bytes_to_like_literal``. The residual filter reapplies
    post-decode for correctness; a regression dropping the wildcard guard
    would *widen* the predicate (Vortex LIKE interprets '%' as match-any).

    Correctness must hold either way (the residual is the safety net), so
    the assertion focuses on the result: only rows containing the literal
    '100%' string match.
    """
    path = tmp_path / "wildcard_needle.vortex"
    df = pl.DataFrame({"s": ["100% pure", "1000 hits", "absolute 100%", "no match"]})
    df.write_vortex(path)

    out = pl.scan_vortex(path).filter(pl.col("s").str.starts_with("100%")).collect()
    # Only "100% pure" starts with the literal "100%". "1000 hits" must not
    # match (would if '%' were interpreted as LIKE wildcard).
    assert out["s"].to_list() == ["100% pure"]
