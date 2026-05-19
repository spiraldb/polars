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
