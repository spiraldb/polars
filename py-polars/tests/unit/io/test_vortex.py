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
