"""Smoke tests for the Vortex read/write integration.

These exercise the full Python → Rust → Vortex pipeline (DSL → IR → streaming
engine → polars-vortex → vortex-file) for the most common shapes. They run
only when the Polars binary was built with the ``vortex`` feature; otherwise
``pl.scan_vortex`` raises and the tests are skipped.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import polars as pl
from polars.testing import assert_frame_equal


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
        return True
    except (AttributeError, ComputeError):
        return False


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
