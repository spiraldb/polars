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
    from polars import _plr

    return hasattr(_plr.PyLazyFrame, "new_from_vortex")


pytestmark = pytest.mark.skipif(
    not _vortex_available(), reason="polars was not built with the 'vortex' feature"
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
    path = tmp_path / "cache_modes.vortex"
    pl.DataFrame({"x": list(range(10))}).write_vortex(path)

    for mode in (None, "global", "off", 4 * 1024 * 1024):
        out = pl.scan_vortex(path, cache_mode=mode).collect()  # type: ignore[arg-type]
        assert out["x"].to_list() == list(range(10))


def test_cache_mode_rejects_invalid_inputs(tmp_path: Path) -> None:
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


def test_filter_returns_matching_rows(tmp_path: Path) -> None:
    path = tmp_path / "file_stats_smoke.vortex"
    pl.DataFrame({"a": list(range(100))}).write_vortex(path)

    out = pl.scan_vortex(path).filter(pl.col("a") > 90).collect()
    assert out.shape == (9, 1)
    assert out["a"].to_list() == list(range(91, 100))


def test_multifile_filter_returns_matching_rows(tmp_path: Path) -> None:
    a = tmp_path / "stats_multi_a.vortex"
    b = tmp_path / "stats_multi_b.vortex"
    pl.DataFrame({"x": [1, 3, 5, 7, 9]}).write_vortex(a)
    pl.DataFrame({"x": [2, 4, 6, 8, 10]}).write_vortex(b)

    out = pl.scan_vortex([a, b]).filter(pl.col("x") > 5).collect()
    assert out.shape == (5, 1)
    assert sorted(out["x"].to_list()) == [6, 7, 8, 9, 10]


def test_multifile_scan_shape_and_ordering(tmp_path: Path) -> None:
    a = tmp_path / "multi_a.vortex"
    b = tmp_path / "multi_b.vortex"
    pl.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]}).write_vortex(a)
    pl.DataFrame({"x": [4, 5], "y": ["d", "e"]}).write_vortex(b)

    out = pl.scan_vortex([a, b]).collect()
    assert out.shape == (5, 2)
    assert out["x"].to_list() == [1, 2, 3, 4, 5]
    assert out["y"].to_list() == ["a", "b", "c", "d", "e"]


def test_multifile_scan_missing_columns_insert(tmp_path: Path) -> None:
    a = tmp_path / "miss_a.vortex"
    b = tmp_path / "miss_b.vortex"
    pl.DataFrame({"x": [1, 2], "y": ["a", "b"]}).write_vortex(a)
    pl.DataFrame({"x": [3, 4]}).write_vortex(b)

    out = pl.scan_vortex([a, b], missing_columns="insert").collect()
    assert out.shape == (4, 2)
    assert out["x"].to_list() == [1, 2, 3, 4]
    assert out["y"].to_list() == ["a", "b", None, None]


def test_multifile_scan_missing_columns_raise(tmp_path: Path) -> None:
    a = tmp_path / "miss_r_a.vortex"
    b = tmp_path / "miss_r_b.vortex"
    pl.DataFrame({"x": [1, 2], "y": ["a", "b"]}).write_vortex(a)
    pl.DataFrame({"x": [3, 4]}).write_vortex(b)

    with pytest.raises(pl.exceptions.PolarsError):
        pl.scan_vortex([a, b], missing_columns="raise").collect()


@pytest.mark.parametrize("engine", ["streaming", "in-memory"])
@pytest.mark.parametrize("push_predicate", [False, True])
def test_float_and_overflow_residuals(
    tmp_path: Path, engine: str, push_predicate: bool
) -> None:
    import struct

    nan_negative = struct.unpack("d", struct.pack("Q", 0xFFF8000000000001))[0]
    df = pl.DataFrame(
        {
            "id": range(7),
            "x": [-0.0, 0.0, -1.0, 1.0, float("nan"), nan_negative, float("inf")],
        }
    )
    path = tmp_path / "float.vortex"
    df.write_vortex(path)
    for predicate in (
        pl.col("x") == 0.0,
        pl.col("x") < 0.0,
        pl.col("x") == float("nan"),
        pl.col("x") > 1.0,
    ):
        expected = df.lazy().filter(predicate).collect(engine=engine)
        actual = (
            pl.scan_vortex(path, push_predicate=push_predicate)
            .filter(predicate)
            .collect(engine=engine)
        )
        assert_frame_equal(actual, expected)
    ints = pl.DataFrame({"x": [-(2**63), 0, 2**63 - 1]})
    ints.write_vortex(path)
    predicate = (pl.col("x") + 1) < 0
    assert_frame_equal(
        pl.scan_vortex(path, push_predicate=push_predicate)
        .filter(predicate)
        .collect(engine=engine),
        ints.lazy().filter(predicate).collect(engine=engine),
    )


@pytest.mark.parametrize("engine", ["streaming", "in-memory"])
def test_projection_order_and_zero_columns(tmp_path: Path, engine: str) -> None:
    df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
    path = tmp_path / "projection.vortex"
    df.write_vortex(path)
    assert_frame_equal(
        pl.scan_vortex(path).select("c", "a").collect(engine=engine),
        df.select("c", "a"),
    )
    assert pl.scan_vortex(path).select(pl.len()).collect(engine=engine).item() == 3
    assert_frame_equal(
        pl.scan_vortex(path).select([]).collect(engine=engine),
        df.lazy().select([]).collect(engine=engine),
    )


@pytest.mark.parametrize("engine", ["streaming", "in-memory"])
def test_row_index_across_files_filter_slice(tmp_path: Path, engine: str) -> None:
    frames = [pl.DataFrame({"x": range(5)}), pl.DataFrame({"x": range(5, 11)})]
    paths = [tmp_path / f"{i}.vortex" for i in range(2)]
    for df, path in zip(frames, paths, strict=True):
        df.write_vortex(path)
    expected = pl.concat(frames).with_row_index("row", offset=9).lazy()
    actual = pl.scan_vortex(paths, row_index_name="row", row_index_offset=9)
    for transform in (
        lambda q: q.filter(pl.col("x") % 2 == 0).slice(1, 3),
        lambda q: q.slice(2, 6).filter(pl.col("x") > 3),
        lambda q: q.tail(4),
    ):
        assert_frame_equal(
            transform(actual).collect(engine=engine),
            transform(expected).collect(engine=engine),
        )


def test_physical_schema_mismatch(tmp_path: Path) -> None:
    a, b = tmp_path / "a.vortex", tmp_path / "b.vortex"
    pl.DataFrame({"x": [1]}).write_vortex(a)
    pl.DataFrame({"x": ["one"]}).write_vortex(b)
    with pytest.raises(pl.exceptions.PolarsError):
        pl.scan_vortex([a, b]).collect()


def test_nested_and_temporal_roundtrip(tmp_path: Path) -> None:
    from datetime import date, datetime
    from decimal import Decimal

    df = pl.DataFrame(
        {
            "struct": [{"x": 1, "y": None}, None, {"x": None, "y": "a"}],
            "list": [[1, None], None, []],
            "date": [date(2026, 1, 1), None, date(2026, 2, 1)],
            "datetime": [datetime(2026, 1, 1), None, datetime(2026, 2, 1)],
            "decimal": [Decimal("1.25"), None, Decimal("-9.00")],
        }
    )
    path = tmp_path / "nested.vortex"
    df.write_vortex(path)
    assert_frame_equal(pl.read_vortex(path), df)


@pytest.mark.parametrize("lazy", [False, True])
def test_omitted_schema_returns_explicit_error(tmp_path: Path, lazy: bool) -> None:
    df = pl.DataFrame({"x": [1, 2, 3]})
    path = tmp_path / "no_schema.vortex"
    write = df.lazy().sink_vortex if lazy else df.write_vortex
    with pytest.raises(
        pl.exceptions.InvalidOperationError,
        match="include_dtype=False is not supported",
    ):
        write(path, include_dtype=False)


def test_duration_write_returns_explicit_error(tmp_path: Path) -> None:
    from datetime import timedelta

    df = pl.DataFrame({"duration": [timedelta(seconds=1), None, timedelta(seconds=3)]})
    path = tmp_path / "duration.vortex"
    with pytest.raises(pl.exceptions.PolarsError, match=r"[Dd]uration"):
        df.write_vortex(path)


def test_map_roundtrip(tmp_path: Path) -> None:
    pa = pytest.importorskip("pyarrow")
    df = pl.from_arrow(
        pa.table(
            {
                "m": pa.array(
                    [[("a", 1), ("b", None)], None, []],
                    type=pa.map_(pa.string(), pa.int64()),
                )
            }
        )
    )
    path = tmp_path / "map.vortex"
    df.write_vortex(path)
    assert_frame_equal(pl.read_vortex(path), df)


def test_sink_lazy_and_partition_completion(tmp_path: Path) -> None:
    df = pl.DataFrame({"key": [0, 1, 0], "value": [2, 3, 4]})
    path = tmp_path / "lazy.vortex"
    plan = df.lazy().sink_vortex(path, lazy=True)
    assert not path.exists()
    assert isinstance(plan, pl.LazyFrame)
    plan.collect()
    assert_frame_equal(pl.read_vortex(path), df)
    directory = tmp_path / "parts"
    df.lazy().sink_vortex(pl.PartitionBy(directory, key="key"), mkdir=True)
    paths = sorted(p for p in directory.rglob("*") if p.is_file())
    assert paths
    actual = pl.scan_vortex(paths, hive_partitioning=True).collect().sort("value")
    assert_frame_equal(actual.select(df.columns), df.sort("value"))


def test_sink_failure_after_valid_batch(tmp_path: Path) -> None:
    path = tmp_path / "failed.vortex"

    def fail(df: pl.DataFrame) -> pl.DataFrame:
        if df["x"].min() >= 2:
            msg = "injected upstream failure"
            raise ValueError(msg)
        return df

    query = pl.concat(
        [
            pl.DataFrame({"x": [0, 1]}).lazy(),
            pl.DataFrame({"x": [2, 3]}).lazy().map_batches(fail, streamable=True),
        ]
    )
    with pytest.raises(
        (ValueError, pl.exceptions.PolarsError), match="injected upstream failure"
    ):
        query.sink_vortex(path)
    if path.exists():
        with pytest.raises(pl.exceptions.PolarsError):
            pl.read_vortex(path)


def test_python_vortex_interoperability(tmp_path: Path) -> None:
    vortex = pytest.importorskip("vortex")
    df = pl.DataFrame({"x": [1, None, 3], "s": ["a", "b", None]})
    native = tmp_path / "native.vortex"
    python = tmp_path / "python.vortex"
    df.write_vortex(native)
    assert_frame_equal(pl.from_arrow(vortex.open(str(native)).to_arrow()), df)
    vortex.io.write(vortex.array(df.to_arrow()), str(python))
    assert_frame_equal(pl.read_vortex(python), df)


@pytest.mark.parametrize("engine", ["streaming", "in-memory"])
def test_residual_filter_limit_order(tmp_path: Path, engine: str) -> None:
    df = pl.DataFrame({"x": range(30), "s": [str(i) for i in range(30)]})
    path = tmp_path / "residual.vortex"
    df.write_vortex(path)
    for transform in (
        lambda q: q.filter((pl.col("x") < 4) | pl.col("s").str.ends_with("7")).head(3),
        lambda q: q.head(10).filter(pl.col("x") > 7),
        lambda q: q.filter(pl.col("x") > 7).head(10),
        lambda q: q.filter(pl.col("s").str.ends_with("7")).select("x"),
    ):
        assert_frame_equal(
            transform(pl.scan_vortex(path)).collect(engine=engine),
            transform(df.lazy()).collect(engine=engine),
        )


def test_empty_file_roundtrip(tmp_path: Path) -> None:
    path = tmp_path / "empty.vortex"
    df = pl.DataFrame(schema={"x": pl.Int64, "s": pl.String})
    df.write_vortex(path)
    assert_frame_equal(pl.read_vortex(path), df)
    assert pl.scan_vortex(path).select(pl.len()).collect().item() == 0


def test_sink_file_object(tmp_path: Path) -> None:
    from io import BytesIO

    df = pl.DataFrame({"x": [1, 2, 3]})
    output = BytesIO()
    df.lazy().sink_vortex(output)
    assert not output.closed
    assert_frame_equal(pl.read_vortex(output.getvalue()), df)


def test_zero_column_nonzero_height_fixture(tmp_path: Path) -> None:
    pa = pytest.importorskip("pyarrow")
    vortex = pytest.importorskip("vortex")
    batch = pa.RecordBatch.from_struct_array(pa.array([{}, {}, {}], type=pa.struct([])))
    path = tmp_path / "zero_columns.vortex"
    vortex.io.write(vortex.array(pa.Table.from_batches([batch])), str(path))
    assert pl.scan_vortex(path).select(pl.len()).collect().item() == 3
