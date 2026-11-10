"""Read tests use files from the published Vortex writer."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

import polars as pl
from polars import _plr
from polars.testing import assert_frame_equal

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from polars._typing import EngineType
pytestmark = pytest.mark.skipif(
    not hasattr(_plr.PyLazyFrame, "new_from_vortex"),
    reason="Vortex feature is disabled",
)


@pytest.fixture
def write_fixture() -> Callable[[pl.DataFrame, Path], None]:
    vortex = pytest.importorskip("vortex")

    def write(df: pl.DataFrame, path: Path) -> None:
        vortex.io.write(vortex.array(df.to_arrow()), str(path))

    return write


@pytest.mark.parametrize("engine", ["streaming", "in-memory"])
def test_read_projection_filter_and_index(
    tmp_path: Path,
    write_fixture: Callable[[pl.DataFrame, Path], None],
    engine: EngineType,
) -> None:
    frames = [
        pl.DataFrame({"x": range(5), "s": list("abcde")}),
        pl.DataFrame({"x": range(5, 10), "s": list("fghij")}),
    ]
    paths = [tmp_path / f"{i}.vortex" for i in range(2)]
    for df, path in zip(frames, paths, strict=True):
        write_fixture(df, path)
    expected = pl.concat(frames)
    scan = pl.scan_vortex(paths)
    assert_frame_equal(
        scan.select("s", "x").collect(engine=engine), expected.select("s", "x")
    )
    assert scan.select(pl.len()).collect(engine=engine).item() == 10
    predicate = pl.col("x") > 3
    assert_frame_equal(
        scan.filter(predicate).head(3).collect(engine=engine),
        expected.filter(predicate).head(3),
    )
    indexed = pl.scan_vortex(paths, row_index_name="row", row_index_offset=8)
    assert_frame_equal(
        indexed.filter(predicate).slice(1, 3).collect(engine=engine),
        expected.with_row_index("row", offset=8).filter(predicate).slice(1, 3),
    )


@pytest.mark.parametrize("push_predicate", [False, True])
def test_read_float_residuals(
    tmp_path: Path,
    write_fixture: Callable[[pl.DataFrame, Path], None],
    push_predicate: bool,
) -> None:
    df = pl.DataFrame({"x": [-0.0, 0.0, -1.0, float("nan"), float("inf")]})
    path = tmp_path / "float.vortex"
    write_fixture(df, path)
    for predicate in (pl.col("x") == 0.0, pl.col("x") < 0.0, pl.col("x") > 1.0):
        assert_frame_equal(
            pl.scan_vortex(path, push_predicate=push_predicate)
            .filter(predicate)
            .collect(),
            df.filter(predicate),
        )
