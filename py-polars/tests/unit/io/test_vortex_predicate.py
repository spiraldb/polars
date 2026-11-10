"""These tests compare native Vortex predicates with Polars predicates."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

import polars as pl
from polars.testing import assert_frame_equal

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Literal


def _native_vortex_available() -> bool:
    from polars import _plr

    return hasattr(_plr.PyLazyFrame, "new_from_vortex")


pytestmark = pytest.mark.skipif(
    not _native_vortex_available(),
    reason="polars was not built with the 'vortex' feature",
)

_INTEGER_TYPES = {
    "i8": (pl.Int8, -(2**7), 2**7 - 1),
    "u8": (pl.UInt8, 0, 2**8 - 1),
    "i16": (pl.Int16, -(2**15), 2**15 - 1),
    "u16": (pl.UInt16, 0, 2**16 - 1),
    "i32": (pl.Int32, -(2**31), 2**31 - 1),
    "u32": (pl.UInt32, 0, 2**32 - 1),
    "i64": (pl.Int64, -(2**63), 2**63 - 1),
    "u64": (pl.UInt64, 0, 2**64 - 1),
}


@pytest.fixture(scope="module")
def predicate_file(
    tmp_path_factory: pytest.TempPathFactory,
) -> tuple[Path, pl.DataFrame]:
    vx = pytest.importorskip("vortex")
    pa = pytest.importorskip("pyarrow")
    columns = {
        "row": pa.array(range(12), type=pa.int64()),
        "a": pa.array([True, True, False, False, None, None] * 2),
        "b": pa.array([True, None, False, None, True, False] * 2),
        "text": pa.array(
            [
                "%a",
                "a%",
                "_a",
                "a_",
                "\\a",
                "a\\",
                "plain",
                None,
                "%%",
                "__",
                "\\\\",
                "",
            ]
        ),
        "nested": pa.array(
            [None, {"value": None}, {"value": -1}, {"value": 1}] * 3,
            type=pa.struct([pa.field("value", pa.int64())]),
        ),
    }
    for name, (dtype, minimum, maximum) in _INTEGER_TYPES.items():
        arrow_type = getattr(pa, str(dtype).lower())()
        columns[name] = pa.array(
            [minimum, minimum + 1, 0, 1, maximum - 1, maximum, None, 2, 3, 4, 5, 6],
            type=arrow_type,
        )
    table = pa.table(columns)
    path = tmp_path_factory.mktemp("vortex_predicate") / "predicates.vortex"
    vx.io.write(table, str(path))
    return path, pl.from_arrow(table)


def _predicate(case: str) -> pl.Expr:
    a, b = pl.col("a"), pl.col("b")
    nested = pl.col("nested").struct.field("value")
    cases = {
        "boolean_and": a & b,
        "boolean_or": a | b,
        "boolean_not": ~a,
        "boolean_not_or": ~(a | b),
        "boolean_not_and": ~(a & b),
        "boolean_eq_missing": a.eq_missing(b),
        "null_equality": pl.col("i8").eq_missing(None),
        "is_not_null": pl.col("i8").is_not_null(),
        "membership_null_equal": pl.col("i8").is_in(
            [None, -128, 127], nulls_equal=True
        ),
        "membership_null_unequal": pl.col("i8").is_in([None, -128, 127]),
        "prefix_percent": pl.col("text").str.starts_with("%"),
        "prefix_underscore": pl.col("text").str.starts_with("_"),
        "prefix_backslash": pl.col("text").str.starts_with("\\"),
        "suffix_percent": pl.col("text").str.ends_with("%"),
        "suffix_underscore": pl.col("text").str.ends_with("_"),
        "suffix_backslash": pl.col("text").str.ends_with("\\"),
        "safe_and_residual": (pl.col("i8") >= 0)
        & (pl.col("text").str.len_chars() == 2),
        "nested_field": nested > 0,
        "nested_null": nested.is_null(),
    }
    return cases[case]


def _assert_predicate(
    source: tuple[Path, pl.DataFrame],
    predicate: pl.Expr,
    push_predicate: bool,
    engine: Literal["streaming", "in-memory"],
) -> None:
    path, frame = source
    expected = frame.lazy().filter(predicate).select("row").collect(engine=engine)
    actual = (
        pl.scan_vortex(path, push_predicate=push_predicate)
        .filter(predicate)
        .select("row")
        .collect(engine=engine)
    )
    assert_frame_equal(actual, expected)


@pytest.mark.parametrize("engine", ["streaming", "in-memory"])
@pytest.mark.parametrize("push_predicate", [True, False])
@pytest.mark.parametrize(
    "case",
    [
        "boolean_and",
        "boolean_or",
        "boolean_not",
        "boolean_not_or",
        "boolean_not_and",
        "boolean_eq_missing",
        "null_equality",
        "is_not_null",
        "membership_null_equal",
        "membership_null_unequal",
        "prefix_percent",
        "prefix_underscore",
        "prefix_backslash",
        "suffix_percent",
        "suffix_underscore",
        "suffix_backslash",
        "safe_and_residual",
        "nested_field",
        "nested_null",
    ],
)
def test_native_vortex_predicate_matches_polars(
    predicate_file: tuple[Path, pl.DataFrame],
    case: str,
    push_predicate: bool,
    engine: Literal["streaming", "in-memory"],
) -> None:
    _assert_predicate(predicate_file, _predicate(case), push_predicate, engine)


@pytest.mark.parametrize("engine", ["streaming", "in-memory"])
@pytest.mark.parametrize("push_predicate", [True, False])
@pytest.mark.parametrize("name", list(_INTEGER_TYPES))
def test_native_vortex_integer_boundaries(
    predicate_file: tuple[Path, pl.DataFrame],
    name: str,
    push_predicate: bool,
    engine: Literal["streaming", "in-memory"],
) -> None:
    dtype, minimum, maximum = _INTEGER_TYPES[name]
    column = pl.col(name)
    for predicate in (
        column == minimum,
        column == pl.lit(maximum, dtype=dtype),
        pl.lit(minimum, dtype=dtype) < column,
    ):
        _assert_predicate(predicate_file, predicate, push_predicate, engine)


@pytest.mark.parametrize("engine", ["streaming", "in-memory"])
@pytest.mark.parametrize("push_predicate", [True, False])
@pytest.mark.parametrize("operator", ["or", "and"])
def test_native_vortex_preserves_fallible_residual_error(
    tmp_path: Path,
    operator: str,
    push_predicate: bool,
    engine: Literal["streaming", "in-memory"],
) -> None:
    vx = pytest.importorskip("vortex")
    pa = pytest.importorskip("pyarrow")
    table = pa.table(
        {"row": [0, 1, 2], "gate": [True, False, False], "raw": ["1", "bad", "2"]}
    )
    path = tmp_path / "fallible.vortex"
    vx.io.write(table, str(path))
    frame = pl.from_arrow(table)
    fallible = pl.col("raw").cast(pl.Int64) > 0
    gate = pl.col("gate")
    predicate = gate | fallible if operator == "or" else gate & fallible
    reference = frame.lazy().filter(predicate).select("row")
    query = (
        pl.scan_vortex(path, push_predicate=push_predicate)
        .filter(predicate)
        .select("row")
    )
    if operator == "or":
        with pytest.raises(pl.exceptions.InvalidOperationError):
            reference.collect(engine=engine)
        with pytest.raises(pl.exceptions.InvalidOperationError):
            query.collect(engine=engine)
    else:
        try:
            expected = reference.collect(engine=engine)
        except pl.exceptions.PolarsError as error:
            with pytest.raises(type(error)):
                query.collect(engine=engine)
        else:
            assert_frame_equal(query.collect(engine=engine), expected)
