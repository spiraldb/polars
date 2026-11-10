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


@pytest.fixture(scope="module")
def typed_predicate_file(
    tmp_path_factory: pytest.TempPathFactory,
) -> tuple[Path, pl.DataFrame]:
    from decimal import Decimal

    vx = pytest.importorskip("vortex")
    pa = pytest.importorskip("pyarrow")
    table = pa.table(
        {
            "row": pa.array(range(8), type=pa.int64()),
            "text": pa.array(
                ["alpha", "beta", None, "gamma", "alpha", "", "beta", None]
            ),
            "choices": pa.array(
                [["alpha"], None, [None], [], ["beta"], [""], ["beta", None], None],
                type=pa.list_(pa.string()),
            ),
            "day": pa.array([-366, -1, None, 0, 1, 365, 366, 10_000], type=pa.date32()),
            "other_day": pa.array(
                [-365, 0, None, 1, 1, 364, 367, 10_001], type=pa.date32()
            ),
            "millisecond_date": pa.array(
                [-86_400_001, -1, None, 0, 1, 86_400_001, 86_400_002, 864_000_000],
                type=pa.date64(),
            ),
            "amount": pa.array(
                [
                    Decimal(value) if value is not None else None
                    for value in [
                        "-100.00",
                        "-0.01",
                        None,
                        "0.00",
                        "0.05",
                        "0.10",
                        "100.00",
                        "9999999999.99",
                    ]
                ],
                type=pa.decimal128(12, 2),
            ),
        }
    )
    path = tmp_path_factory.mktemp("vortex_typed_predicate") / "typed.vortex"
    vx.io.write(table, str(path))
    return path, pl.from_arrow(table)


def _assert_typed_predicate(
    source: tuple[Path, pl.DataFrame],
    predicate: pl.Expr,
    push_predicate: bool,
    engine: Literal["streaming", "in-memory"],
    *,
    expect_push: bool | None = None,
) -> None:
    from polars import _plr

    path, frame = source
    for offset, length in [(0, 8), (1, 5)]:
        expected = (
            frame.lazy()
            .slice(offset, length)
            .filter(predicate)
            .select("row")
            .collect(engine=engine)
        )
        _plr.reset_vortex_io_metrics()
        actual = (
            pl.scan_vortex(path, push_predicate=push_predicate)
            .slice(offset, length)
            .filter(predicate)
            .select("row")
            .collect(engine=engine)
        )
        assert_frame_equal(actual, expected)
        filters = _plr.get_vortex_io_metrics()[5]
        if not push_predicate or expect_push is False:
            assert filters == 0
        elif expect_push is True:
            assert filters > 0


@pytest.mark.parametrize("engine", ["streaming", "in-memory"])
@pytest.mark.parametrize("push_predicate", [True, False])
@pytest.mark.parametrize("nulls_equal", [True, False])
@pytest.mark.parametrize("negate", [True, False])
@pytest.mark.parametrize("values", [["alpha"], ["alpha", None], [], [None]])
def test_native_vortex_literal_membership_nulls_and_not(
    typed_predicate_file: tuple[Path, pl.DataFrame],
    values: list[str | None],
    nulls_equal: bool,
    negate: bool,
    push_predicate: bool,
    engine: Literal["streaming", "in-memory"],
) -> None:
    values_expr = pl.lit(pl.Series(values, dtype=pl.String)).implode()
    predicate = pl.col("text").is_in(values_expr, nulls_equal=nulls_equal)
    if negate:
        predicate = ~predicate
    _assert_typed_predicate(
        typed_predicate_file,
        predicate,
        push_predicate,
        engine,
        expect_push=True if any(value is not None for value in values) else None,
    )


@pytest.mark.parametrize("engine", ["streaming", "in-memory"])
@pytest.mark.parametrize("push_predicate", [True, False])
@pytest.mark.parametrize("nulls_equal", [True, False])
@pytest.mark.parametrize("collection", ["rowwise", "null", "large"])
def test_native_vortex_membership_collection_fallbacks(
    typed_predicate_file: tuple[Path, pl.DataFrame],
    collection: str,
    nulls_equal: bool,
    push_predicate: bool,
    engine: Literal["streaming", "in-memory"],
) -> None:
    if collection == "rowwise":
        values = pl.col("choices")
    elif collection == "null":
        values = pl.lit(None, dtype=pl.List(pl.String))
    else:
        values = pl.lit(
            pl.Series([str(i) for i in range(65)], dtype=pl.String)
        ).implode()
    predicate = ~pl.col("text").is_in(values, nulls_equal=nulls_equal)
    _assert_typed_predicate(
        typed_predicate_file, predicate, push_predicate, engine, expect_push=False
    )


@pytest.mark.parametrize("engine", ["streaming", "in-memory"])
@pytest.mark.parametrize("push_predicate", [True, False])
@pytest.mark.parametrize("column", ["day", "millisecond_date"])
@pytest.mark.parametrize("operator", ["eq", "lt", "ge"])
def test_native_vortex_date_literal_physical_units(
    typed_predicate_file: tuple[Path, pl.DataFrame],
    column: str,
    operator: str,
    push_predicate: bool,
    engine: Literal["streaming", "in-memory"],
) -> None:
    from datetime import date

    lhs, rhs = pl.col(column), pl.lit(date(1970, 1, 1))
    predicate = {"eq": lhs == rhs, "lt": lhs < rhs, "ge": lhs >= rhs}[operator]
    _assert_typed_predicate(
        typed_predicate_file,
        predicate,
        push_predicate,
        engine,
        expect_push=column == "day",
    )


@pytest.mark.parametrize("engine", ["streaming", "in-memory"])
@pytest.mark.parametrize("push_predicate", [True, False])
@pytest.mark.parametrize("operator", ["eq", "lt", "ge"])
@pytest.mark.parametrize(
    "value", ["0.05", "0.0500", "-0.0100", "0.051", "99999999999.99"]
)
def test_native_vortex_decimal_literal_exact_rescale(
    typed_predicate_file: tuple[Path, pl.DataFrame],
    value: str,
    operator: str,
    push_predicate: bool,
    engine: Literal["streaming", "in-memory"],
) -> None:
    from decimal import Decimal

    lhs, rhs = pl.col("amount"), pl.lit(Decimal(value))
    predicate = {"eq": lhs == rhs, "lt": lhs < rhs, "ge": lhs >= rhs}[operator]
    _assert_typed_predicate(
        typed_predicate_file,
        predicate,
        push_predicate,
        engine,
        expect_push=value in {"0.05", "0.0500", "-0.0100"},
    )


@pytest.mark.parametrize("engine", ["streaming", "in-memory"])
@pytest.mark.parametrize("push_predicate", [True, False])
def test_native_vortex_date32_field_comparison(
    typed_predicate_file: tuple[Path, pl.DataFrame],
    push_predicate: bool,
    engine: Literal["streaming", "in-memory"],
) -> None:
    assert typed_predicate_file[1].schema["millisecond_date"] == pl.Datetime("ms")
    _assert_typed_predicate(
        typed_predicate_file,
        pl.col("day") < pl.col("other_day"),
        push_predicate,
        engine,
        expect_push=True,
    )


@pytest.mark.parametrize("engine", ["streaming", "in-memory"])
@pytest.mark.parametrize("push_predicate", [True, False])
@pytest.mark.parametrize("case", ["length", "vector", "gather", "udf"])
def test_native_vortex_preserves_input_dependent_residuals(
    typed_predicate_file: tuple[Path, pl.DataFrame],
    case: str,
    push_predicate: bool,
    engine: Literal["streaming", "in-memory"],
) -> None:
    path, frame = typed_predicate_file
    if case == "length":
        predicate = (pl.len() > 4) & (pl.col("row") > 5)
    elif case == "vector":
        predicate = (pl.col("row") == pl.lit(pl.Series(range(8), dtype=pl.Int64))) & (
            pl.col("row") > 4
        )
    elif case == "gather":
        predicate = (pl.col("row").gather([999]) > 0) & (pl.col("row") < 0)
    else:

        def fail(values: pl.Series) -> pl.Series:
            msg = "fallible Vortex residual"
            raise ValueError(msg)

        predicate = pl.col("row").map_batches(
            fail, return_dtype=pl.Boolean, is_elementwise=True
        ) & (pl.col("row") < 0)
    reference = frame.lazy().filter(predicate).select("row")
    scan = (
        pl.scan_vortex(path, push_predicate=push_predicate)
        .filter(predicate)
        .select("row")
    )
    if case in {"udf", "gather"}:
        with pytest.raises((ValueError, pl.exceptions.PolarsError)):
            reference.collect(engine=engine)
        with pytest.raises((ValueError, pl.exceptions.PolarsError)):
            scan.collect(engine=engine)
    else:
        assert_frame_equal(
            scan.collect(engine=engine), reference.collect(engine=engine)
        )


@pytest.mark.parametrize("engine", ["streaming", "in-memory"])
@pytest.mark.parametrize("push_predicate", [True, False])
@pytest.mark.parametrize("closed", ["both", "left", "right", "none"])
@pytest.mark.parametrize("typed_bounds", [False, True])
def test_native_vortex_between_exact_decimal_bounds(
    typed_predicate_file: tuple[Path, pl.DataFrame],
    closed: Literal["both", "left", "right", "none"],
    typed_bounds: bool,
    push_predicate: bool,
    engine: Literal["streaming", "in-memory"],
) -> None:
    from decimal import Decimal

    lower, upper = pl.lit(Decimal("0.0000")), pl.lit(Decimal("0.1000"))
    if typed_bounds:
        lower = pl.lit(Decimal("0.00"), dtype=pl.Decimal(12, 2))
        upper = pl.lit(Decimal("0.10"), dtype=pl.Decimal(12, 2))
    predicate = pl.col("amount").is_between(lower, upper, closed=closed)
    _assert_typed_predicate(
        typed_predicate_file, predicate, push_predicate, engine, expect_push=True
    )


@pytest.mark.parametrize("engine", ["streaming", "in-memory"])
@pytest.mark.parametrize("push_predicate", [True, False])
def test_native_vortex_list_ordering_error_survives_empty_gate(
    tmp_path: Path,
    push_predicate: bool,
    engine: Literal["streaming", "in-memory"],
) -> None:
    vx = pytest.importorskip("vortex")
    pa = pytest.importorskip("pyarrow")
    table = pa.table({"a": [[1], [2]], "b": [[0], [1]], "k": [1, 2]})
    path = tmp_path / "list_ordering.vortex"
    vx.io.write(table, str(path))
    predicate = (pl.col("a") > pl.col("b")) & (pl.col("k") < 0)
    with pytest.raises(pl.exceptions.InvalidOperationError):
        pl.from_arrow(table).lazy().filter(predicate).collect(engine=engine)
    with pytest.raises(pl.exceptions.InvalidOperationError):
        pl.scan_vortex(path, push_predicate=push_predicate).filter(predicate).collect(
            engine=engine
        )


@pytest.mark.parametrize("engine", ["streaming", "in-memory"])
@pytest.mark.parametrize("push_predicate", [True, False])
def test_native_vortex_between_preserves_decimal_cast_overflow(
    tmp_path: Path,
    push_predicate: bool,
    engine: Literal["streaming", "in-memory"],
) -> None:
    from decimal import Decimal

    vx = pytest.importorskip("vortex")
    pa = pytest.importorskip("pyarrow")
    boundary = Decimal("9" * 36 + ".99")
    table = pa.table(
        {
            "amount": pa.array(
                [boundary, boundary.copy_negate()], type=pa.decimal128(38, 2)
            ),
            "k": [1, 2],
        }
    )
    path = tmp_path / "between_cast_overflow.vortex"
    vx.io.write(table, str(path))
    predicate = (
        pl.col("amount")
        .cast(pl.Decimal(38, 4), strict=True)
        .is_between(
            pl.lit(Decimal("-1.0000"), dtype=pl.Decimal(38, 4)),
            pl.lit(Decimal("1.0000"), dtype=pl.Decimal(38, 4)),
        )
    )
    frame = pl.from_arrow(table)
    with pytest.raises(pl.exceptions.InvalidOperationError):
        frame.lazy().filter(predicate).collect(engine=engine)
    with pytest.raises(pl.exceptions.InvalidOperationError):
        pl.scan_vortex(path, push_predicate=push_predicate).filter(predicate).collect(
            engine=engine
        )
    gate = pl.col("k") < 0
    for combined in [predicate & gate, predicate | gate]:
        reference = frame.lazy().filter(combined)
        query = pl.scan_vortex(path, push_predicate=push_predicate).filter(combined)
        # Polars can short-circuit an empty AND gate before the cast.
        try:
            expected = reference.collect(engine=engine)
        except pl.exceptions.PolarsError as error:
            with pytest.raises(type(error)):
                query.collect(engine=engine)
        else:
            assert_frame_equal(query.collect(engine=engine), expected)
