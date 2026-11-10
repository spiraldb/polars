"""Check integer join ranges without loss of exact join semantics."""

from __future__ import annotations

from datetime import date, timedelta
from typing import TYPE_CHECKING

import pytest

import polars as pl
from polars.testing import assert_frame_equal

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from pathlib import Path

    from polars._typing import PolarsDataType
    from tests.conftest import PlMonkeyPatch


def _vortex_available() -> bool:
    from polars import _plr

    return hasattr(_plr.PyLazyFrame, "new_from_vortex")


pytestmark = pytest.mark.skipif(
    not _vortex_available(), reason="polars was not built with the 'vortex' feature"
)

APPLIED_RANGE = "[vortex]: applied runtime join range"


def _fact(dtype: PolarsDataType = pl.Int64) -> pl.DataFrame:
    keys = [*range(4096), None, 101, 101, 103]
    if dtype == pl.Int64:
        keys.extend([-(2**63), 2**63 - 1])
    else:
        keys.extend([0, 2**64 - 1])
    size = len(keys)
    return pl.DataFrame(
        {
            "k": pl.Series(keys, dtype=dtype),
            "row_id": range(size),
            "day": [date(1994, 1, 1) + timedelta(days=i % 3) for i in range(size)],
            "mode": ["SHIP" if i % 2 == 0 else "MAIL" for i in range(size)],
        }
    )


def _build(
    keys: Sequence[int | None], dtype: PolarsDataType = pl.Int64
) -> pl.LazyFrame:
    return (
        pl.DataFrame({"k": pl.Series(keys, dtype=dtype), "build_id": range(len(keys))})
        .lazy()
        .filter(pl.col("build_id") >= 0)
    )


def _write_fact(df: pl.DataFrame, tmp_path: Path, *, parts: int = 1) -> list[Path]:
    paths = []
    for part in range(parts):
        start = df.height * part // parts
        end = df.height * (part + 1) // parts
        path = tmp_path / f"fact-{part}.vortex"
        df.slice(start, end - start).write_vortex(path, row_block_size=128)
        paths.append(path)
    return paths


def _assert_exact(actual: pl.DataFrame, expected: pl.DataFrame) -> None:
    assert_frame_equal(
        actual.sort(actual.columns), expected.sort(expected.columns), check_exact=True
    )


def _check_join(
    factory: Callable[[pl.LazyFrame], pl.LazyFrame],
    fact: pl.DataFrame,
    paths: list[Path],
    plmonkeypatch: PlMonkeyPatch,
    capfd: pytest.CaptureFixture[str],
    *,
    attached: bool | None = True,
    accepted: bool | None = True,
    push_predicate: bool = True,
) -> tuple[pl.DataFrame, tuple[int, ...]]:
    from polars import _plr

    expected = factory(fact.lazy()).collect(engine="in-memory")
    plmonkeypatch.setenv("POLARS_VERBOSE", "1")
    plmonkeypatch.setenv("POLARS_JOIN_RUNTIME_FILTERS", "0")
    off = factory(
        pl.scan_vortex(paths, cache_mode="off", push_predicate=push_predicate)
    )
    assert "dynamic_predicate" not in off.explain(engine="streaming")
    _assert_exact(off.collect(engine="streaming"), expected)

    plmonkeypatch.setenv("POLARS_JOIN_RUNTIME_FILTERS", "1")
    on = factory(pl.scan_vortex(paths, cache_mode="off", push_predicate=push_predicate))
    plan = on.explain(engine="streaming")
    if attached is True:
        assert "dynamic_predicate" in plan, plan
    elif attached is False:
        assert "dynamic_predicate" not in plan, plan
    capfd.readouterr()
    _plr.reset_vortex_io_metrics()
    actual = on.collect(engine="streaming")
    err = capfd.readouterr().err
    metrics = _plr.get_vortex_io_metrics()
    if accepted is True:
        assert APPLIED_RANGE in err, (plan, err)
    elif accepted is False:
        assert APPLIED_RANGE not in err, (plan, err)
    _assert_exact(actual, expected)
    return actual, metrics


def test_selective_runtime_range_preserves_nulls_and_duplicate_matches(
    tmp_path: Path, plmonkeypatch: PlMonkeyPatch, capfd: pytest.CaptureFixture[str]
) -> None:
    fact = _fact()
    build = _build([101, 101, 103, None])
    out, _ = _check_join(
        lambda scan: scan.join(build, on="k"),
        fact,
        _write_fact(fact, tmp_path),
        plmonkeypatch,
        capfd,
    )
    assert out["k"].null_count() == 0
    assert out.filter(pl.col("k") == 101).height == 6


@pytest.mark.parametrize("dtype", [pl.Int64, pl.UInt64])
def test_runtime_range_preserves_integer_extremes(
    dtype: PolarsDataType,
    tmp_path: Path,
    plmonkeypatch: PlMonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    fact = _fact(dtype)
    low, high = (-(2**63), 2**63 - 1) if dtype == pl.Int64 else (0, 2**64 - 1)
    build = _build([low, high, None], dtype)
    _check_join(
        lambda scan: scan.join(build, on="k"),
        fact,
        _write_fact(fact, tmp_path),
        plmonkeypatch,
        capfd,
    )


@pytest.mark.parametrize("keys", [[], [None, None, None]], ids=["empty", "all-null"])
def test_empty_runtime_build_retains_exact_empty_result(
    keys: list[int | None],
    tmp_path: Path,
    plmonkeypatch: PlMonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    fact = _fact()
    build = _build(keys)
    out, _ = _check_join(
        lambda scan: scan.join(build, on="k"),
        fact,
        _write_fact(fact, tmp_path),
        plmonkeypatch,
        capfd,
        attached=None,
        accepted=None,
    )
    assert out.is_empty()


def test_nonoverlapping_runtime_range_retains_exact_empty_result(
    tmp_path: Path, plmonkeypatch: PlMonkeyPatch, capfd: pytest.CaptureFixture[str]
) -> None:
    fact = _fact()
    build = _build([6000, 6001])
    out, _ = _check_join(
        lambda scan: scan.join(build, on="k"),
        fact,
        _write_fact(fact, tmp_path),
        plmonkeypatch,
        capfd,
    )
    assert out.is_empty()


def test_multifile_runtime_range_retains_static_date_and_membership_filters(
    tmp_path: Path, plmonkeypatch: PlMonkeyPatch, capfd: pytest.CaptureFixture[str]
) -> None:
    fact = _fact()
    build = _build([101, 103])
    _check_join(
        lambda scan: scan.filter(
            (pl.col("day") >= date(1994, 1, 2)) & pl.col("mode").is_in(["MAIL"])
        ).join(build, on="k"),
        fact,
        _write_fact(fact, tmp_path, parts=3),
        plmonkeypatch,
        capfd,
    )


def test_static_filter_survives_trusted_dynamic_predicate(
    tmp_path: Path, plmonkeypatch: PlMonkeyPatch, capfd: pytest.CaptureFixture[str]
) -> None:
    fact = _fact()
    build = _build([101, 103])
    out, metrics = _check_join(
        lambda scan: scan.filter(
            (pl.col("day") >= date(1994, 1, 2)) & pl.col("mode").is_in(["ZZZ"])
        ).join(build, on="k"),
        fact,
        _write_fact(fact, tmp_path),
        plmonkeypatch,
        capfd,
    )
    assert out.is_empty()
    assert metrics[3] == 0  # No Arrow batch exports after the static file predicate.


@pytest.mark.parametrize(
    "case", ["nulls-equal", "left", "full", "nonplain", "push-disabled"]
)
def test_ineligible_runtime_join_retains_exact_fallback(
    case: str,
    tmp_path: Path,
    plmonkeypatch: PlMonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    fact = _fact()
    build = _build([101, 103, None])

    def join(scan: pl.LazyFrame) -> pl.LazyFrame:
        if case == "nulls-equal":
            return scan.join(build, on="k", nulls_equal=True)
        if case in {"left", "full"}:
            return scan.join(build, on="k", how=case)  # type: ignore[arg-type]
        if case == "nonplain":
            # Avoid arithmetic on the extreme-key fixture values.
            return scan.join(build, left_on=pl.col("row_id") + 1, right_on="k")
        return scan.join(build, on="k")

    _check_join(
        join,
        fact,
        _write_fact(fact, tmp_path),
        plmonkeypatch,
        capfd,
        attached=None if case == "nonplain" else False,
        accepted=False,
        push_predicate=case != "push-disabled",
    )


def test_pending_runtime_range_keeps_the_scan_and_exact_join(
    tmp_path: Path, plmonkeypatch: PlMonkeyPatch, capfd: pytest.CaptureFixture[str]
) -> None:
    fact = _fact()
    paths = _write_fact(fact, tmp_path)
    build = (
        _build([101, 103, 105])
        .join(pl.DataFrame({"k": [101, 103, 105], "tag": [0, 1, 2]}).lazy(), on="k")
        .filter(pl.col("build_id") >= 0)
    )
    options = pl.QueryOptFlags(join_order=False)
    expected = fact.lazy().join(build, on="k").collect(engine="in-memory")
    plmonkeypatch.setenv("POLARS_VERBOSE", "1")
    plmonkeypatch.setenv("POLARS_JOIN_SAMPLE_LIMIT", "1")
    for enabled in ["0", "1"]:
        plmonkeypatch.setenv("POLARS_JOIN_RUNTIME_FILTERS", enabled)
        query = pl.scan_vortex(paths, cache_mode="off").join(build, on="k")
        plan = query.explain(engine="streaming", optimizations=options)
        if enabled == "1":
            assert "dynamic_predicate" in plan, plan
            assert "BUILD SIDE: Prefer" in plan, plan
        capfd.readouterr()
        actual = query.collect(engine="streaming", optimizations=options)
        err = capfd.readouterr().err
        assert APPLIED_RANGE not in err, (plan, err)
        _assert_exact(actual, expected)


@pytest.mark.parametrize("operation", ["strict-cast", "opaque-udf"])
def test_runtime_range_does_not_hide_probe_expression_errors(
    operation: str,
    tmp_path: Path,
    plmonkeypatch: PlMonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    fact = _fact().with_columns(
        pl.when(pl.col("k") == 2)
        .then(pl.lit("bad"))
        .otherwise(pl.lit("1"))
        .alias("text")
    )
    paths = _write_fact(fact, tmp_path)
    build = _build([1])

    def fail_on_bad(values: pl.Series) -> pl.Series:
        if values.eq("bad").any():
            msg = "bad probe row"
            raise ValueError(msg)
        return pl.Series([1] * len(values), dtype=pl.Int64)

    plmonkeypatch.setenv("POLARS_VERBOSE", "1")
    for enabled in ["0", "1"]:
        plmonkeypatch.setenv("POLARS_JOIN_RUNTIME_FILTERS", enabled)
        expression = (
            pl.col("text").cast(pl.Int64, strict=True)
            if operation == "strict-cast"
            else pl.col("text").map_batches(
                fail_on_bad, return_dtype=pl.Int64, is_elementwise=True
            )
        )
        query = (
            pl.scan_vortex(paths, cache_mode="off")
            .with_columns(expression.alias("number"))
            .join(build, on="k")
            .select("k", "number", "build_id")
        )
        plan = query.explain(engine="streaming")
        assert "dynamic_predicate" not in plan, plan
        capfd.readouterr()
        if operation == "strict-cast":
            with pytest.raises(pl.exceptions.InvalidOperationError, match="conversion"):
                query.collect(engine="streaming")
        else:
            with pytest.raises(
                (ValueError, pl.exceptions.ComputeError), match="bad probe row"
            ):
                query.collect(engine="streaming")
        assert APPLIED_RANGE not in capfd.readouterr().err


@pytest.mark.parametrize(
    "case", ["string-key", "row-index-key", "indexed-physical-key"]
)
def test_runtime_range_rejects_noninteger_or_indexed_probe_scans(
    case: str,
    tmp_path: Path,
    plmonkeypatch: PlMonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    fact = _fact()
    if case == "string-key":
        fact = fact.with_columns(pl.col("k").cast(pl.String))
        build = (
            pl.DataFrame({"k": ["101", "103"], "build_id": [0, 1]})
            .lazy()
            .filter(pl.col("build_id") >= 0)
        )
    elif case == "row-index-key":
        build = _build([1, 3], pl.UInt32)
    else:
        build = _build([101, 103])

    def join(scan: pl.LazyFrame) -> pl.LazyFrame:
        if case == "row-index-key":
            return scan.with_row_index("idx").join(build, left_on="idx", right_on="k")
        if case == "indexed-physical-key":
            return scan.with_row_index("idx").join(build, on="k")
        return scan.join(build, on="k")

    _check_join(
        join,
        fact,
        _write_fact(fact, tmp_path),
        plmonkeypatch,
        capfd,
        attached=False,
        accepted=False,
    )


def test_semi_join_runtime_range_preserves_probe_duplicates_and_null_rules(
    tmp_path: Path, plmonkeypatch: PlMonkeyPatch, capfd: pytest.CaptureFixture[str]
) -> None:
    fact = _fact()
    build = _build([101, 101, 103, None])
    out, _ = _check_join(
        lambda scan: scan.join(build, on="k", how="semi"),
        fact,
        _write_fact(fact, tmp_path),
        plmonkeypatch,
        capfd,
    )
    assert out.height == 5
    assert out["k"].null_count() == 0
    assert out["row_id"].n_unique() == out.height


def test_anti_join_runtime_range_filters_only_the_large_right_probe(
    tmp_path: Path, plmonkeypatch: PlMonkeyPatch, capfd: pytest.CaptureFixture[str]
) -> None:
    fact = _fact()
    build = _build([101, 6000, None, 101])
    out, _ = _check_join(
        lambda scan: build.join(scan, on="k", how="anti"),
        fact,
        _write_fact(fact, tmp_path),
        plmonkeypatch,
        capfd,
    )
    assert out["k"].sort().to_list() == [None, 6000]


def test_composite_runtime_join_does_not_hide_a_computed_key_error(
    tmp_path: Path, plmonkeypatch: PlMonkeyPatch, capfd: pytest.CaptureFixture[str]
) -> None:
    fact = _fact().with_columns(
        pl.when(pl.col("k") == 2)
        .then(pl.lit("bad"))
        .otherwise(pl.lit("1"))
        .alias("text")
    )
    paths = _write_fact(fact, tmp_path)
    build = _build([1]).with_columns(pl.lit(1, dtype=pl.Int64).alias("cast_key"))
    plmonkeypatch.setenv("POLARS_VERBOSE", "1")
    for enabled in ["0", "1"]:
        plmonkeypatch.setenv("POLARS_JOIN_RUNTIME_FILTERS", enabled)
        query = pl.scan_vortex(paths, cache_mode="off").join(
            build,
            left_on=[pl.col("text").cast(pl.Int64, strict=True), pl.col("k")],
            right_on=["cast_key", "k"],
        )
        plan = query.explain(engine="streaming")
        assert "dynamic_predicate" not in plan, plan
        capfd.readouterr()
        with pytest.raises(pl.exceptions.InvalidOperationError, match="conversion"):
            query.collect(engine="streaming")
        assert APPLIED_RANGE not in capfd.readouterr().err
