"""Native scans must finish with small shared budgets and ordered files."""

from __future__ import annotations

import os
import subprocess
import sys
from typing import TYPE_CHECKING

import pytest

from polars import _plr

if TYPE_CHECKING:
    from pathlib import Path


pytestmark = pytest.mark.skipif(
    not hasattr(_plr.PyLazyFrame, "new_from_vortex"),
    reason="polars was not built with the 'vortex' feature",
)


_WORKER = r"""
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
import sys

import polars as pl
from polars.testing import assert_frame_equal

root = Path(sys.argv[1])
rows = 100_003
frames = []
paths = []
for file_index in range(3):
    frame = pl.DataFrame({
        "id": range(file_index * rows, (file_index + 1) * rows),
        "mode": [["MAIL", "SHIP", "RAIL", None][i % 4] for i in range(rows)],
        "date": [date(1993, 1, 1) + timedelta(days=i % 730) for i in range(rows)],
        "amount": pl.Series([Decimal(i % 150) / 100 for i in range(rows)],
                            dtype=pl.Decimal(12, 2)),
        "payload": [str(i % 127) + "x" * (i % 257) for i in range(rows)],
    })
    path = root / f"part-{file_index}.vortex"
    frame.write_vortex(path)
    paths.append(path)
    frames.append(frame)
empty = root / "empty.vortex"
frames[0].head(0).write_vortex(empty)
paths.insert(0, empty)
paths.insert(2, empty)
expected = pl.concat(frames)

def scan():
    return pl.scan_vortex(paths, scan_concurrency=4)

assert_frame_equal(scan().collect(engine="streaming"), expected)
predicate = (
    pl.col("mode").is_in(["MAIL", "SHIP"])
    & (pl.col("date") >= date(1994, 1, 1))
    & (pl.col("amount") < Decimal("0.25"))
)
assert_frame_equal(
    scan().filter(predicate).collect(engine="streaming"),
    expected.filter(predicate),
)
for offset, length in [(65_530, 30), (rows - 5, 20), (2 * rows - 5, 20)]:
    assert_frame_equal(
        scan().slice(offset, length).collect(engine="streaming"),
        expected.slice(offset, length),
    )
assert_frame_equal(scan().tail(120).collect(engine="streaming"), expected.tail(120))
prune_middle = (pl.col("id") < rows) | (pl.col("id") >= 2 * rows)
assert_frame_equal(
    scan().filter(prune_middle).collect(engine="streaming"),
    expected.filter(prune_middle),
)
for _ in range(3):
    assert_frame_equal(scan().head(1).collect(engine="streaming"), expected.head(1))
assert_frame_equal(
    scan().filter(pl.col("id") < 0).collect(engine="streaming"),
    expected.head(0),
)
assert_frame_equal(
    scan().filter(pl.col("mode") == "MAIL").select(pl.len()).collect(engine="streaming"),
    expected.filter(pl.col("mode") == "MAIL").select(pl.len()),
)

def concurrent_query(index):
    condition = pl.col("id") % 2 == index
    return scan().filter(condition).select("id", "amount").collect(engine="streaming")

with ThreadPoolExecutor(2) as executor:
    results = list(executor.map(concurrent_query, [0, 1]))
for index, result in enumerate(results):
    assert_frame_equal(result, expected.filter(pl.col("id") % 2 == index).select("id", "amount"))
print("resource cases passed", flush=True)
"""


@pytest.mark.parametrize("threads", [1, 4])
@pytest.mark.parametrize(("prefetch", "kilobytes"), [(1, 1), (4, 64)])
def test_native_vortex_shared_budget_progress(
    tmp_path: Path, threads: int, prefetch: int, kilobytes: int
) -> None:
    env = dict(
        os.environ,
        POLARS_MAX_THREADS=str(threads),
        POLARS_ASYNC_THREAD_COUNT=str(threads),
        POLARS_MAX_BLOCKING_THREAD_COUNT="16",
        POLARS_VORTEX_PREFETCH_SIZE=str(prefetch),
        POLARS_VORTEX_PREFETCH_KBYTES_BUDGET=str(kilobytes),
    )
    result = subprocess.run(
        [sys.executable, "-c", _WORKER, str(tmp_path)],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "resource cases passed" in result.stdout
