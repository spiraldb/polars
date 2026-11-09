"""Compare materialized scans in isolated processes.

The storage cache state is uncontrolled.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import random
import resource
import shutil
import subprocess
import sys
import time
from pathlib import Path

SEGMENT_CACHE_POLICIES = {
    "native": "off: cache_mode='off'",
    "native-residual": "off: cache_mode='off'",
    "python": "off: without_segment_cache=True",
    "parquet": "not applicable: no Vortex segment cache; default Parquet reader options",
}


def reuse_inputs(
    directory: Path, source: Path, rows: int, width: int
) -> tuple[dict, dict]:
    """Reuse all six files and retain their original content hashes."""
    metadata = json.loads((source / "input_metadata.json").read_text())
    if metadata["rows"] != rows or metadata["numeric_width"] != width:
        msg = "The requested dimensions differ from the original input dimensions."
        raise ValueError(msg)
    methods = {}
    for order in ("sorted", "shuffled"):
        for suffix in ("arrow", "parquet", "vortex"):
            name = f"{order}.{suffix}"
            original, destination = source / name, directory / name
            if original.stat().st_size != metadata["files"][name]["bytes"]:
                msg = f"The input size differs from its original manifest: {name}"
                raise ValueError(msg)
            try:
                os.link(original, destination)
                methods[name] = "hardlink"
            except OSError:
                shutil.copy2(original, destination)
                methods[name] = "copy"
    return metadata, methods


def worker(args: argparse.Namespace) -> None:
    import polars as pl
    from polars.testing import assert_frame_equal

    directory = Path(args.directory)
    expected = pl.read_ipc(directory / f"{args.order}.arrow")
    threshold = int(args.rows * args.selectivity)
    columns = ["id", "text", "v0"] if args.narrow else expected.columns
    predicate = pl.col("id") < threshold
    expected = expected.filter(predicate).select(columns)
    # Reference materialization is outside the timer. It can affect storage caches.
    if args.backend == "parquet":
        query = pl.scan_parquet(directory / f"{args.order}.parquet")
    elif args.backend == "python":
        import vortex

        vortex.set_worker_threads(args.threads)
        query = vortex.open(
            str(directory / f"{args.order}.vortex"),
            without_segment_cache=True,
            memory_map=False,
        ).to_polars()
    else:
        query = pl.scan_vortex(
            directory / f"{args.order}.vortex",
            push_predicate=args.backend != "native-residual",
            cache_mode="off",
        )
    query = query.filter(predicate).select(columns)
    import polars._plr as runtime

    instrumented = args.backend.startswith("native") and hasattr(
        runtime, "get_vortex_io_metrics"
    )
    metrics = []
    timings = []
    for _ in range(args.repeats):
        if instrumented:
            runtime.reset_vortex_io_metrics()
        start = time.perf_counter()
        actual = query.collect(engine="streaming")
        timings.append(time.perf_counter() - start)
        metrics.append(runtime.get_vortex_io_metrics() if instrumented else None)
        assert_frame_equal(actual, expected)
    import polars._plr as runtime

    print(
        json.dumps(
            {
                "backend": args.backend,
                "order": args.order,
                "selectivity": args.selectivity,
                "projection": "narrow" if args.narrow else "all",
                "seconds": timings,
                "rows": actual.height,
                "expected_rows": int(args.rows * args.selectivity),
                "correct": True,
                "peak_rss_raw": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                "peak_rss_unit": "bytes" if sys.platform == "darwin" else "KiB",
                "peak_rss_scope": "process including reference",
                "io_metrics": metrics,
                "io_metric_fields": [
                    "requests",
                    "bytes_requested",
                    "bytes_received",
                    "batches",
                    "exported_column_batches",
                    "pushed_filters",
                ],
                "read_bytes": [m[2] for m in metrics] if instrumented else None,
                "exported_column_batches": [m[4] for m in metrics]
                if instrumented
                else None,
                "io_instrumented": instrumented,
                "io_counter_scope": "Adapter get_range buffers and Arrow materialization. Predicate evaluation can decode extra fields.",
                "cache_state": "uncontrolled OS cache, fresh process, repeated reads",
                "segment_cache_policy": SEGMENT_CACHE_POLICIES[args.backend],
                "vortex_memory_map": False if args.backend == "python" else None,
                "polars": pl.__version__,
                "frontend": pl.__file__,
                "runtime": runtime.__file__,
                "threads": pl.thread_pool_size(),
                "vortex_worker_threads": vortex.worker_threads()
                if args.backend == "python"
                else None,
                "cpu_pool": "additional Vortex pool plus Polars"
                if args.backend == "python"
                else "shared Polars pool",
            }
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory")
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--reuse-inputs-from", type=Path)
    parser.add_argument("--width", type=int, default=20)
    parser.add_argument(
        "--profile", default="release", help="Declared native build profile"
    )
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument(
        "--backend", choices=["native", "native-residual", "parquet", "python"]
    )
    parser.add_argument("--order", choices=["sorted", "shuffled"])
    parser.add_argument("--selectivity", type=float)
    parser.add_argument("--narrow", action="store_true")
    args = parser.parse_args()
    if args.worker:
        worker(args)
        return
    import vortex

    import polars as pl

    vortex.set_worker_threads(args.threads)
    directory = Path(args.directory)
    directory.mkdir(parents=True, exist_ok=True)
    if any(directory.iterdir()):
        parser.error("The output directory must be empty.")
    reused_metadata = None
    reuse_methods = {}
    if args.reuse_inputs_from is not None:
        reused_metadata, reuse_methods = reuse_inputs(
            directory, args.reuse_inputs_from.resolve(), args.rows, args.width
        )
    else:
        for order in ("sorted", "shuffled"):
            ids = list(range(args.rows))
            if order == "shuffled":
                random.Random(42).shuffle(ids)
            data = {
                "id": ids,
                "text": [f"record-{i % 10007:05d}" if i % 11 else None for i in ids],
            }
            data.update(
                {
                    f"v{j}": [
                        (
                            (i * 6364136223846793005 + j * 1442695040888963407)
                            % (2**63 - 1)
                        )
                        if (i + j) % 17
                        else None
                        for i in ids
                    ]
                    for j in range(args.width)
                }
            )
            df = pl.DataFrame(data)
            df.write_ipc(directory / f"{order}.arrow")
            df.write_parquet(
                directory / f"{order}.parquet", compression="zstd", row_group_size=65536
            )
            # The published writer supplies one common Vortex input for both readers.
            vortex.io.write(
                vortex.array(df.to_arrow()), str(directory / f"{order}.vortex")
            )
    from polars import _plr

    runtime_path = Path(_plr.__file__)
    runtime_sha256 = None
    if runtime_path.is_file() and runtime_path.suffix in (".so", ".pyd", ".dll"):
        with runtime_path.open("rb") as binary:
            runtime_sha256 = hashlib.file_digest(binary, "sha256").hexdigest()
    dirty_tracked = bool(
        subprocess.check_output(
            ["git", "status", "--porcelain", "--untracked-files=no"], text=True
        ).strip()
    )
    metadata = {
        "rows": args.rows,
        "numeric_width": args.width,
        "total_columns": args.width + 2,
        "dataset_seed": 42,
        "trial_order_seed": 43,
        "native_profile_declared": args.profile,
        "git_dirty_tracked": dirty_tracked,
        "native_runtime_path": str(runtime_path),
        "native_runtime_binary_sha256": runtime_sha256,
        "rustflags": os.environ.get("RUSTFLAGS"),
        "cflags": os.environ.get("CFLAGS"),
        "git_head_at_input_generation": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip(),
        "python": sys.version,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "threads_requested": args.threads,
        "cpu_pool_policy": "Python Vortex uses an additional worker pool. Native Vortex shares the Polars CPU pool. Equal worker requests do not establish equal total thread counts.",
        "vortex": vortex.__version__,
        "polars": pl.__version__,
        "parquet_compression": "zstd",
        "parquet_row_group_size": 65536,
        "vortex_writer": "published Python default",
        "float_columns": False,
        "repeats": args.repeats,
        "expected_cases": 64,
        "cache_state": "Uncontrolled operating system cache. No storage purge occurred.",
        "segment_cache_policies": SEGMENT_CACHE_POLICIES,
        "python_vortex_memory_map": False,
        "inputs_reused_from": str(args.reuse_inputs_from.resolve())
        if args.reuse_inputs_from
        else None,
        "input_reuse_methods": reuse_methods,
        "original_input_git_head": reused_metadata.get("git_head_at_input_generation")
        if reused_metadata
        else None,
        "files": {},
    }
    for path in sorted(directory.iterdir()):
        if path.suffix in (".arrow", ".vortex", ".parquet"):
            digest = hashlib.sha256()
            with path.open("rb") as source:
                for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
                    digest.update(chunk)
            metadata["files"][path.name] = {
                "bytes": path.stat().st_size,
                "sha256": digest.hexdigest(),
            }
    if reused_metadata is not None:
        if metadata["files"] != reused_metadata["files"]:
            msg = "The reused input hashes differ from the original manifest."
            raise ValueError(msg)
    (directory / "input_metadata.json").write_text(json.dumps(metadata, indent=2))
    cases = [
        (backend, order, selectivity, narrow)
        for backend in ("native", "native-residual", "parquet", "python")
        for order in ("sorted", "shuffled")
        for selectivity in (0.01, 0.1, 0.5, 1.0)
        for narrow in (False, True)
    ]
    random.Random(43).shuffle(cases)
    env = dict(os.environ, POLARS_MAX_THREADS=str(args.threads), POLARS_FORCE_PKG="32")
    results = []
    for backend, order, selectivity, narrow in cases:
        command = [
            sys.executable,
            str(Path(__file__).resolve()),
            str(directory),
            "--worker",
            "--backend",
            backend,
            "--order",
            order,
            "--selectivity",
            str(selectivity),
            "--rows",
            str(args.rows),
            "--repeats",
            str(args.repeats),
            "--threads",
            str(args.threads),
        ]
        if narrow:
            command.append("--narrow")
        result = subprocess.run(command, text=True, capture_output=True, env=env)
        if result.returncode:
            results.append(
                {
                    "backend": backend,
                    "order": order,
                    "selectivity": selectivity,
                    "narrow": narrow,
                    "error": result.stderr,
                }
            )
        else:
            results.append(json.loads(result.stdout))
        (directory / "results.json").write_text(json.dumps(results, indent=2))
    if any("error" in result for result in results):
        msg = "One or more benchmark cases failed. See results.json."
        raise SystemExit(msg)


if __name__ == "__main__":
    main()
