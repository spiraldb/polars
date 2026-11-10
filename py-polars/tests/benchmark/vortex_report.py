"""Create a local report only if all benchmark results pass correctness checks."""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    metadata = json.loads((args.directory / "input_metadata.json").read_text())
    results = json.loads((args.directory / "results.json").read_text())
    expected = {
        (backend, order, selectivity, projection)
        for backend in ("native", "native-residual", "parquet", "python")
        for order in ("sorted", "shuffled")
        for selectivity in (0.01, 0.1, 0.5, 1.0)
        for projection in ("all", "narrow")
    }
    actual = {
        (r.get("backend"), r.get("order"), r.get("selectivity"), r.get("projection"))
        for r in results
    }
    errors = [
        r
        for r in results
        if r.get("correct") is not True
        or "error" in r
        or r.get("rows") != r.get("expected_rows")
        or len(r.get("seconds", [])) != metadata["repeats"]
    ]
    policies = metadata.get("segment_cache_policies")
    policy_errors = policies is not None and any(
        result.get("segment_cache_policy") != policies[result["backend"]]
        for result in results
    )
    if errors or policy_errors or actual != expected or len(results) != len(expected):
        msg = "Report blocked. The run lacks complete, correct results for every case."
        raise SystemExit(msg)
    lines = [
        "# Native Vortex benchmark",
        "",
        "All 64 cases passed exact result comparisons against the reference input.",
        "",
        f"The input contains {metadata['rows']:,} rows and {metadata['total_columns']} columns.",
        "The dataset contains integer and string columns. It contains no float columns.",
        "The dataset seed is 42. The trial order seed is 43.",
        f"The declared native build profile is `{metadata['native_profile_declared']}`.",
        f"The process requests {metadata['threads_requested']} Polars threads.",
        f"The Python backend requests {metadata['threads_requested']} additional Vortex workers.",
        "Native Vortex shares the Polars CPU pool. These requests do not establish equal total thread counts.",
        "",
        f"Python: `{metadata['python'].splitlines()[0]}`.",
        f"Platform: `{metadata['platform']}`.",
        f"Polars: `{metadata['polars']}`. Vortex: `{metadata['vortex']}`.",
        f"Source commit: `{metadata.get('git_head_at_input_generation', 'unrecorded')}`.",
        f"Tracked source changes: `{metadata.get('git_dirty_tracked', 'unrecorded')}`.",
        f"Native binary SHA-256: `{metadata.get('native_runtime_binary_sha256', 'unrecorded')}`.",
        "",
        "Each case uses a separate process. Each process repeats the same query.",
        "The operating system cache state is uncontrolled. No storage purge occurred.",
        f"Segment cache policies: `{json.dumps(metadata.get('segment_cache_policies', 'unrecorded'), sort_keys=True)}`.",
        f"Python Vortex memory mapping: `{metadata.get('python_vortex_memory_map', 'unrecorded')}`.",
        f"Original inputs: `{metadata.get('inputs_reused_from') or 'generated for this run'}`.",
        "The timer excludes reference construction and scan plan construction.",
        "Peak resident memory includes the reference result and runtime initialization.",
        "Adapter bytes and exported column batches remain unmeasured unless an instrumented result supplies them.",
        "These results do not establish cold storage performance.",
        "Native byte counters describe explicit get_range buffers. They do not measure physical reads or transport bytes.",
        "Exported column batches sum the Arrow batch widths. They do not count unique columns or internal decodes.",
        "Predicate evaluation can decode additional fields.",
        "Each trial resets native counters after plan construction and before execution.",
        "Schema discovery before the reset is outside these counters.",
        "",
        "| Order | Selected rows | Projection | Backend | First run, seconds | Repeated median, seconds | Peak RSS, raw |",
        "| --- | ---: | --- | --- | ---: | ---: | ---: |",
    ]
    for result in sorted(
        results,
        key=lambda r: (r["order"], r["selectivity"], r["projection"], r["backend"]),
    ):
        repeated = result["seconds"][1:]
        median = f"{statistics.median(repeated):.6f}" if repeated else "unavailable"
        lines.append(
            f"| {result['order']} | {result['rows']} | {result['projection']} | {result['backend']} | {result['seconds'][0]:.6f} | {median} | {result['peak_rss_raw']} {result['peak_rss_unit']} |"
        )
    instrumented = [r for r in results if r.get("io_instrumented")]
    if instrumented:
        lines += [
            "",
            "| Order | Selected rows | Projection | Backend | First execution IO counters |",
            "| --- | ---: | --- | --- | --- |",
        ]
        for result in sorted(
            instrumented,
            key=lambda r: (r["order"], r["selectivity"], r["projection"], r["backend"]),
        ):
            counters = dict(
                zip(result["io_metric_fields"], result["io_metrics"][0], strict=True)
            )
            lines.append(
                f"| {result['order']} | {result['rows']} | {result['projection']} | {result['backend']} | `{json.dumps(counters, sort_keys=True)}` |"
            )
    lines += [
        "",
        "The input manifest records file sizes and SHA-256 digests in `input_metadata.json`.",
        "The raw observations remain in `results.json`.",
        "",
    ]
    (args.directory / "report.md").write_text("\n".join(lines))


if __name__ == "__main__":
    main()
