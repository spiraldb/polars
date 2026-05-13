"""User-facing Python API for reading Vortex files into Polars."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

from polars._utils.wrap import wrap_ldf
from polars.io._utils import get_sources
from polars.io.cloud.credential_provider._builder import (
    _init_credential_provider_builder,
)
from polars.io.scan_options._options import ScanOptions

with contextlib.suppress(ImportError):
    from polars._plr import PyLazyFrame

if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import Literal

    from polars import DataFrame, LazyFrame
    from polars._typing import (
        FileSource,
        SchemaDict,
        StorageOptionsDict,
    )
    from polars.io.cloud import CredentialProviderFunction


def scan_vortex(
    source: FileSource,
    *,
    n_rows: int | None = None,
    row_index_name: str | None = None,
    row_index_offset: int = 0,
    use_statistics: bool = True,
    push_predicate: bool = True,
    push_projection: bool = True,
    aggressive_pushdown: bool = False,
    initial_read_size: int | None = None,
    scan_concurrency: int | None = None,
    hive_partitioning: bool | None = None,
    glob: bool = True,
    hidden_file_prefix: str | Sequence[str] | None = None,
    schema: SchemaDict | None = None,
    hive_schema: SchemaDict | None = None,
    try_parse_hive_dates: bool = True,
    rechunk: bool = False,
    cache: bool = True,
    storage_options: StorageOptionsDict | None = None,
    credential_provider: CredentialProviderFunction | Literal["auto"] | None = "auto",
    include_file_paths: str | None = None,
    missing_columns: Literal["insert", "raise"] = "raise",
    extra_columns: Literal["ignore", "raise"] = "raise",
) -> LazyFrame:
    """
    Lazily read from a local or cloud-hosted Vortex file (or files).

    This function allows the query optimizer to push down predicates and projections to
    the scan level, leveraging Vortex's expression-based pushdown and zone-level pruning
    for substantial perf wins on filtered scans.

    Parameters
    ----------
    source
        Path(s) to a file or directory. When the path has a cloud scheme
        (``s3://``, ``gs://``, ``az://``), authenticate via ``storage_options``.
    n_rows
        Stop reading from the file after reading ``n_rows`` rows.
    row_index_name
        If set, insert a row index column with this name.
    row_index_offset
        Offset to start the row index column (only used if ``row_index_name`` is set).
    use_statistics
        If True (default), populate ``table_statistics`` from the Vortex footer so the
        optimizer can perform whole-file pruning.
    push_predicate
        If True (default), translate the pushable parts of any filter into Vortex
        expressions and hand them to ``ScanBuilder::with_filter``. Disable if a buggy
        convertor causes incorrect rows to be skipped (the multi-scan layer always
        re-applies the full predicate post-decode, so this is a perf flag, not a
        correctness flag).
    push_projection
        If True (default), push column projection as a Vortex ``pack(...)`` expression.
    aggressive_pushdown
        Enable additional pushdown shapes (temporal extracts, struct field access).
        Off by default to keep the convertor surface conservative.
    initial_read_size
        Override Vortex's initial postscript read size (in bytes). Tune for high-latency
        object stores when the default round-trip is suboptimal.
    scan_concurrency
        Vortex per-file scan concurrency (passed to ``ScanBuilder::with_concurrency``).
    hive_partitioning
        Use hive-style partition extraction from path components.
    glob
        Expand glob patterns in the path.
    hidden_file_prefix
        Files with these prefixes will be skipped.
    schema
        Pre-supplied schema. Skips the file-open schema discovery at IR-build time.
    hive_schema
        Schema for the hive partition columns.
    try_parse_hive_dates
        Try to parse hive partition values as dates.
    rechunk
        Rechunk after scanning.
    cache
        Cache the result.
    storage_options
        Cloud storage authentication and configuration.
    credential_provider
        Cloud credential provider.
    include_file_paths
        Include the source file path as a column with this name.
    missing_columns
        Behavior when columns in ``schema`` are missing from the file.
    extra_columns
        Behavior when the file has columns not in ``schema``.

    Returns
    -------
    LazyFrame

    See Also
    --------
    read_vortex : Eagerly read a Vortex file into a DataFrame.
    """
    sources = get_sources(source)

    credential_provider_builder = _init_credential_provider_builder(
        credential_provider, sources, storage_options, "scan_vortex"
    )
    del credential_provider

    pylf = PyLazyFrame.new_from_vortex(
        sources=sources,
        schema=schema,
        scan_options=ScanOptions(
            row_index=(
                (row_index_name, row_index_offset)
                if row_index_name is not None
                else None
            ),
            pre_slice=(0, n_rows) if n_rows is not None else None,
            cast_options=None,
            extra_columns=extra_columns,
            missing_columns=missing_columns,
            include_file_paths=include_file_paths,
            glob=glob,
            hidden_file_prefix=(
                [hidden_file_prefix]
                if isinstance(hidden_file_prefix, str)
                else hidden_file_prefix
            ),
            hive_partitioning=hive_partitioning,
            hive_schema=hive_schema,
            try_parse_hive_dates=try_parse_hive_dates,
            rechunk=rechunk,
            cache=cache,
            storage_options=storage_options,
            credential_provider=credential_provider_builder,
            column_mapping=None,
            default_values=None,
            deletion_files=None,
            table_statistics=None,
            row_count=None,
        ),
        use_statistics=use_statistics,
        push_predicate=push_predicate,
        push_projection=push_projection,
        aggressive_pushdown=aggressive_pushdown,
        initial_read_size=initial_read_size,
        scan_concurrency=scan_concurrency,
    )
    return wrap_ldf(pylf)


def read_vortex(
    source: FileSource,
    *,
    n_rows: int | None = None,
    row_index_name: str | None = None,
    row_index_offset: int = 0,
    use_statistics: bool = True,
    schema: SchemaDict | None = None,
    rechunk: bool = False,
    storage_options: StorageOptionsDict | None = None,
    credential_provider: CredentialProviderFunction | Literal["auto"] | None = "auto",
    include_file_paths: str | None = None,
    missing_columns: Literal["insert", "raise"] = "raise",
) -> DataFrame:
    """
    Eagerly read a Vortex file (or files) into a DataFrame.

    Equivalent to ``pl.scan_vortex(source, ...).collect()``. Prefer ``scan_vortex`` if
    you want any optimizations (predicate/projection pushdown).

    See ``scan_vortex`` for the full parameter list.
    """
    return scan_vortex(
        source,
        n_rows=n_rows,
        row_index_name=row_index_name,
        row_index_offset=row_index_offset,
        use_statistics=use_statistics,
        schema=schema,
        rechunk=rechunk,
        storage_options=storage_options,
        credential_provider=credential_provider,
        include_file_paths=include_file_paths,
        missing_columns=missing_columns,
    ).collect()
