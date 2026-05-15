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
    from polars import _plr as plr
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
    push_predicate: bool = True,
    initial_read_size: int | None = None,
    scan_concurrency: int | None = None,
    cache_mode: Literal["global", "off"] | int | None = None,
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
    push_predicate
        If True (default), translate the pushable parts of any filter into Vortex
        expressions and hand them to ``ScanBuilder::with_filter``. Disable if a buggy
        convertor causes incorrect rows to be skipped (the multi-scan layer always
        re-applies the full predicate post-decode, so this is a perf flag, not a
        correctness flag).
    initial_read_size
        Override Vortex's initial postscript read size (in bytes). Tune for high-latency
        object stores when the default round-trip is suboptimal.
    scan_concurrency
        Vortex per-file scan concurrency (passed to ``ScanBuilder::with_concurrency``).
        ``None`` lets Vortex pick based on the layout's natural splits.
    cache_mode
        Controls how this scan uses Vortex's segment cache (decompressed-segment
        cache across queries on the same file — one of Vortex's biggest perf wins
        over Parquet).

        - ``None`` (default) or ``"global"``: use the process-global cache (sized
          via :func:`set_vortex_cache_bytes` or the ``POLARS_VORTEX_CACHE_BYTES``
          environment variable).
        - ``"off"``: disable caching for this scan (no segments retained across
          calls). Useful for one-shot benchmark runs or when memory pressure is
          a concern.
        - ``int`` (positive bytes): allocate a fresh per-scan cache with this
          byte budget. Independent of the global cache and dropped when the
          scan completes.
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

    cache_mode_kind, cache_dedicated_bytes = _resolve_cache_mode(cache_mode)

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
        push_predicate=push_predicate,
        initial_read_size=initial_read_size,
        scan_concurrency=scan_concurrency,
        cache_mode_kind=cache_mode_kind,
        cache_dedicated_bytes=cache_dedicated_bytes,
    )
    return wrap_ldf(pylf)


def _resolve_cache_mode(
    cache_mode: Literal["global", "off"] | int | None,
) -> tuple[str, int | None]:
    """Map the user-facing ``cache_mode`` to the (kind, dedicated_bytes) pyo3 pair."""
    if cache_mode is None or cache_mode == "global":
        return "global", None
    if cache_mode == "off":
        return "off", None
    # `bool` is a subclass of `int` in Python; reject True/False explicitly so
    # callers don't accidentally get a 1- or 0-byte dedicated cache.
    if isinstance(cache_mode, bool):
        msg = (
            f"cache_mode must be None, 'global', 'off', or a positive int "
            f"(got bool {cache_mode!r})"
        )
        raise TypeError(msg)
    if not isinstance(cache_mode, int):
        msg = (
            f"cache_mode must be None, 'global', 'off', or a positive int "
            f"(got {cache_mode!r})"
        )
        raise TypeError(msg)
    if cache_mode <= 0:
        msg = (
            "cache_mode int must be a positive byte count; "
            "pass 'off' to disable caching"
        )
        raise ValueError(msg)
    return "dedicated", cache_mode


def set_vortex_cache_bytes(byte_budget: int) -> None:
    """
    Set the process-global Vortex segment cache size, in bytes. Pass ``0`` to disable.

    Vortex's segment cache stores decompressed columnar segments across queries on the
    same file — one of its biggest perf wins over Parquet. Default is 512 MiB
    (also tunable via the ``POLARS_VORTEX_CACHE_BYTES`` environment variable).

    Parameters
    ----------
    byte_budget
        Maximum number of bytes the cache may use. ``0`` disables caching.

    Examples
    --------
    >>> import polars as pl
    >>> pl.set_vortex_cache_bytes(2 * 1024**3)  # 2 GiB  # doctest: +SKIP

    See Also
    --------
    scan_vortex
    """
    plr.set_vortex_cache_bytes(int(byte_budget))


def read_vortex(
    source: FileSource,
    *,
    n_rows: int | None = None,
    row_index_name: str | None = None,
    row_index_offset: int = 0,
    cache_mode: Literal["global", "off"] | int | None = None,
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
        cache_mode=cache_mode,
        schema=schema,
        rechunk=rechunk,
        storage_options=storage_options,
        credential_provider=credential_provider,
        include_file_paths=include_file_paths,
        missing_columns=missing_columns,
    ).collect()
