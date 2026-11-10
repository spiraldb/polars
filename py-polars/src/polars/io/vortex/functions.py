"""Read Vortex files with native Polars IO."""

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

    The query optimizer can send projections and supported predicates to the reader.
    Polars evaluates the remaining predicates after the scan.

    Parameters
    ----------
    source
        Path(s) to a file or directory. When the path has a cloud scheme
        (``s3://``, ``gs://``, ``az://``), authenticate via ``storage_options``.
    n_rows
        Read at most ``n_rows`` rows from the source.
    row_index_name
        If set, insert a row index column with this name.
    row_index_offset
        Offset to start the row index column (only used if ``row_index_name`` is set).
    push_predicate
        Send supported filter conditions to Vortex. Polars also evaluates the
        original predicate. A pushed condition must preserve every matching row.
        Set this parameter to False to compare results without predicate pushdown.
    initial_read_size
        Override Vortex's initial postscript read size (in bytes). Tune for high-latency
        object stores when the default round-trip is suboptimal.
    scan_concurrency
        Limit the active scan splits across this file. ``None`` selects the
        Polars pipeline count. This limit applies to the whole file.
    cache_mode
        Select the cache for serialized segment bytes. This cache does not store
        decoded arrays. Cached bytes can reduce reads but do not eliminate decoding.

        - ``None`` or ``"global"`` selects the process default byte budget.
          Each file open receives a separate cache. The initial budget is zero.
        - ``"off"`` disables the segment cache.
        - A positive integer sets a separate cache budget for each file open.

        The cache does not control the operating system cache.
    hive_partitioning
        Use hive-style partition extraction from path components.
    glob
        Expand glob patterns in the path.
    hidden_file_prefix
        The reader skips files with these prefixes.
    schema
        A supplied schema avoids file schema discovery during plan construction.
    hive_schema
        Schema for the hive partition columns.
    try_parse_hive_dates
        Try to parse hive partition values as dates.
    rechunk
        Combine each result column into one contiguous array.
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
    if n_rows is not None and n_rows < 0:
        msg = "n_rows must be nonnegative"
        raise ValueError(msg)
    for name, value in (
        ("initial_read_size", initial_read_size),
        ("scan_concurrency", scan_concurrency),
    ):
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, int) or value <= 0
        ):
            msg = f"{name} must be a positive integer"
            raise ValueError(msg)
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
) -> tuple[Literal["global", "off", "dedicated"], int | None]:
    """Map ``cache_mode`` to the native cache policy."""
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
            "cache_mode int must be a positive byte count. "
            "pass 'off' to disable caching"
        )
        raise ValueError(msg)
    return "dedicated", cache_mode


def set_vortex_cache_bytes(byte_budget: int) -> None:
    """
    Set the default segment cache budget for subsequent file opens.

    Each file open receives a separate cache. The initial budget is zero.
    Pass ``0`` to disable the cache for subsequent opens.

    The segment cache stores serialized file bytes. It does not store decoded
    arrays. The budget applies to each file open.
    It excludes the operating system cache.

    Parameters
    ----------
    byte_budget
        Maximum number of bytes that the cache can use. ``0`` disables caching.

    Examples
    --------
    >>> import polars as pl
    >>> pl.set_vortex_cache_bytes(2 * 1024**3)  # 2 GiB  # doctest: +SKIP

    See Also
    --------
    scan_vortex
    """
    if isinstance(byte_budget, bool) or not isinstance(byte_budget, int):
        msg = "byte_budget must be an integer"
        raise TypeError(msg)
    if byte_budget < 0:
        msg = "byte_budget must be nonnegative"
        raise ValueError(msg)
    plr.set_vortex_cache_bytes(byte_budget)


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

    This method evaluates ``pl.scan_vortex(source, ...).collect()``.

    If a query needs filters or selected columns, use ``scan_vortex``.

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
