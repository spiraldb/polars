# Native Vortex IO

The `vortex` Cargo feature enables native reads through the Polars streaming engine. The Python runtime requires its `vortex` feature too. The integration uses Vortex 0.93.0 and the Polars byte source for local files and cloud objects.

Schema conversion uses the Arrow C Data Interface. The handoff shares Arrow-owned buffers for compatible arrays. The reader keeps float comparisons, arithmetic, and unsupported predicates in Polars. Each file open uses a separate optional cache of serialized segment bytes.

The process default cache budget is zero. `set_vortex_cache_bytes` changes that budget for subsequent file opens. A cache never shares entries across different file opens.

The reader runs Vortex CPU tasks on the Polars streaming scheduler. General Vortex futures use the same scheduler because they can evaluate filters. IO futures use the Polars Tokio runtime. Blocking IO uses its blocking pool. The integration creates no Vortex worker pool.

A dropped scan cancels its pending task handles. A CPU operation that already starts runs to completion. Each file limits its active split and export tasks together.

The reader disables byte view compaction and requests 64-bit list offsets. These choices avoid payload copies and a second conversion of list offsets. Sliced primitive, Boolean, and byte view arrays retain their original data and validity buffers.

Map offsets still require a conversion to the Polars 64-bit representation. Nullable structs can require a new validity bitmap because Polars propagates parent nulls into children. These conversions preserve the shared value payloads.

The writer rejects unsupported logical types rather than silently changing their values. Categorical and Enum values require an explicit cast to String. Vortex 0.93.0 does not support Duration or integer values wider than 64 bits through its Arrow importer.

The writer requires a schema inside each file. `include_dtype=False` returns an error because the native reader does not accept an external file schema.

The writer also rejects Object columns and custom extension types, including these types inside nested columns.
