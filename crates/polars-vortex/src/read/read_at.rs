//! `PolarsInstrumentedVortexReadAt`: decorator over Vortex's native `FileReadAt` /
//! `ObjectStoreReadAt` that adds Polars' `IOMetrics` and `with_concurrency_budget`. Filled in
//! by PR-2/PR-5.
