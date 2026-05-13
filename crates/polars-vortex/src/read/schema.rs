//! Vortex `DType` ↔ Polars `Schema` translation helpers.
//!
//! Filled in by PR-2. The implementation needs to bridge upstream `arrow_schema::Schema`
//! (which Vortex's `DType::to_arrow_schema()` produces) and polars-arrow's `ArrowSchema`.
//! These types are nominally distinct — polars-arrow is Polars' internal fork — so the
//! converter walks Vortex's `DType` recursively and emits polars-arrow types directly,
//! avoiding the upstream-arrow intermediate.
