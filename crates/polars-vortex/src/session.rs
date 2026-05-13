//! Process-global [`VortexSession`] backed by Polars' global Tokio runtime.
//!
//! The session is constructed lazily on first access. All Vortex async I/O — file opens, segment
//! fetches, partition decoding, and writes — runs on the [`polars_core::runtime::ASYNC`] Tokio
//! runtime by way of a [`vortex::io::runtime::Handle`] over Polars' Tokio handle. We deliberately
//! avoid spawning a second runtime (would double thread count for no win) and avoid the
//! single-threaded `CurrentThreadRuntime` (would bottleneck multi-pipeline streaming).
//!
//! The segment cache is also process-global. Default size is 512 MiB, tunable via the
//! `POLARS_VORTEX_CACHE_BYTES` env var (set to `0` to disable).

use std::sync::{Arc, LazyLock};

use parking_lot::RwLock;
use polars_core::runtime::ASYNC;
use vortex::VortexSessionDefault;
use vortex::io::runtime::{Executor, Handle};
use vortex::io::session::RuntimeSessionExt;
use vortex::layout::segments::SegmentCache;
use vortex::session::VortexSession;

/// Returns the process-global [`VortexSession`].
///
/// Constructed on first call and shared by every Vortex scan and write thereafter. The session is
/// thread-safe and cheaply cloneable.
pub fn session() -> &'static VortexSession {
    &SESSION
}

/// Returns a [`Handle`] to the Vortex runtime (which is itself a thin adapter over Polars' global
/// Tokio runtime). Useful when calling Vortex APIs that take an explicit handle (e.g.
/// `FileReadAt::open_with_handle`).
pub fn handle() -> Handle {
    Handle::new(Arc::downgrade(&EXECUTOR))
}

// We hold the executor in a `LazyLock` so that the `Weak<dyn Executor>` inside each `Handle` we
// hand out always resolves. The executor itself is just Polars' Tokio handle, which implements
// `vortex::io::runtime::Executor`.
static EXECUTOR: LazyLock<Arc<dyn Executor>> =
    LazyLock::new(|| Arc::new(ASYNC.handle()) as Arc<dyn Executor>);

static SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    // Default session: registers DType, Array, Layout, ScalarFn, ArrayKernels, AggregateFn,
    // RuntimeSession, plus default file encodings (gated on the `files` feature in vortex).
    // Then we override the RuntimeSession handle to point at Polars' Tokio runtime explicitly,
    // since the default uses `Handle::find()` which returns `None` outside a Tokio context.
    VortexSession::default().with_handle(handle())
});

/// Returns the process-global Vortex segment cache.
///
/// Used to share decompressed segments across queries on the same file — one of Vortex's biggest
/// perf wins over Parquet.
pub fn segment_cache() -> Arc<dyn SegmentCache> {
    GLOBAL_SEGMENT_CACHE.read().clone()
}

/// Replaces the global segment cache with a fresh [`vortex::layout::segments::MokaSegmentCache`]
/// of the given byte budget, or with a [`vortex::layout::segments::NoOpSegmentCache`] if
/// `bytes == 0`.
pub fn set_global_cache_bytes(bytes: u64) {
    *GLOBAL_SEGMENT_CACHE.write() = build_cache(bytes);
}

static GLOBAL_SEGMENT_CACHE: LazyLock<RwLock<Arc<dyn SegmentCache>>> = LazyLock::new(|| {
    let bytes = std::env::var("POLARS_VORTEX_CACHE_BYTES")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(512 * 1024 * 1024);
    RwLock::new(build_cache(bytes))
});

fn build_cache(bytes: u64) -> Arc<dyn SegmentCache> {
    use vortex::layout::segments::{MokaSegmentCache, NoOpSegmentCache};
    if bytes == 0 {
        Arc::new(NoOpSegmentCache)
    } else {
        Arc::new(MokaSegmentCache::new(bytes))
    }
}
