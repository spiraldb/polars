//! Vortex execution on the runtimes that Polars owns.
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use futures::future::BoxFuture;
use polars_async::executor::{self, CancelHandle, TaskPriority};
use polars_core::runtime::ASYNC;
use vortex::VortexSessionDefault;
use vortex::io::runtime::{AbortHandle, AbortHandleRef, Executor, Handle};
use vortex::io::session::RuntimeSessionExt;
use vortex::layout::segments::{MokaSegmentCache, NoOpSegmentCache, SegmentCache, SegmentEviction};
use vortex::session::VortexSession;

struct PolarsExecutor;
impl Executor for PolarsExecutor {
    fn spawn(&self, future: BoxFuture<'static, ()>) -> AbortHandleRef {
        // General Vortex futures also perform inline filter and decode work.
        let task = executor::spawn(TaskPriority::Low, None, future);
        Box::new(PolarsAbort(task.cancel_handle()))
    }
    fn spawn_io(&self, future: BoxFuture<'static, ()>) -> AbortHandleRef {
        Box::new(ASYNC.spawn(future).abort_handle())
    }
    fn spawn_cpu(&self, task: Box<dyn FnOnce() + Send + 'static>) -> AbortHandleRef {
        let task = executor::spawn(TaskPriority::Low, None, async move { task() });
        Box::new(PolarsAbort(task.cancel_handle()))
    }
    fn spawn_blocking_io(&self, task: Box<dyn FnOnce() + Send + 'static>) -> AbortHandleRef {
        Box::new(ASYNC.spawn_blocking(task).abort_handle())
    }
}
struct PolarsAbort(CancelHandle);
impl AbortHandle for PolarsAbort {
    fn abort(self: Box<Self>) {
        self.0.cancel();
    }
}
static EXECUTOR: LazyLock<Arc<dyn Executor>> = LazyLock::new(|| Arc::new(PolarsExecutor));
static SESSION: LazyLock<VortexSession> =
    LazyLock::new(|| VortexSession::default().with_handle(handle()));
pub fn session() -> &'static VortexSession {
    &SESSION
}
pub fn handle() -> Handle {
    Handle::new(Arc::downgrade(&EXECUTOR))
}
static DEFAULT_CACHE_BYTES: LazyLock<AtomicU64> = LazyLock::new(|| {
    AtomicU64::new(
        std::env::var("POLARS_VORTEX_CACHE_BYTES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0),
    )
});
/// Set the default budget for each subsequent file open.
pub fn set_global_cache_bytes(bytes: u64) {
    DEFAULT_CACHE_BYTES.store(bytes, Ordering::Relaxed);
}
pub fn segment_cache() -> Arc<dyn SegmentCache> {
    build_cache(DEFAULT_CACHE_BYTES.load(Ordering::Relaxed))
}
pub(crate) fn build_cache(bytes: u64) -> Arc<dyn SegmentCache> {
    if bytes == 0 {
        Arc::new(NoOpSegmentCache)
    } else {
        // Each open owns this cache, so segment numbers cannot alias other files.
        Arc::new(MokaSegmentCache::new(bytes, SegmentEviction::Lru).for_file("open"))
    }
}

#[cfg(test)]
mod tests {
    use std::panic::AssertUnwindSafe;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Condvar, Mutex};
    use std::time::Duration;

    use futures::FutureExt;
    use polars_async::executor::{ALLOW_RAYON_THREADS, is_scheduling_polars_executor_thread};

    use super::*;

    fn cpu_affinity() -> (bool, bool) {
        (
            is_scheduling_polars_executor_thread(),
            ALLOW_RAYON_THREADS.get(),
        )
    }

    #[test]
    fn runtime_routes_cpu_and_io_to_their_owners() {
        ASYNC.block_on(async {
            assert_eq!(
                handle().spawn(async { cpu_affinity() }).await,
                (true, false)
            );
            assert_eq!(handle().spawn_cpu(cpu_affinity).await, (true, false));
            let io = handle()
                .spawn_io(async {
                    assert!(tokio::runtime::Handle::try_current().is_ok());
                    assert!(!is_scheduling_polars_executor_thread());
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    7
                })
                .await;
            assert_eq!(io, 7);
            handle()
                .spawn_blocking(|| {
                    assert!(!is_scheduling_polars_executor_thread());
                    assert!(tokio::runtime::Handle::try_current().is_ok());
                })
                .await;
        });
    }

    #[test]
    fn nested_cpu_wait_yields_the_worker() {
        ASYNC.block_on(async {
            let runtime = handle();
            let result = tokio::time::timeout(
                Duration::from_secs(10),
                handle().spawn(async move {
                    let first = runtime.spawn_cpu(|| 20).await;
                    let second = runtime.spawn_cpu(|| 22).await;
                    first + second
                }),
            )
            .await
            .unwrap();
            assert_eq!(result, 42);
        });
    }

    #[test]
    fn task_panics_reach_the_caller_and_workers_survive() {
        ASYNC.block_on(async {
            assert!(
                AssertUnwindSafe(handle().spawn(async { panic!("general task panic") }))
                    .catch_unwind()
                    .await
                    .is_err()
            );
            assert!(
                AssertUnwindSafe(handle().spawn_cpu(|| panic!("CPU task panic")))
                    .catch_unwind()
                    .await
                    .is_err()
            );
            assert_eq!(handle().spawn_cpu(|| 42).await, 42);
        });
    }

    #[test]
    fn dropping_general_and_io_tasks_cancels_their_futures() {
        ASYNC.block_on(async {
            for io in [false, true] {
                let (started_tx, started_rx) = tokio::sync::oneshot::channel();
                let (mut release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
                let future = async move {
                    let _ = started_tx.send(());
                    let _ = release_rx.await;
                };
                let task = if io {
                    handle().spawn_io(future)
                } else {
                    handle().spawn(future)
                };
                started_rx.await.unwrap();
                drop(task);
                tokio::time::timeout(Duration::from_secs(10), release_tx.closed())
                    .await
                    .unwrap();
            }
        });
    }

    #[test]
    fn dropping_queued_cpu_task_prevents_execution() {
        let workers = polars_config::config().max_threads();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let mut blockers = Vec::with_capacity(workers);
        struct ReleaseGate(Arc<(Mutex<bool>, Condvar)>);
        impl Drop for ReleaseGate {
            fn drop(&mut self) {
                *self.0.0.lock().unwrap() = true;
                self.0.1.notify_all();
            }
        }
        // Release workers before their task handles drop, including on test failure.
        let release = ReleaseGate(gate.clone());
        for _ in 0..workers {
            let gate = gate.clone();
            let started_tx = started_tx.clone();
            blockers.push(handle().spawn_cpu(move || {
                started_tx.send(()).unwrap();
                let (lock, ready) = &*gate;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = ready.wait(released).unwrap();
                }
            }));
        }
        for _ in 0..workers {
            started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        }
        let ran = Arc::new(AtomicBool::new(false));
        let task_ran = ran.clone();
        let queued = handle().spawn_cpu(move || task_ran.store(true, Ordering::Release));
        drop(queued);
        drop(release);
        ASYNC.block_on(async {
            for task in blockers {
                task.await;
            }
        });
        assert!(!ran.load(Ordering::Acquire));
    }
}
