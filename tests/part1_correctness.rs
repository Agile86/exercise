use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use admission::{AcquireError, ClusterConfig, ClusterPool, PoolConfig};

fn pool(clusters: &[(&str, u64)]) -> Arc<ClusterPool> {
    ClusterPool::new(PoolConfig::default().group(
        "g",
        clusters
            .iter()
            .map(|(name, max)| ClusterConfig::new(*name, *max))
            .collect(),
    ))
}

#[test]
fn permit_can_cross_threads() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<admission::Permit>();
    assert_send_sync::<ClusterPool>();
}

#[test]
fn unknown_group() {
    let pool = pool(&[("a", 1)]);
    assert_eq!(pool.try_acquire("nope").err(), Some(AcquireError::UnknownGroup));
}

#[test]
fn admits_up_to_capacity_then_busy() {
    let pool = pool(&[("a", 2), ("b", 1)]);
    let permits: Vec<_> = (0..3).map(|_| pool.try_acquire("g").unwrap()).collect();
    assert_eq!(pool.try_acquire("g").err(), Some(AcquireError::Busy));
    assert_eq!(pool.running("g", "a"), Some(2));
    assert_eq!(pool.running("g", "b"), Some(1));
    permits.into_iter().for_each(|p| p.release());
    assert_eq!(pool.running("g", "a"), Some(0));
    assert_eq!(pool.admissions("g", "a"), 2);
    assert_eq!(pool.admissions("g", "b"), 1);
}

#[test]
fn never_exceeds_capacity_under_contention() {
    const CAP: u64 = 4;
    let pool = pool(&[("a", CAP), ("b", CAP), ("c", CAP)]);
    let in_flight: Arc<HashMap<&str, AtomicU64>> =
        Arc::new(["a", "b", "c"].map(|n| (n, AtomicU64::new(0))).into());
    let violations = Arc::new(AtomicUsize::new(0));

    thread::scope(|s| {
        for _ in 0..16 {
            let (pool, in_flight, violations) = (&pool, &in_flight, &violations);
            s.spawn(move || {
                for _ in 0..20_000 {
                    let Ok(permit) = pool.try_acquire("g") else {
                        continue;
                    };
                    let counter = &in_flight[permit.cluster()];
                    if counter.fetch_add(1, Ordering::SeqCst) + 1 > CAP {
                        violations.fetch_add(1, Ordering::SeqCst);
                    }
                    std::hint::spin_loop();
                    counter.fetch_sub(1, Ordering::SeqCst);
                    permit.release();
                }
            });
        }
    });

    assert_eq!(violations.load(Ordering::SeqCst), 0, "a cluster ran more than {CAP} queries");
    for name in ["a", "b", "c"] {
        assert_eq!(pool.running("g", name), Some(0), "cluster {name} leaked slots");
    }
}

#[test]
fn no_spurious_busy_while_capacity_exists() {
    // 12 slots, 12 simultaneous callers: every one of them must be admitted.
    const CALLERS: usize = 12;
    let pool = pool(&[("a", 4), ("b", 4), ("c", 4)]);
    let rejected = AtomicUsize::new(0);

    for _ in 0..300 {
        let start = Barrier::new(CALLERS);
        let hold = Barrier::new(CALLERS);
        thread::scope(|s| {
            for _ in 0..CALLERS {
                s.spawn(|| {
                    start.wait();
                    let permit = pool.try_acquire("g");
                    if permit.is_err() {
                        rejected.fetch_add(1, Ordering::SeqCst);
                    }
                    hold.wait();
                    if let Ok(p) = permit {
                        p.release();
                    }
                });
            }
        });
    }
    assert_eq!(rejected.load(Ordering::SeqCst), 0);
}

#[test]
fn lowering_capacity_below_running_does_not_panic() {
    let pool = pool(&[("a", 4)]);
    let mut held: Vec<_> = (0..3).map(|_| pool.try_acquire("g").unwrap()).collect();

    pool.set_max_running("g", "a", 1);
    assert_eq!(pool.try_acquire("g").err(), Some(AcquireError::Busy));

    held.pop().unwrap().release();
    held.pop().unwrap().release();
    assert_eq!(pool.try_acquire("g").err(), Some(AcquireError::Busy));

    held.pop().unwrap().release();
    assert!(pool.try_acquire("g").is_ok());
}

#[tokio::test]
async fn aborted_query_releases_its_slot() {
    let pool = pool(&[("a", 2)]);
    let tasks: Vec<_> = (0..2)
        .map(|_| {
            let pool = Arc::clone(&pool);
            tokio::spawn(async move {
                let permit = pool.try_acquire("g").unwrap();
                tokio::time::sleep(Duration::from_secs(3600)).await; // the query
                permit.release();
            })
        })
        .collect();

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(pool.running("g", "a"), Some(2));

    // The client disconnected: the gateway drops the request future.
    for task in tasks {
        task.abort();
        let _ = task.await;
    }
    assert_eq!(pool.running("g", "a"), Some(0));
}

#[tokio::test]
async fn timed_out_query_releases_its_slot() {
    let pool = pool(&[("a", 1)]);
    let run = async {
        let permit = pool.try_acquire("g").unwrap();
        tokio::time::sleep(Duration::from_secs(3600)).await;
        permit.release();
    };
    assert!(tokio::time::timeout(Duration::from_millis(10), run).await.is_err());
    assert_eq!(pool.running("g", "a"), Some(0));
    assert!(pool.try_acquire("g").is_ok());
}
