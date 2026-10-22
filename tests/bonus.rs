//! Bonus tests. Run with: cargo test --test bonus -- --include-ignored

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use admission::{AcquireError, ClusterConfig, ClusterPool, PoolConfig};

fn config(clusters: &[(&str, u64)]) -> PoolConfig {
    PoolConfig::default().group(
        "g",
        clusters
            .iter()
            .map(|(name, max)| ClusterConfig::new(*name, *max))
            .collect(),
    )
}

#[tokio::test]
#[ignore = "bonus"]
async fn reconcile_keeps_admissions_and_releases_made_during_the_fetch() {
    let pool = ClusterPool::new(config(&[("a", 10)]));

    // 3 real queries, plus 2 "ghost" slots whose release was lost (e.g. a gateway bug).
    let held = Arc::new(Mutex::new(
        (0..3).map(|_| pool.try_acquire("g").unwrap()).collect::<Vec<_>>(),
    ));
    for _ in 0..2 {
        std::mem::forget(pool.try_acquire("g").unwrap());
    }
    assert_eq!(pool.running("g", "a"), Some(5));

    // The engine reports 3 running. While that (slow) answer is in flight,
    // 2 new queries are admitted and 1 of the real ones finishes.
    pool.reconcile("g", "a", || {
        let (pool, held) = (Arc::clone(&pool), Arc::clone(&held));
        async move {
            let mut held = held.lock().unwrap();
            held.push(pool.try_acquire("g").unwrap());
            held.push(pool.try_acquire("g").unwrap());
            held.remove(0).release();
            3
        }
    })
    .await;

    // Truth: 3 + 2 - 1 = 4. The ghosts are gone, the concurrent updates are kept.
    assert_eq!(pool.running("g", "a"), Some(4));

    for permit in held.lock().unwrap().drain(..) {
        permit.release();
    }
    assert_eq!(pool.running("g", "a"), Some(0));
}

#[tokio::test]
#[ignore = "bonus"]
async fn reconcile_clamps_to_capacity() {
    let pool = ClusterPool::new(config(&[("a", 4)]));
    pool.reconcile("g", "a", || async { 100 }).await;
    assert_eq!(pool.running("g", "a"), Some(4));
    assert_eq!(pool.try_acquire("g").err(), Some(AcquireError::Busy));
}

#[test]
#[ignore = "bonus"]
fn reload_keeps_in_flight_queries_counted() {
    let cfg = config(&[("a", 4)]);
    let pool = ClusterPool::new(cfg.clone());
    let held: Vec<_> = (0..4).map(|_| pool.try_acquire("g").unwrap()).collect();

    pool.reload(cfg);
    assert_eq!(pool.running("g", "a"), Some(4));
    assert_eq!(pool.try_acquire("g").err(), Some(AcquireError::Busy));

    held.into_iter().for_each(|p| p.release());
    assert_eq!(pool.running("g", "a"), Some(0));
    let again: Vec<_> = (0..4).map(|_| pool.try_acquire("g").unwrap()).collect();
    assert_eq!(pool.try_acquire("g").err(), Some(AcquireError::Busy));
    drop(again);
}

#[test]
#[ignore = "bonus"]
fn reload_can_lower_capacity_below_running() {
    let pool = ClusterPool::new(config(&[("a", 4)]));
    let mut held: Vec<_> = (0..4).map(|_| pool.try_acquire("g").unwrap()).collect();

    pool.reload(config(&[("a", 2)]));
    assert_eq!(pool.try_acquire("g").err(), Some(AcquireError::Busy));

    for _ in 0..3 {
        held.pop().unwrap().release();
    }
    assert_eq!(pool.running("g", "a"), Some(1));
    let one_more = pool.try_acquire("g").unwrap();
    assert_eq!(pool.try_acquire("g").err(), Some(AcquireError::Busy));
    drop((held, one_more));
}

#[test]
#[ignore = "bonus"]
fn removed_cluster_drains_and_added_cluster_takes_traffic() {
    let pool = ClusterPool::new(config(&[("a", 1), ("b", 1)]));
    let p1 = pool.try_acquire("g").unwrap();
    let p2 = pool.try_acquire("g").unwrap();
    let (on_a, on_b) = if p1.cluster() == "a" { (p1, p2) } else { (p2, p1) };

    pool.reload(config(&[("b", 1), ("c", 1)]));
    assert_eq!(pool.running("g", "a"), None);
    assert_eq!(pool.running("g", "b"), Some(1));

    let on_c = pool.try_acquire("g").unwrap();
    assert_eq!(on_c.cluster(), "c");
    assert_eq!(pool.try_acquire("g").err(), Some(AcquireError::Busy));

    on_a.release(); // must not panic or touch b/c
    assert_eq!(pool.try_acquire("g").err(), Some(AcquireError::Busy));

    on_b.release();
    assert_eq!(pool.try_acquire("g").unwrap().cluster(), "b");
    drop(on_c);
}

#[tokio::test]
#[ignore = "bonus"]
async fn reconcile_after_reload_targets_the_shared_counter() {
    let cfg = config(&[("a", 4)]);
    let pool = ClusterPool::new(cfg.clone());
    let held: Vec<_> = (0..2).map(|_| pool.try_acquire("g").unwrap()).collect();

    pool.reload(cfg);
    pool.reconcile("g", "a", || async { 2 }).await;
    assert_eq!(pool.running("g", "a"), Some(2));

    held.into_iter().for_each(|p| p.release());
    assert_eq!(pool.running("g", "a"), Some(0));
}

#[test]
#[ignore = "bonus"]
fn capacity_holds_across_concurrent_reloads() {
    const CAP: u64 = 4;
    let cfg = config(&[("a", CAP), ("b", CAP)]);
    let pool = ClusterPool::new(cfg.clone());
    let in_flight: HashMap<&str, AtomicU64> =
        ["a", "b"].map(|n| (n, AtomicU64::new(0))).into();
    let violations = AtomicUsize::new(0);
    let done = AtomicBool::new(false);

    thread::scope(|s| {
        s.spawn(|| {
            while !done.load(Ordering::Relaxed) {
                pool.reload(cfg.clone());
                thread::yield_now();
            }
        });
        let workers: Vec<_> = (0..8)
            .map(|_| {
                s.spawn(|| {
                    for _ in 0..20_000 {
                        let Ok(permit) = pool.try_acquire("g") else {
                            continue;
                        };
                        let counter = &in_flight[permit.cluster()];
                        if counter.fetch_add(1, Ordering::SeqCst) + 1 > CAP {
                            violations.fetch_add(1, Ordering::SeqCst);
                        }
                        (0..200).for_each(|_| std::hint::spin_loop());
                        counter.fetch_sub(1, Ordering::SeqCst);
                        permit.release();
                    }
                })
            })
            .collect();
        for w in workers {
            w.join().unwrap();
        }
        done.store(true, Ordering::Relaxed);
    });

    assert_eq!(violations.load(Ordering::SeqCst), 0, "reload let a cluster exceed {CAP}");
    assert_eq!(pool.running("g", "a"), Some(0));
    assert_eq!(pool.running("g", "b"), Some(0));
}
