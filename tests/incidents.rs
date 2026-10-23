//! One reproducing test per production incident (see README, Part 1).
//! Each test fails on the starter code and passes after the matching fix.

use std::sync::{Arc, Barrier};
use std::thread;

use admission::{ClusterConfig, ClusterPool, Permit, PoolConfig};

fn pool(clusters: &[(&str, u64)]) -> Arc<ClusterPool> {
    ClusterPool::new(PoolConfig::default().group(
        "g",
        clusters
            .iter()
            .map(|(name, max)| ClusterConfig::new(*name, *max))
            .collect(),
    ))
}

/// `threads` threads start together and each calls `try_acquire` `attempts` times,
/// keeping every permit it gets. Returns all permits that were handed out.
fn stampede(pool: &Arc<ClusterPool>, threads: usize, attempts: usize) -> Vec<Permit> {
    let barrier = Barrier::new(threads);
    thread::scope(|s| {
        let workers: Vec<_> = (0..threads)
            .map(|_| {
                s.spawn(|| {
                    barrier.wait();
                    (0..attempts)
                        .filter_map(|_| pool.try_acquire("g").ok())
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        workers
            .into_iter()
            .flat_map(|w| w.join().unwrap())
            .collect()
    })
}

// ---------------------------------------------------------------------------
// INC-1: cap exceeded, then wedged.
// ---------------------------------------------------------------------------

/// Many threads race for the slots of one cluster. It must never hand out more
/// permits than `max_running`, and its counter must agree with the live permits.
#[test]
fn inc1_cap_is_never_exceeded_under_contention() {
    const MAX: u64 = 1_000;
    for round in 0..5 {
        let pool = pool(&[("a", MAX)]);
        let permits = stampede(&pool, 16, 1_000);

        assert!(
            permits.len() as u64 <= MAX,
            "round {round}: {} permits handed out for max_running = {MAX}",
            permits.len()
        );
        assert_eq!(
            pool.running("g", "a"),
            Some(permits.len() as u64),
            "round {round}: counter disagrees with the number of live permits"
        );
    }
}

/// After the spike every permit is given back. The cluster must be idle again and
/// admit new queries. (Starter: the counter lost updates, so releasing every permit
/// underflows it and the group answers `Busy` forever.)
#[test]
fn inc1_group_is_not_wedged_after_a_spike() {
    const MAX: u64 = 1_000;
    let pool = pool(&[("a", MAX)]);

    stampede(&pool, 16, 1_000)
        .into_iter()
        .for_each(|p| p.release());

    assert_eq!(pool.running("g", "a"), Some(0), "all permits were released");
    assert!(
        pool.try_acquire("g").is_ok(),
        "idle cluster must admit a new query"
    );
}
