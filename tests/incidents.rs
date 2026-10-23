//! One reproducing test per production incident (see README, Part 1).
//! Each test fails on the starter code and passes after the matching fix.

use std::sync::{Arc, Barrier};
use std::thread;

use admission::{AcquireError, ClusterConfig, ClusterPool, Permit, PoolConfig};

fn pool(clusters: &[(&str, u64)]) -> Arc<ClusterPool> {
    ClusterPool::new(
        PoolConfig::default().group(
            "g",
            clusters
                .iter()
                .map(|(name, max)| ClusterConfig::new(*name, *max))
                .collect(),
        ),
    )
}

/// Running counts of `clusters`, for failure messages: `a=Some(40), b=Some(0)`.
fn state(pool: &ClusterPool, clusters: &[&str]) -> String {
    clusters
        .iter()
        .map(|c| format!("{c}={:?}", pool.running("g", c)))
        .collect::<Vec<_>>()
        .join(", ")
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

/// The counter can be moved under live permits by `reconcile` (the engine reports 0
/// while one query is still in flight). Releasing that permit must not wrap the counter
/// below zero: `u64::MAX` would make the cluster look full until a restart.
#[tokio::test]
async fn inc1_release_never_underflows_the_counter() {
    let pool = pool(&[("a", 10)]);
    let permit = pool.try_acquire("g").unwrap();

    pool.reconcile("g", "a", || async { 0 }).await;
    assert_eq!(pool.running("g", "a"), Some(0), "setup: reconcile applied");

    permit.release();

    assert_eq!(
        pool.running("g", "a"),
        Some(0),
        "release with a zero counter must stay at zero"
    );
    assert!(pool.try_acquire("g").is_ok(), "idle cluster must admit a query");
}

// ---------------------------------------------------------------------------
// INC-2: capacity cut below the current load.
// ---------------------------------------------------------------------------

/// `a` is full (40 running), then an operator cuts it to 10. `a` is now over its
/// cap, but `b` is idle: new queries must go to `b`. (Starter: `max - running`
/// underflows; debug builds panic, release builds see `a` as hugely free, pick it,
/// fail to increment and answer `Busy` for the whole group.)
#[test]
fn inc2_cut_below_running_sends_traffic_to_other_clusters() {
    let pool = pool(&[("a", 40), ("b", 40)]);

    // Fill `a` only: take `b` out of rotation while we do it.
    pool.set_max_running("g", "b", 0);
    let _held: Vec<_> = (0..40).map(|_| pool.try_acquire("g").unwrap()).collect();
    pool.set_max_running("g", "b", 40);
    assert_eq!(
        pool.running("g", "a"),
        Some(40),
        "setup: {}",
        state(&pool, &["a", "b"])
    );

    pool.set_max_running("g", "a", 10);

    for i in 0..5 {
        let permit = pool.try_acquire("g").unwrap_or_else(|e| {
            panic!(
                "query {i}: {e:?} although `b` is idle (cap 40) [{}]",
                state(&pool, &["a", "b"])
            )
        });
        assert_eq!(
            permit.cluster(),
            "b",
            "query {i} was admitted to the over-cap cluster [{}]",
            state(&pool, &["a", "b"])
        );
    }
    assert_eq!(
        pool.running("g", "a"),
        Some(40),
        "`a` must take nothing new while it is over its cap"
    );
}

/// A cluster over its new cap admits nothing until it has drained *below* the cap,
/// and never panics while it is over it.
#[test]
fn inc2_over_cap_cluster_admits_again_only_after_draining_below_new_cap() {
    let pool = pool(&[("a", 40)]);
    let mut held: Vec<_> = (0..40).map(|_| pool.try_acquire("g").unwrap()).collect();

    pool.set_max_running("g", "a", 10);
    assert_eq!(
        pool.try_acquire("g").err(),
        Some(admission::AcquireError::Busy),
        "40 running, cap 10 [{}]",
        state(&pool, &["a"])
    );

    held.drain(..30).for_each(|p| p.release());
    assert_eq!(
        pool.try_acquire("g").err(),
        Some(admission::AcquireError::Busy),
        "running == cap (10 of 10) is still full [{}]",
        state(&pool, &["a"])
    );

    held.drain(..1).for_each(|p| p.release());
    assert!(
        pool.try_acquire("g").is_ok(),
        "9 running, cap 10: one slot is free [{}]",
        state(&pool, &["a"])
    );
}

// ---------------------------------------------------------------------------
// INC-3: Busy while the group still has free slots.
// ---------------------------------------------------------------------------

/// `threads` threads start together and each makes exactly one `try_acquire`.
fn one_shot(pool: &Arc<ClusterPool>, threads: usize) -> Vec<Result<Permit, AcquireError>> {
    let barrier = Barrier::new(threads);
    thread::scope(|s| {
        let workers: Vec<_> = (0..threads)
            .map(|_| {
                s.spawn(|| {
                    barrier.wait();
                    pool.try_acquire("g")
                })
            })
            .collect();
        workers.into_iter().map(|w| w.join().unwrap()).collect()
    })
}

/// 16 callers, 16 slots in total (`a`: 8, `b`: 8), nobody releases. While a caller is
/// still trying, at most 15 slots are taken, so the group is never full and no caller
/// may see `Busy`. (Starter: every caller picks one cluster from a stale snapshot; if
/// that cluster fills up meanwhile it answers `Busy` without trying the other one.)
#[test]
fn inc3_no_busy_while_the_group_has_free_slots() {
    for round in 0..3_000 {
        let pool = pool(&[("a", 8), ("b", 8)]);
        let results = one_shot(&pool, 16);

        let busy = results.iter().filter(|r| r.is_err()).count();
        assert_eq!(
            busy,
            0,
            "round {round}: {busy} of 16 callers got Busy, but the group has 16 slots for 16 \
             callers [{}]",
            state(&pool, &["a", "b"])
        );
    }
}

// ---------------------------------------------------------------------------
// INC-4: slots never recovered after clients disconnected.
// ---------------------------------------------------------------------------

/// A permit that is dropped without an explicit `release()` must give its slot back.
#[test]
fn inc4_dropped_permit_gives_its_slot_back() {
    let pool = pool(&[("a", 1)]);
    let permit = pool.try_acquire("g").unwrap();
    assert_eq!(pool.running("g", "a"), Some(1));

    drop(permit);

    assert_eq!(
        pool.running("g", "a"),
        Some(0),
        "slot leaked by a dropped permit"
    );
    assert!(
        pool.try_acquire("g").is_ok(),
        "the only slot must be usable again"
    );
}

/// The real incident: clients time out and disconnect, so the tasks serving them are
/// cancelled while they hold a permit. Cancelling drops the future and its permit.
#[tokio::test]
async fn inc4_cancelled_request_gives_its_slot_back() {
    let pool = pool(&[("a", 2)]);

    let handlers: Vec<_> = (0..2)
        .map(|_| {
            let pool = Arc::clone(&pool);
            tokio::spawn(async move {
                let _permit = pool.try_acquire("g").unwrap();
                std::future::pending::<()>().await; // waiting for a Trino that never answers
            })
        })
        .collect();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while pool.running("g", "a") != Some(2) {
        assert!(
            std::time::Instant::now() < deadline,
            "setup: handlers never took their slots [{}]",
            state(&pool, &["a"])
        );
        tokio::task::yield_now().await;
    }

    // Clients disconnect.
    handlers.iter().for_each(|h| h.abort());
    for h in handlers {
        let _ = h.await;
    }

    assert_eq!(
        pool.running("g", "a"),
        Some(0),
        "2 cancelled requests leaked their slots [{}]",
        state(&pool, &["a"])
    );
    assert!(pool.try_acquire("g").is_ok());
}

/// A handler that panics while holding a permit must not leak the slot either.
#[test]
fn inc4_panicking_handler_gives_its_slot_back() {
    let pool = pool(&[("a", 1)]);
    let for_thread = Arc::clone(&pool);

    let result = thread::spawn(move || {
        let _permit = for_thread.try_acquire("g").unwrap();
        panic!("query handler crashed");
    })
    .join();

    assert!(
        result.is_err(),
        "setup: the handler thread should have panicked"
    );
    assert_eq!(
        pool.running("g", "a"),
        Some(0),
        "slot leaked by a panicking handler"
    );
}

/// Guard for the fix: `release()` followed by the permit being dropped must count
/// once, not twice. (Passes on the starter; fails if `Drop` is added naively.)
#[test]
fn inc4_explicit_release_is_not_counted_twice() {
    let pool = pool(&[("a", 2)]);
    let first = pool.try_acquire("g").unwrap();
    let _second = pool.try_acquire("g").unwrap();

    first.release();

    assert_eq!(
        pool.running("g", "a"),
        Some(1),
        "one permit is still held [{}]",
        state(&pool, &["a"])
    );
}
