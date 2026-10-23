//! Part 3 behaviour that `waiting.rs` does not cover: other ways a slot can appear,
//! odd durations, and a waiter that gives up must not swallow a wake-up.

use std::sync::Arc;
use std::time::{Duration, Instant};

use admission::{AcquireError, ClusterConfig, ClusterPool, PoolConfig};

fn pool(max_running: u64) -> Arc<ClusterPool> {
    ClusterPool::new(PoolConfig::default().group("g", vec![ClusterConfig::new("a", max_running)]))
}

async fn wait_until(what: &str, cond: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

fn spawn_waiter(
    pool: &Arc<ClusterPool>,
    wait: Duration,
) -> tokio::task::JoinHandle<Result<admission::Permit, AcquireError>> {
    let pool = Arc::clone(pool);
    tokio::spawn(async move { pool.acquire_timeout("g", wait).await })
}

#[tokio::test]
async fn zero_wait_is_a_single_attempt_that_times_out() {
    let pool = pool(1);
    let _held = pool.try_acquire("g").unwrap();

    let start = Instant::now();
    let err = pool.acquire_timeout("g", Duration::ZERO).await.err();
    assert_eq!(err, Some(AcquireError::Timeout));
    assert!(start.elapsed() < Duration::from_millis(100));
    assert_eq!(pool.queued("g"), 0);

    // With a slot free, a zero wait still succeeds.
    drop(_held);
    assert!(pool.acquire_timeout("g", Duration::ZERO).await.is_ok());
}

#[tokio::test]
async fn huge_wait_does_not_overflow() {
    let pool = pool(1);
    let held = pool.try_acquire("g").unwrap();
    let waiter = spawn_waiter(&pool, Duration::MAX);
    wait_until("one queued waiter", || pool.queued("g") == 1).await;

    held.release();
    assert!(waiter.await.unwrap().is_ok());
}

#[tokio::test]
async fn raising_the_cap_wakes_waiters() {
    let pool = pool(1);
    let _held = pool.try_acquire("g").unwrap();
    let waiters: Vec<_> = (0..3)
        .map(|_| spawn_waiter(&pool, Duration::from_secs(5)))
        .collect();
    wait_until("three queued waiters", || pool.queued("g") == 3).await;

    let raised_at = Instant::now();
    pool.set_max_running("g", "a", 4); // 1 held + 3 waiters
    for w in waiters {
        assert!(w.await.unwrap().is_ok());
    }
    assert!(raised_at.elapsed() < Duration::from_millis(200));
    assert_eq!(pool.queued("g"), 0);
}

#[tokio::test]
async fn reload_with_more_capacity_wakes_waiters() {
    let pool = pool(1);
    let _held = pool.try_acquire("g").unwrap();
    let waiter = spawn_waiter(&pool, Duration::from_secs(5));
    wait_until("one queued waiter", || pool.queued("g") == 1).await;

    let reloaded_at = Instant::now();
    pool.reload(PoolConfig::default().group("g", vec![ClusterConfig::new("a", 2)]));
    assert!(waiter.await.unwrap().is_ok());
    assert!(reloaded_at.elapsed() < Duration::from_millis(200));
}

#[tokio::test]
async fn waiter_of_a_removed_group_gets_unknown_group() {
    let pool = pool(1);
    let _held = pool.try_acquire("g").unwrap();
    let waiter = spawn_waiter(&pool, Duration::from_secs(5));
    wait_until("one queued waiter", || pool.queued("g") == 1).await;

    let reloaded_at = Instant::now();
    pool.reload(PoolConfig::default().group("other", vec![ClusterConfig::new("x", 1)]));
    assert_eq!(
        waiter.await.unwrap().err(),
        Some(AcquireError::UnknownGroup)
    );
    assert!(reloaded_at.elapsed() < Duration::from_millis(200));
}

/// One release wakes one waiter: with N waiters and N releases, everyone gets in.
#[tokio::test]
async fn each_release_serves_one_waiter() {
    let pool = pool(3);
    let held: Vec<_> = (0..3).map(|_| pool.try_acquire("g").unwrap()).collect();
    let waiters: Vec<_> = (0..3)
        .map(|_| spawn_waiter(&pool, Duration::from_secs(5)))
        .collect();
    wait_until("three queued waiters", || pool.queued("g") == 3).await;

    for permit in held {
        permit.release();
    }
    let mut got = Vec::new();
    for w in waiters {
        got.push(w.await.unwrap().expect("every waiter gets a slot"));
    }
    assert_eq!(pool.running("g", "a"), Some(3));
    assert_eq!(pool.queued("g"), 0);
}

/// A waiter that times out or is cancelled at about the moment a slot is freed must
/// not swallow the wake-up meant for the other waiter.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_waiter_that_gives_up_does_not_swallow_the_wakeup() {
    for round in 0..100 {
        let pool = pool(1);
        let held = pool.try_acquire("g").unwrap();

        // `short` is first in the queue and gives up right about when the slot frees.
        let short = spawn_waiter(&pool, Duration::from_millis(5));
        wait_until("short waiter queued", || pool.queued("g") == 1).await;
        let long = spawn_waiter(&pool, Duration::from_secs(5));
        wait_until("both queued", || pool.queued("g") == 2).await;

        tokio::time::sleep(Duration::from_millis(5)).await;
        held.release();

        // Whoever got the slot gives it back; the long waiter must end up with it.
        if let Ok(permit) = short.await.unwrap() {
            drop(permit);
        }
        let started = Instant::now();
        let permit = long.await.unwrap();
        assert!(
            permit.is_ok() && started.elapsed() < Duration::from_secs(1),
            "round {round}: the long waiter missed its wake-up ({:?})",
            permit.err()
        );
    }
}
