//! The selection policy: "roughly least-loaded", i.e. queries are spread over the
//! clusters of a group by how many free slots each has.

use admission::{ClusterConfig, ClusterPool, Permit, PoolConfig};
use std::sync::Arc;

fn pool(caps: &[u64]) -> Arc<ClusterPool> {
    ClusterPool::new(
        PoolConfig::default().group(
            "g",
            caps.iter()
                .enumerate()
                .map(|(i, cap)| ClusterConfig::new(format!("c{i}"), *cap))
                .collect(),
        ),
    )
}

fn running(pool: &ClusterPool, clusters: usize) -> Vec<u64> {
    (0..clusters)
        .map(|i| pool.running("g", &format!("c{i}")).unwrap())
        .collect()
}

fn acquire_n(pool: &Arc<ClusterPool>, n: usize) -> Vec<Permit> {
    (0..n).map(|_| pool.try_acquire("g").unwrap()).collect()
}

/// Small groups are exact: the free slots are levelled out, and a cluster with fewer
/// free slots than the others gets nothing until they have come down to its level.
#[test]
fn small_group_levels_out_free_slots_exactly() {
    let pool = pool(&[100, 50, 10]);
    let _held = acquire_n(&pool, 90);

    let r = running(&pool, 3);
    let free = [100 - r[0], 50 - r[1], 10 - r[2]];
    assert_eq!(
        r[2], 0,
        "the 10-slot cluster must stay untouched: running = {r:?}"
    );
    assert!(
        free[0].abs_diff(free[1]) <= 1,
        "free slots of the two big clusters should be level: running = {r:?}"
    );
}

/// Equal clusters share the load evenly, also in a bigger group.
#[test]
fn big_group_spreads_evenly() {
    let pool = pool(&[1000; 8]);
    let _held = acquire_n(&pool, 4000);

    let r = running(&pool, 8);
    let (min, max) = (r.iter().min().unwrap(), r.iter().max().unwrap());
    assert!(max - min <= 16, "load is uneven: running = {r:?}");
}

/// In a bigger group, a cluster with more capacity takes proportionally more traffic.
#[test]
fn big_group_prefers_the_cluster_with_more_free_slots() {
    let pool = pool(&[8, 16, 24, 32, 40, 48, 56, 64]);
    let _held = acquire_n(&pool, 200);

    let r = running(&pool, 8);
    assert!(
        r[7] > r[0] + 20,
        "the biggest cluster should carry much more: running = {r:?}"
    );
    assert!(r
        .iter()
        .zip([8, 16, 24, 32, 40, 48, 56, 64])
        .all(|(r, cap)| *r <= cap));
}

/// Whatever the selection does, the whole capacity of a group can be used and
/// `Busy` only appears once it is exhausted.
#[test]
fn every_slot_of_a_group_can_be_used() {
    for caps in [vec![3, 5], vec![1; 9], vec![7, 1, 4, 2, 9, 3]] {
        let total: u64 = caps.iter().sum();
        let pool = pool(&caps);
        let held = acquire_n(&pool, total as usize);
        assert!(
            pool.try_acquire("g").is_err(),
            "caps {caps:?} should be full"
        );
        drop(held);
        assert_eq!(running(&pool, caps.len()).iter().sum::<u64>(), 0);
    }
}
