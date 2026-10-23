# Notes

## Part 1: Incidents

| Incident | Root cause | Reproducing test | Fix |
|----------|------------|------------------|-----|
| INC-1 | `try_increment` was `load`, compare, `store(current + 1)`: not atomic. Two threads read the same `running`, both pass the cap check, both store the same value, so one increment is lost. More permits than `max_running` are handed out and `running` is lower than the real number of holders. When those holders release, `decrement` (`fetch_sub`, unguarded) takes the counter below 0, it wraps to ~`u64::MAX`, the cluster looks permanently full and every query gets `Busy`, until a restart resets the counter. | `inc1_cap_is_never_exceeded_under_contention` (16 threads race for 1000 slots: starter hands out 1021) and `inc1_group_is_not_wedged_after_a_spike` (after releasing everything, starter has `running = 2^64 - 19`). Both fail 11/11 runs on the starter. | Check and increment are one CAS loop (`fetch_update`). `decrement` is also a CAS that stops at 0 (`checked_sub`), so an accounting error elsewhere (`reconcile`, `reload`) can no longer wedge the cluster. |
| INC-2 | `available()` was `max_running - running` on `u64`. After `set_max_running` lowers the cap below the current load (40 -> 10 with 40 running) the subtraction underflows. Debug build: `attempt to subtract with overflow` panic (staging). Release build: it wraps to ~`2^64 - 30`, so the over-cap cluster looks like the emptiest one, `pick` (max by `available`) always chooses it, `try_increment` correctly refuses (`40 >= 10`) and `try_acquire` returns `Busy` although the other clusters are idle. It lasts until the cluster drains below the new cap. | `inc2_cut_below_running_sends_traffic_to_other_clusters` (debug: panic; release: `Busy although b is idle [a=40, b=0]`) and `inc2_over_cap_cluster_admits_again_only_after_draining_below_new_cap` (debug panic only: with one cluster `Busy` is the right answer even when it is reached by accident). Run both with `cargo test` and `cargo test --release`. | `available()` uses `saturating_sub`, so an over-cap cluster has 0 free slots and the existing `available > 0` filter drops it from the candidates. |
| INC-3 | `try_acquire` took a snapshot of free slots, picked ONE cluster from it and, when `try_increment` on that cluster failed, returned `Busy` at once. The snapshot is stale by the time of the increment: under load several callers choose the same "best" cluster, the first ones take its last slots, the rest get `Busy` while the other clusters in the group still have room (dashboard: free slots). A failed increment only says that one cluster is full, not the group. | `inc3_no_busy_while_the_group_has_free_slots`: 16 threads, one `try_acquire` each, group `a(8)+b(8)` and nobody releases, so the group can never be full while a caller is still trying and `Busy` is always wrong. Fails 40/40 runs on the starter (debug and release, 3000 rounds; failure shows e.g. `[a=Some(7), b=Some(8)]`). With 200 rounds release only failed 4/10, hence 3000. | Candidates are sorted by free slots, best first, and tried in order; `Busy` is returned only after every candidate refused. `pick` is gone (the sort replaces it). The selection and its cost are reworked in Part 2: ties still all go to the first cluster, so callers collide on its CAS before falling through. |
| INC-4 | `Permit` returned its slot only through an explicit `release()`; it had no `Drop`. When a client times out and disconnects, the task serving it is cancelled, tokio drops its future and the `Permit` inside it, and the slot is never given back (same for a panicking handler). Each disconnected client during the Trino outage leaked one slot for good, so the group ran at reduced capacity until a restart reset the counters. | `inc4_dropped_permit_gives_its_slot_back`, `inc4_cancelled_request_gives_its_slot_back` (two tasks hold a slot, are `abort()`ed, starter shows `a=Some(2)`), `inc4_panicking_handler_gives_its_slot_back`. Plus the guard `inc4_explicit_release_is_not_counted_twice` (passes on the starter, protects against double release once `Drop` exists). | `impl Drop for Permit` returns the slot; `release(self)` is now an empty method that just consumes the permit, so `Drop` runs exactly once on every path (normal end, `drop`, cancellation, panic unwind). Known leftover, not part of this incident: `Permit` still finds its cluster again by `(group, name)` on drop, which is wrong after a `reload`; see Bonus (INC-6). |

## Part 2: Performance

Machine (CPU and core count): AMD Ryzen 9 5900HS, 8 cores / 16 threads, Linux, rustc 1.98.1, `cargo bench` (criterion, 3 s measurement).

| Bench | Before | After |
|-------|--------|-------|
| acquire_release/1  | median 2.61 Melem/s (range 2.58-2.70), ~383 ns/iter | median 26.5 Melem/s (26.4-26.6) |
| acquire_release/4  | median 4.39 Melem/s (range 4.21-4.62), ~912 ns/iter | median 33.1 Melem/s (32.8-34.3) |
| acquire_release/16 | median 6.81 Melem/s (range 4.02-7.07), ~2.35 µs/iter | median 44.7 Melem/s (44.0-45.8): 6.6x the starter median, 11x the 4.02 outlier |

Baseline = 4 runs of `cargo bench` on the unmodified starter (centre estimate of each run, Melem/s):
/1: 2.62, 2.58, 2.70, 2.60; /4: 4.21, 4.62, 4.46, 4.31; /16: 4.02, 6.73, 6.88, 7.07.
The first /16 run (4.02) is an outlier (laptop CPU, boost clocks / SMT noise); the other three
agree within ~5%. I use the median (6.81) as the baseline, which is the stricter choice.

Target: >= 3x on acquire_release/16, i.e. >= ~20.4 Melem/s against the median baseline
(>= ~12 Melem/s if the 4.02 outlier were the baseline). Going from 4 to 16 threads gives
only ~1.5x more throughput on 4x the threads: the hot path is serialized on shared state.

The INC-1..4 fixes made the code *slower* before any optimisation: /16 dropped from 6.81 to
2.05 Melem/s. The starter was fast partly because of its bugs (a plain `store` instead of a
CAS never retries and never contends; the lost updates were "free"). So the real distance to
the target was ~10x, not 3x. Every row below is the median-ish of 3 `cargo bench` runs
(Melem/s) after that change was added on top of the previous rows:

| Step | /1 | /4 | /16 |
|------|----|----|-----|
| starter (median of 4 runs) | 2.61 | 4.39 | 6.81 |
| + INC-1..4 fixes (CAS loops, `Drop`, retry other clusters) | 2.77 | 2.37 | 2.05 |
| 1. no allocation / `Vec<Arc>` clone, `Permit` holds `Arc<ClusterState>` | 7.4 | 4.2 | 2.2 |
| 2. `admissions` as a per-cluster `AtomicU64` | 13.7 | 9.5 | 12.7 |
| 3. scan without `Vec`, `sort` and name lookups | 25.6 | 12.4 | 12.9 |
| 4. group table behind 32 sharded `RwLock`s | 24.3 | 17.3 | 16.7 |
| 5. padding `ClusterState` to its own cache lines (**reverted**) | 24.4 | 17.7 | 16.3 |
| 6. power of two choices for groups with more than 4 clusters | 26.5 | 33.1 | 44.7 |

Changes, and why each one helps (or didn't):

1. **No allocation and no clone on the hot path.** The starter did `group.to_string()` for the
   lookup, cloned the `Vec<Arc<ClusterState>>` (one allocation plus 8 atomic refcount
   increments on shared counters), built a `Vec<Candidate>` with 8 `String` clones, then
   searched the winner again by name, and did all of it a second time in `release`. Now the
   lookup borrows `&str` under the read guard and `Permit` keeps the `Arc<ClusterState>` it was
   given, so `Drop` is a single CAS. Helps single-thread a lot (2.8 -> 7.4, fewer `malloc`/`free`
   and string compares). Does almost nothing at /16: the threads are not CPU-bound, they wait
   on something shared.
2. **`admissions` is an `AtomicU64` in `ClusterState`.** It was one global
   `Mutex<HashMap<String, u64>>` plus a `format!` per query, taken by every thread of every
   group. That mutex was the main serialiser: /16 went 2.2 -> 12.7 (5.8x) from this alone. The
   counter now sits next to `running`, on a cache line the thread already owns after its CAS.
   Side effect I had to handle: the metric used to survive `reload` (separate map) and would
   have reset with the state, so `reload` now keeps existing `ClusterState`s by (group, name)
   and only updates `max_running` (see Bonus; this is also most of the INC-6 fix).
3. **Selection without `Vec` / `sort`.** One pass over the clusters to find the one with the
   most free slots, then one CAS; if the CAS loses the race it scans again with fresh values
   (a full cluster has 0 free and drops out, so `Busy` still means "nobody has room"). Doubles
   /1 (7.4 -> 25.6) because the sort and 9 allocations are gone. /16 did not move: still stuck.
4. **Sharded `RwLock` for the group table.** A read lock is not free: `read()` and dropping the
   guard are atomic writes to the lock word, so every query of every thread bounced one cache
   line between cores. There are now 32 copies of the table (an `Arc<HashMap>`, the
   `ClusterState`s are shared), each behind its own lock on its own 128-byte-aligned slot. A
   reader locks the shard of its thread (thread-local index, no shared atomic); `reload` takes
   all 32 write locks in order. `try_acquire` never takes an exclusive lock. /16 12.9 -> 16.7,
   /4 12.4 -> 17.3. std only, no `unsafe`. I considered `arc-swap` (fewer moving parts for
   reload) and did not add a dependency because this was enough.
5. **Padding `ClusterState` to separate cache lines: did not help, reverted.** I suspected false
   sharing between cluster states allocated next to each other. Results were within noise
   (16.7 vs 16.3 at /16), so I removed it rather than keep 128 unexplained bytes per cluster.
6. **Power of two choices.** After 1-4 the threads were still at 16.7. The cause was the policy,
   not the data structure: "scan all and take the best" makes every thread see the *same* best
   cluster (free slots differ by a handful out of a million) so all 16 threads CAS the same
   counter at once, and the 7 other cluster counters sit idle. Looking at two random clusters
   and taking the one with more free slots spreads the threads over all counters and reads 2
   cache lines instead of 8. /16 16.7 -> 44.7, /4 17.3 -> 33.1. Throughput now grows with the
   thread count (26 -> 33 -> 45 Melem/s) instead of collapsing.

Not done on purpose: lock-free group lookup (`arc-swap`/RCU), per-thread permit caches,
`Ordering` weaker than `Relaxed` (there is no data published through these counters, `Relaxed`
is already what is used), a different hash for the group map. None was needed for the target
and each adds code I would have to defend.

Selection policy, and why:

"Roughly least-loaded by free slots":

- Groups with up to 4 clusters (`EXACT_SCAN_MAX`): exact. Scan all, take the cluster with the
  most free slots (ties broken by a per-thread rotating start so equal clusters are not all
  hit through the same index). Reading 4 counters costs about the same as sampling 2, and an
  approximation is visibly worse here: with 3 clusters (100/50/10 slots) power of two choices
  compares the two big ones only 1 time in 3 and gave 56-66 queries on the biggest one after
  90 queries, where the exact policy gives 70 / 20 / 0 and levels the free slots at 30 / 30 / 10.
- Groups with more than 4 clusters: power of two choices, i.e. two distinct random clusters,
  the one with more free slots wins. It never picks the more loaded of the two, so the most
  loaded cluster of a group is hardly ever chosen. Measured spread (`tests/selection.rs`): 8
  equal clusters of 1000 after 4000 queries differ by 2-3 running queries; 8 clusters of
  8..64 slots after 200 queries end with 7-21 free slots each and the biggest cluster carries
  the most. If the two choices are full or the CAS loses, it falls back to the exact scan, so
  `Busy` is only returned when no cluster has a free slot.
- Cost of the approximation: for big groups a query can go to a cluster that is not the
  single emptiest one. For a SQL gateway that is irrelevant next to query duration; what
  matters is that no cluster is hammered or starved, and that is what the tests check.

## Part 3: Waiting for a slot

## Part 4: Global admission across 6 pods

## Bonus: INC-5 and INC-6

## Before shipping, I would still want

-
