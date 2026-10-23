# Notes

## Part 1: Incidents

| Incident | Root cause | Reproducing test | Fix |
|----------|------------|------------------|-----|
| INC-1 | `try_increment` was `load`, compare, `store(current + 1)`: not atomic. Two threads read the same `running`, both pass the cap check, both store the same value, so one increment is lost. More permits than `max_running` are handed out and `running` is lower than the real number of holders. When those holders release, `decrement` (`fetch_sub`, unguarded) takes the counter below 0, it wraps to ~`u64::MAX`, the cluster looks permanently full and every query gets `Busy`, until a restart resets the counter. | `inc1_cap_is_never_exceeded_under_contention` (16 threads race for 1000 slots: starter hands out 1021) and `inc1_group_is_not_wedged_after_a_spike` (after releasing everything, starter has `running = 2^64 - 19`). Both fail 11/11 runs on the starter. | Check and increment are one CAS loop (`fetch_update`). `decrement` is also a CAS that stops at 0 (`checked_sub`), so an accounting error elsewhere (`reconcile`, `reload`) can no longer wedge the cluster. |
| INC-2 | `available()` was `max_running - running` on `u64`. After `set_max_running` lowers the cap below the current load (40 -> 10 with 40 running) the subtraction underflows. Debug build: `attempt to subtract with overflow` panic (staging). Release build: it wraps to ~`2^64 - 30`, so the over-cap cluster looks like the emptiest one, `pick` (max by `available`) always chooses it, `try_increment` correctly refuses (`40 >= 10`) and `try_acquire` returns `Busy` although the other clusters are idle. It lasts until the cluster drains below the new cap. | `inc2_cut_below_running_sends_traffic_to_other_clusters` (debug: panic; release: `Busy although b is idle [a=40, b=0]`) and `inc2_over_cap_cluster_admits_again_only_after_draining_below_new_cap` (debug panic only: with one cluster `Busy` is the right answer even when it is reached by accident). Run both with `cargo test` and `cargo test --release`. | `available()` uses `saturating_sub`, so an over-cap cluster has 0 free slots and the existing `available > 0` filter drops it from the candidates. |
| INC-3 | | | |
| INC-4 | | | |

## Part 2: Performance

Machine (CPU and core count): AMD Ryzen 9 5900HS, 8 cores / 16 threads, Linux, rustc 1.98.1, `cargo bench` (criterion, 3 s measurement).

| Bench | Before | After |
|-------|--------|-------|
| acquire_release/1  | median 2.61 Melem/s (range 2.58-2.70), ~383 ns/iter | |
| acquire_release/4  | median 4.39 Melem/s (range 4.21-4.62), ~912 ns/iter | |
| acquire_release/16 | median 6.81 Melem/s (range 4.02-7.07), ~2.35 µs/iter | |

Baseline = 4 runs of `cargo bench` on the unmodified starter (centre estimate of each run, Melem/s):
/1: 2.62, 2.58, 2.70, 2.60; /4: 4.21, 4.62, 4.46, 4.31; /16: 4.02, 6.73, 6.88, 7.07.
The first /16 run (4.02) is an outlier (laptop CPU, boost clocks / SMT noise); the other three
agree within ~5%. I use the median (6.81) as the baseline, which is the stricter choice.

Target: >= 3x on acquire_release/16, i.e. >= ~20.4 Melem/s against the median baseline
(>= ~12 Melem/s if the 4.02 outlier were the baseline). Going from 4 to 16 threads gives
only ~1.5x more throughput on 4x the threads: the hot path is serialized on shared state.

Changes, and why each one helps (or didn't):

-

Selection policy, and why:

## Part 3: Waiting for a slot

## Part 4: Global admission across 6 pods

## Bonus: INC-5 and INC-6

## Before shipping, I would still want

-
