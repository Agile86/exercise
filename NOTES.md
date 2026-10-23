# Notes

## Part 1: Incidents

| Incident | Root cause | Reproducing test | Fix |
|----------|------------|------------------|-----|
| INC-1 | | | |
| INC-2 | | | |
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
