# Admission Control Exercise

**Time box: 90 minutes.** Stop when time runs out. A partial solution with clear notes beats a rushed complete one.

## Context

[QueryFlux](https://github.com/lakeops-org/queryflux) is a SQL gateway written in Rust. It sits in front of groups of engine clusters (Trino, StarRocks, DuckDB, ...) and routes each query to one of them. Before a query is forwarded, it must be **admitted** to a cluster. Each cluster accepts at most `max_running` concurrent queries.

This crate is a simplified version of that admission controller. It is in production, and it is causing incidents. Your job is to make it production ready.

`try_acquire` runs on every query, at thousands of queries per second, on 16-core gateway pods. It is on the hot path.

## Setup

```sh
cargo test     # the smoke tests in tests/smoke.rs, plus any tests you add
cargo bench    # Part 2
```

You need stable Rust.

## Rules

- Keep the public API: type names, method names, and signatures. Everything else is yours to change.
- **We run a hidden test suite against your submission.** The behavior the hidden suite checks is described in this README and in the doc comments in `src/lib.rs`.
- Prefer `std` and `tokio`. If you add a dependency, justify it in `NOTES.md`.
- AI assistants are allowed. In the follow-up call you'll walk us through your changes and explain every line, and we'll ask you to change something live.

## Part 1: Incidents (about 35 min)

These four incidents were reported against the current code. For each one:

1. Find the root cause.
2. **Write a test that reproduces it** and fails on the original code. Put your tests in `tests/incidents.rs`.
3. Fix it.
4. Write a short root-cause analysis in `NOTES.md`.

> **INC-1: Cap exceeded, then wedged.** During a traffic spike, `trino-3` was seen running more queries than its `max_running`. The next morning the gateway rejected every query to the group as `Busy`, even though Trino was idle. A pod restart fixed it.

> **INC-2: Capacity cut.** An operator lowered a cluster's `max_running` from 40 to 10 with `set_max_running` while it was busy. In staging, which runs a debug build, the gateway pod panicked. In production, the whole group kept returning `Busy` for a while, even though its other clusters were idle.

> **INC-3: Busy with free capacity.** In a load test, clients got `Busy` errors while the dashboard showed free slots in the group.

> **INC-4: Slots never recovered.** During a Trino outage, clients timed out and disconnected. After Trino came back, the group stayed at reduced capacity until the gateway was restarted.

Some incidents may share a root cause, and one incident may have more than one cause.

## Part 2: Performance (about 30 min)

Run `cargo bench` before you change anything and save the numbers.

**Target: at least 3× the starter's throughput on `acquire_release/16`, on your machine, without breaking correctness.**

Record the before and after numbers in `NOTES.md`, and explain *why* each change helps. If a change didn't help, say so. A change you can't explain counts for less than one you can.

The behavior must stay "roughly least-loaded": queries spread across clusters in a group by how much capacity each has free. The exact policy is up to you. Justify it.

## Part 3: Waiting for a slot (about 15 min)

Implement `acquire_timeout` and `queued`. When every cluster in the group is full, the caller waits up to `wait` for a slot instead of failing straight away:

- When a slot frees up, a waiter must be woken **promptly**, not on the next poll.
- When the wait expires, the call returns `Timeout`.
- A waiter whose future is dropped must leave the queue.
- `queued(group)` reports how many callers are waiting right now.

## Part 4: Design (about 10 min, written in `NOTES.md`)

We are scaling to **6 gateway pods** behind a load balancer, all sharing the same clusters. Today each pod enforces `max_running` on its own, so a cluster with `max_running = 40` can receive 240 queries.

How would you enforce the cap across all pods?

- Give your design and what it costs on the hot path.
- Say what happens when a pod dies holding slots, and when the network partitions.
- Say what you would deliberately *not* build.

Keep it to half a page.

## Bonus (optional, very hard)

Two more problems from production:

> **INC-5:** The `reconcile` job, which corrects drift against the engine's own count, is supposed to fix INC-1-style drift. Under load it makes the counters *less* accurate.

> **INC-6:** During config reloads, clusters briefly run above their caps, and the dashboards show running counts reset to zero.

Fix `reconcile` and `reload`. Explain in `NOTES.md` what was wrong and what guarantees your version gives. `try_acquire` must not take an exclusive lock. Extra credit: a [`loom`](https://github.com/tokio-rs/loom) model or a deterministic interleaving test.

## Submitting

Push your work to a **private** repository and invite the reviewers, or email us the output of `git format-patch origin/main`. Please don't fork this repo publicly or open pull requests against it.
