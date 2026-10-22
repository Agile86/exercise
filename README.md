# Admission Control Exercise

**Time box: 90 minutes.** Stop when time runs out. A partial solution with clear notes beats a rushed complete one.

## Context

[QueryFlux](https://github.com/lakeops-org/queryflux) is a SQL gateway written in Rust. It sits in front of groups of engine clusters (Trino, StarRocks, DuckDB, ...) and routes each query to one of them. Before a query is forwarded, it must be **admitted** to a cluster. Each cluster accepts at most `max_running` concurrent queries.

This crate is a simplified version of that admission controller. It works in a demo, but it is **not production ready**. Your job is to make it production ready.

`try_acquire` runs on every query, at thousands of queries per second, so it is on the hot path.

## Setup

```sh
cargo test --no-fail-fast  # Parts 1 and 3
cargo bench                # Part 2
cargo test --test bonus -- --include-ignored   # Bonus
```

You need stable Rust. The bench uses `criterion` and writes reports to `target/criterion/`.

## Rules

- Keep the public API that the tests and the bench use: type names, method names, and signatures. Everything else, including internals and private types, is yours to change.
- Do not edit the tests. You may *add* tests.
- Prefer `std` and `tokio`. If you add a dependency, justify it in `NOTES.md`.
- AI assistants are allowed. In the follow-up call you'll walk us through your changes and explain them, line by line.

## Tasks

### Part 1: Correctness (about 25 min)

`cargo test --test part1_correctness` fails. Find and fix the bugs. For each one, write a sentence or two in `NOTES.md` saying what the bug was and how it shows up in production.

The tests also describe behavior the code must have. For example, a query that is aborted because the client disconnected must not hold its slot forever.

### Part 2: Performance (about 30 min)

Run `cargo bench` and save the numbers. Make `try_acquire` + `release` as cheap as you can, then record the before and after numbers in `NOTES.md`.

For each change, explain *why* it helps. A change you can't explain counts for less than one you can. If something doesn't scale with thread count, say what limits it.

### Part 3: Queueing (about 20 min)

Implement `acquire_timeout` and `queued`.

When every cluster in the group is full, the caller waits up to `wait` for a slot instead of failing straight away. `queued(group)` reports how many callers are waiting right now, and it is exported as a gauge. The tests are in `tests/part3_queueing.rs`.

Watch out for lost wakeups, waiters that are cancelled, and timeouts.

### Part 4: Notes (about 10 min)

Fill in `NOTES.md`. Besides the parts above, answer this: **what would you still want before shipping this?**

### Bonus (optional, very hard)

Production counters drift. Engines kill queries, gateway pods crash, and releases get lost. Config also changes at runtime. Make these two methods correct:

- **`reconcile(group, cluster, fetch)`**: `fetch` asks the engine how many queries it is actually running.
  - It is slow (50–500 ms), and its answer is already stale when it arrives.
  - Correct the counter *without* losing admissions or releases that happen while `fetch` is in flight.
  - Never exceed `max_running`.
- **`reload(config)`**: swap in a new config while queries are in flight.
  - Permits admitted under the old config must release correctly.
  - Capacity must hold *across* the old and new configs: a reload must not let a cluster run 2× its limit.
  - A removed cluster takes no new queries but drains the ones it has.
  - Lowering `max_running` below the current count pauses admission until the count drains. It must not panic or underflow.
  - `try_acquire` must not take an exclusive lock.

The tests are in `tests/bonus.rs`. A partial solution with good reasoning in `NOTES.md` is worth a lot. Extra credit: a [`loom`](https://github.com/tokio-rs/loom) model or a deterministic interleaving test that proves there are no lost updates.

## Submitting

Push your work to a **private** repository and invite the reviewers, or email us the output of `git format-patch origin/main`. Please don't fork this repo publicly or open pull requests against it.
