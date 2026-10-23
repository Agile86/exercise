//! Cluster admission control for a SQL gateway.
//!
//! A gateway fronts several *groups* of engine clusters (Trino, StarRocks, ...).
//! Every incoming query must be admitted to one cluster of its target group
//! before it is forwarded, and each cluster accepts at most `max_running`
//! concurrent queries. The gateway handles thousands of queries per second,
//! so `try_acquire` sits on the hot path of every request.
//!
//! See README.md for the tasks.

use std::cell::Cell;
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{fence, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::{timeout, Instant};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterConfig {
    pub name: String,
    pub max_running: u64,
}

impl ClusterConfig {
    pub fn new(name: impl Into<String>, max_running: u64) -> Self {
        Self {
            name: name.into(),
            max_running,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PoolConfig {
    pub groups: HashMap<String, Vec<ClusterConfig>>,
}

impl PoolConfig {
    pub fn group(mut self, name: impl Into<String>, clusters: Vec<ClusterConfig>) -> Self {
        self.groups.insert(name.into(), clusters);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquireError {
    /// The group does not exist in the current config.
    UnknownGroup,
    /// Every cluster in the group is at capacity.
    Busy,
    /// `acquire_timeout` waited for its full duration without getting a slot.
    Timeout,
}

#[derive(Debug)]
struct ClusterState {
    name: String,
    max_running: AtomicU64,
    running: AtomicU64,
    /// Total queries ever admitted (a metric scraped by Prometheus).
    admissions: AtomicU64,
    /// Where callers waiting for a slot of this cluster's group are woken.
    group: Arc<GroupShared>,
}

impl ClusterState {
    fn new(config: &ClusterConfig, group: Arc<GroupShared>) -> Self {
        Self {
            name: config.name.clone(),
            max_running: AtomicU64::new(config.max_running),
            running: AtomicU64::new(0),
            admissions: AtomicU64::new(0),
            group,
        }
    }

    fn running(&self) -> u64 {
        self.running.load(Ordering::Relaxed)
    }

    fn max_running(&self) -> u64 {
        self.max_running.load(Ordering::Relaxed)
    }

    /// Free slots. Zero, not an underflow, when the cap was lowered below the load.
    fn available(&self) -> u64 {
        self.max_running().saturating_sub(self.running())
    }

    /// Take one slot if the cluster is below its cap. The check and the increment are
    /// a single compare-and-swap, so concurrent callers cannot both take the last slot.
    fn try_increment(&self) -> bool {
        self.running
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                (current < self.max_running()).then(|| current + 1)
            })
            .is_ok()
    }

    /// Give one slot back. Never goes below zero: a counter that wrapped to `u64::MAX`
    /// would make the cluster look full until the process restarts.
    ///
    /// `SeqCst`, not `Relaxed`: together with the `SeqCst` read of `queued` that follows
    /// it in `GroupShared::slot_freed`, this is the releasing half of the handshake that
    /// keeps a waiter from sleeping through a free slot (see `QueueGuard::enter`).
    /// On x86 a read-modify-write is a full barrier anyway, so this costs nothing there.
    fn decrement(&self) {
        let _ = self
            .running
            .fetch_update(Ordering::SeqCst, Ordering::Relaxed, |current| {
                current.checked_sub(1)
            });
    }
}

/// What the callers of one group share: the wait queue for `acquire_timeout`.
/// Aligned so `queued`, which every release reads, does not share a cache line with the
/// counters that every query writes.
#[derive(Debug, Default)]
#[repr(align(128))]
struct GroupShared {
    notify: Notify,
    /// Callers currently inside `acquire_timeout` waiting for this group.
    queued: AtomicU64,
}

impl GroupShared {
    /// A query finished and gave its slot back: wake one waiter, if there is one. The
    /// check keeps `Notify` (a shared, written word) off the path of every release when
    /// nobody waits.
    fn slot_freed(&self) {
        if self.queued.load(Ordering::SeqCst) > 0 {
            self.notify.notify_one();
        }
    }

    /// Capacity changed by an unknown amount (`set_max_running`, `reload`): every waiter
    /// looks again.
    fn capacity_changed(&self) {
        if self.queued.load(Ordering::SeqCst) > 0 {
            self.notify.notify_waiters();
        }
    }
}

/// Counts a caller as queued for as long as it lives, whichever way its future ends:
/// slot obtained, timeout, or dropped by the caller.
struct QueueGuard<'a>(&'a GroupShared);

impl<'a> QueueGuard<'a> {
    fn enter(shared: &'a GroupShared) -> Self {
        shared.queued.fetch_add(1, Ordering::SeqCst);
        // The waiter announces itself (`queued`) and then looks for a free slot; a
        // releaser frees the slot (`running`) and then looks for waiters. With a
        // `SeqCst` fence here and `SeqCst` on the releaser's side, at least one of them
        // sees the other: either the releaser sees `queued > 0` and wakes us, or we see
        // the freed slot. Without it both can miss each other and the waiter sleeps
        // until its timeout although a slot is free.
        fence(Ordering::SeqCst);
        Self(shared)
    }
}

impl Drop for QueueGuard<'_> {
    fn drop(&mut self) {
        self.0.queued.fetch_sub(1, Ordering::SeqCst);
    }
}

struct Group {
    shared: Arc<GroupShared>,
    clusters: Vec<Arc<ClusterState>>,
}

type Groups = HashMap<String, Group>;

/// Groups with at most this many clusters always pick the exact least-loaded one.
const EXACT_SCAN_MAX: usize = 4;

/// Number of independent copies of the group table (see `ClusterPool`).
const SHARDS: usize = 32;

/// One copy of the group table behind its own lock. The alignment keeps neighbouring
/// shards on different cache lines (128 covers the adjacent-line prefetcher).
#[repr(align(128))]
struct Shard(RwLock<Arc<Groups>>);

static NEXT_THREAD: AtomicUsize = AtomicUsize::new(0);

/// Per-thread state, so picking a shard or a random cluster costs no shared atomic.
struct ThreadCtx {
    shard: usize,
    rng: Cell<u64>,
}

thread_local! {
    static CTX: ThreadCtx = ThreadCtx::new();
}

impl ThreadCtx {
    fn new() -> Self {
        let id = NEXT_THREAD.fetch_add(1, Ordering::Relaxed);
        Self {
            shard: id % SHARDS,
            // Odd multiplier, so the seed is never 0 (xorshift would stay at 0).
            rng: Cell::new((id as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15)),
        }
    }

    /// xorshift64: cheap and good enough to spread load; not for anything else.
    fn random(&self) -> u64 {
        let mut x = self.rng.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng.set(x);
        x
    }
}

pub struct ClusterPool {
    /// `SHARDS` copies of the same table. Taking a read lock writes to the lock word,
    /// so with a single `RwLock` every thread of the gateway fights for one cache line
    /// on every query. A reader locks only the shard of its own thread; `reload` locks
    /// all of them. The `ClusterState`s inside are shared by all copies.
    shards: Box<[Shard]>,
}

impl ClusterPool {
    pub fn new(config: PoolConfig) -> Arc<Self> {
        let groups = Arc::new(build_groups(&config, &HashMap::new()));
        Arc::new(Self {
            shards: (0..SHARDS)
                .map(|_| Shard(RwLock::new(Arc::clone(&groups))))
                .collect(),
        })
    }

    /// Read access to the group table through this thread's shard.
    fn read(&self) -> RwLockReadGuard<'_, Arc<Groups>> {
        let shard = CTX.with(|c| c.shard);
        self.shards[shard].0.read().unwrap()
    }

    fn find(&self, group: &str, cluster: &str) -> Option<Arc<ClusterState>> {
        self.read()
            .get(group)?
            .clusters
            .iter()
            .find(|c| c.name == cluster)
            .cloned()
    }

    /// Admit one query to the least-loaded cluster of `group`, or fail fast with `Busy`.
    pub fn try_acquire(self: &Arc<Self>, group: &str) -> Result<Permit, AcquireError> {
        // Borrow the cluster list under the read guard: no clone of the Vec, no Arc
        // traffic for clusters we do not pick.
        let groups = self.read();
        let clusters = &groups
            .get(group)
            .ok_or(AcquireError::UnknownGroup)?
            .clusters;

        let n = clusters.len();
        if n == 0 {
            return Err(AcquireError::Busy);
        }
        let random = CTX.with(|c| c.random());

        // Fast path for bigger groups, "power of two choices": look at two different
        // random clusters and take the one with more free slots. Unlike "scan everything
        // and take the best" it does not send all threads to the same cluster at once
        // (they all saw the same best one, so they all fought over its counter), and it
        // reads 2 counters instead of n. It is only roughly least-loaded, so groups of up
        // to `EXACT_SCAN_MAX` clusters skip it: scanning 4 counters costs about the same
        // and keeps the policy exact where the approximation is visibly worse.
        if n > EXACT_SCAN_MAX {
            let i = (((random & 0xFFFF_FFFF) * n as u64) >> 32) as usize;
            let offset = 1 + (((random >> 32) * (n as u64 - 1)) >> 32) as usize;
            let j = if i + offset >= n {
                i + offset - n
            } else {
                i + offset
            };
            let (a, b) = (&clusters[i], &clusters[j]);
            let (state, available) = {
                let (fa, fb) = (a.available(), b.available());
                if fb > fa {
                    (b, fb)
                } else {
                    (a, fa)
                }
            };
            if available > 0 && state.try_increment() {
                return Ok(admit(state));
            }
        }

        // Slow path, exact: the cluster with the most free slots. The scan reads a
        // snapshot that is stale by the time we increment, so another caller may take
        // the last slot first. Then the CAS fails and we scan again with fresh values; a
        // full cluster drops out by itself (0 free slots), so `Busy` is only returned
        // once no cluster has a free slot left.
        let start = (random >> 7) as usize % n;
        loop {
            let mut best: Option<(&Arc<ClusterState>, u64)> = None;
            for k in 0..n {
                let i = if start + k >= n {
                    start + k - n
                } else {
                    start + k
                };
                let state = &clusters[i];
                let available = state.available();
                if available > 0 && best.is_none_or(|(_, most)| available > most) {
                    best = Some((state, available));
                }
            }

            let Some((state, _)) = best else {
                return Err(AcquireError::Busy);
            };
            if state.try_increment() {
                return Ok(admit(state));
            }
        }
    }

    /// Like `try_acquire`, but when every cluster is full wait up to `wait` for a slot.
    ///
    /// A waiter is woken by the release that frees a slot (no polling) and then tries
    /// like any other caller, so a caller arriving at that moment can overtake it; the
    /// waiter simply goes back to waiting. Returns `Timeout` once `wait` has passed, after
    /// one last attempt. Dropping the future takes the caller out of the queue.
    pub async fn acquire_timeout(
        self: &Arc<Self>,
        group: &str,
        wait: Duration,
    ) -> Result<Permit, AcquireError> {
        match self.try_acquire(group) {
            Err(AcquireError::Busy) => {}
            done => return done,
        }
        let shared = match self.read().get(group) {
            Some(g) => Arc::clone(&g.shared),
            None => return Err(AcquireError::UnknownGroup),
        };
        // `None`: the deadline does not fit in an `Instant`, wait without one.
        let deadline = Instant::now().checked_add(wait);

        let _queued = QueueGuard::enter(&shared);
        loop {
            // Register for wake-ups *before* looking for a slot, so a slot freed between
            // the look and the sleep is not missed.
            let notified = shared.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            match self.try_acquire(group) {
                Err(AcquireError::Busy) => {}
                done => return done,
            }

            match deadline {
                Some(deadline) => {
                    let left = deadline.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        return Err(AcquireError::Timeout);
                    }
                    let _ = timeout(left, notified).await;
                }
                None => notified.await,
            }
        }
    }

    /// Current running count for a cluster, or `None` if it is not in the current config.
    pub fn running(&self, group: &str, cluster: &str) -> Option<u64> {
        self.find(group, cluster).map(|c| c.running())
    }

    /// Number of callers currently waiting inside `acquire_timeout` for `group`.
    pub fn queued(&self, group: &str) -> u64 {
        self.read()
            .get(group)
            .map_or(0, |g| g.shared.queued.load(Ordering::SeqCst))
    }

    /// Total queries ever admitted to a cluster (a metric scraped by Prometheus).
    pub fn admissions(&self, group: &str, cluster: &str) -> u64 {
        self.find(group, cluster)
            .map_or(0, |c| c.admissions.load(Ordering::Relaxed))
    }

    /// Admin API: change a cluster's capacity at runtime.
    pub fn set_max_running(&self, group: &str, cluster: &str, max_running: u64) {
        if let Some(state) = self.find(group, cluster) {
            state.max_running.store(max_running, Ordering::SeqCst);
            state.group.capacity_changed();
        }
    }

    /// Bonus: swap in a new config while queries are in flight.
    ///
    /// A cluster keeps its identity across reloads by (group, name). Clusters removed
    /// from the config take no new queries; clusters added to it take traffic at once.
    pub fn reload(&self, config: PoolConfig) {
        // Lock every shard (always in index order) so readers never see two tables.
        let mut guards: Vec<_> = self.shards.iter().map(|s| s.0.write().unwrap()).collect();
        let old = Arc::clone(&guards[0]);
        let new = Arc::new(build_groups(&config, &old));
        for guard in &mut guards {
            **guard = Arc::clone(&new);
        }
        drop(guards);

        // Capacity may have grown, and a group may be gone: waiters look again (and
        // get `UnknownGroup` for a group that no longer exists).
        for group in old.values().chain(new.values()) {
            group.shared.capacity_changed();
        }
    }

    /// Bonus: correct a cluster's running counter against the engine's own count.
    /// `fetch` asks the engine how many queries it is running; it is slow.
    pub async fn reconcile<F, Fut>(&self, group: &str, cluster: &str, fetch: F)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = u64>,
    {
        let target = fetch().await;
        if let Some(state) = self.find(group, cluster) {
            state.running.store(target, Ordering::SeqCst);
            state.group.capacity_changed();
        }
    }
}

/// Build the group table for `config`. A group or cluster that already exists in `old`
/// under the same name is kept as it is, so its counters and its waiters survive; only
/// the `max_running` of a cluster is updated.
fn build_groups(config: &PoolConfig, old: &Groups) -> Groups {
    config
        .groups
        .iter()
        .map(|(name, clusters)| {
            let old_group = old.get(name);
            let shared = old_group.map_or_else(Default::default, |g| Arc::clone(&g.shared));
            let clusters = clusters
                .iter()
                .map(|c| {
                    let existing =
                        old_group.and_then(|g| g.clusters.iter().find(|s| s.name == c.name));
                    match existing {
                        Some(state) => {
                            state.max_running.store(c.max_running, Ordering::SeqCst);
                            Arc::clone(state)
                        }
                        None => Arc::new(ClusterState::new(c, Arc::clone(&shared))),
                    }
                })
                .collect();
            (name.clone(), Group { shared, clusters })
        })
        .collect()
}

/// Count the admission and wrap the slot we just took in a `Permit`.
fn admit(state: &Arc<ClusterState>) -> Permit {
    state.admissions.fetch_add(1, Ordering::Relaxed);
    Permit {
        state: Arc::clone(state),
    }
}

/// Proof of admission. The holder may run one query on `cluster()`.
pub struct Permit {
    state: Arc<ClusterState>,
}

impl Permit {
    pub fn cluster(&self) -> &str {
        &self.state.name
    }

    /// Give the slot back once the query has finished. Dropping the permit does the
    /// same, so a cancelled or panicking request cannot leak its slot.
    pub fn release(self) {}
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.state.decrement();
        self.state.group.slot_freed();
    }
}
