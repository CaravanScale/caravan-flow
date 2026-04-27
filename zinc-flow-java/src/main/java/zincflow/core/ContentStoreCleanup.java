package zincflow.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/// Tracks every {@link ClaimContent} currently in-flight through the
/// pipeline and periodically asks a {@link ContentStore} to delete
/// claims that are no longer referenced. Without this, claims would
/// accumulate in {@link FileContentStore} forever — a
/// ContentStore has no built-in TTL, and the FlowFile that owned a
/// claim may be long gone by the time we notice.
///
/// Thread-safe. The active set is a {@link HashSet} under a monitor —
/// claims come and go often enough that a copy-on-write set would
/// churn more than the contention costs. Sweeps are light I/O (one
/// {@code delete} per orphaned claim), so serializing them keeps the
/// codepath simple.
///
/// Mirror of zinc-flow-csharp's ContentStoreCleanup.
public final class ContentStoreCleanup {

    private static final Logger log = LoggerFactory.getLogger(ContentStoreCleanup.class);

    private final ContentStore store;
    private final Set<String> active = new HashSet<>();
    private final Object lock = new Object();
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> running;

    public ContentStoreCleanup(ContentStore store) {
        if (store == null) throw new IllegalArgumentException("ContentStoreCleanup: store must not be null");
        this.store = store;
    }

    /// Register a claim that's now live. Safe to call from any thread
    /// — typically from {@link ContentHelpers#maybeOffload} or wherever
    /// else a new ClaimContent is created.
    public void track(String claimId) {
        if (claimId == null || claimId.isEmpty()) return;
        synchronized (lock) { active.add(claimId); }
    }

    /// Mark a claim as releasable. The next sweep will delete it from
    /// the store if it isn't re-tracked before then.
    public void release(String claimId) {
        if (claimId == null) return;
        synchronized (lock) { active.remove(claimId); }
    }

    public int activeCount() {
        synchronized (lock) { return active.size(); }
    }

    /// Delete any claim known to the store that isn't currently in the
    /// active set. Returns the number deleted — callers can log or
    /// metric-ize this without having to subscribe to logging.
    public int sweep(List<String> knownClaims) {
        if (knownClaims == null || knownClaims.isEmpty()) return 0;
        Set<String> snapshot;
        synchronized (lock) { snapshot = Set.copyOf(active); }
        int deleted = 0;
        for (String claimId : knownClaims) {
            if (!snapshot.contains(claimId)) {
                try {
                    store.delete(claimId);
                    deleted++;
                } catch (RuntimeException ex) {
                    log.warn("content-cleanup: failed to delete {} — {}", claimId, ex.toString());
                }
            }
        }
        return deleted;
    }

    /// Start a periodic sweep. Caller provides a {@link ClaimEnumerator}
    /// that returns every claim the store currently holds — disk-backed
    /// stores walk the directory, memory stores enumerate the map.
    /// Running more than once is idempotent: the previous schedule is
    /// cancelled first.
    public void startPeriodicSweep(long period, TimeUnit unit, ClaimEnumerator enumerator) {
        stopPeriodicSweep();
        // Scheduler thread stays platform (STPE timing relies on
        // park/unpark). The sweep body runs on a virtual thread so a
        // slow enumerator or a disk hiccup on delete can't stall the
        // next tick.
        scheduler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().daemon().name("zinc-flow-content-cleanup").factory());
        running = scheduler.scheduleAtFixedRate(
                () -> Thread.startVirtualThread(() -> runSweep(enumerator)),
                period, period, unit);
    }

    private void runSweep(ClaimEnumerator enumerator) {
        try {
            int deleted = sweep(enumerator.enumerate());
            if (deleted > 0) {
                log.info("content-cleanup: swept {} orphaned claim(s)", deleted);
            }
        } catch (RuntimeException ex) {
            log.warn("content-cleanup: sweep failed — {}", ex.toString());
        }
    }

    public void stopPeriodicSweep() {
        if (running != null) {
            running.cancel(false);
            running = null;
        }
        if (scheduler != null) {
            scheduler.shutdown();
            scheduler = null;
        }
    }

    @FunctionalInterface
    public interface ClaimEnumerator {
        List<String> enumerate();
    }
}
