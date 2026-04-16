package zincflow.providers;

import zincflow.core.ComponentState;
import zincflow.core.Provider;

import java.util.ArrayList;
import java.util.List;

/// Provenance recorder — a bounded ring buffer of FlowFile lifecycle
/// events (created / processed / routed / dropped / failed). Oldest
/// entries evict once the buffer fills. When the provider is disabled
/// {@link #record} becomes a no-op, so production operators can flip
/// provenance off without rebuilding the pipeline.
///
/// Mirrors zinc-flow-csharp's ProvenanceProvider (Core/Providers.cs).
public final class ProvenanceProvider implements Provider {

    public static final String NAME = "provenance";
    public static final String TYPE = "ProvenanceProvider";
    /// Default buffer size — matches the C# default.
    public static final int DEFAULT_CAPACITY = 100_000;

    /// Shared no-op instance — {@code record(...)} early-returns because
    /// the provider stays DISABLED. Useful as a null-object for callers
    /// that want to call {@code record} unconditionally when no
    /// provenance provider is wired.
    public static final ProvenanceProvider DISABLED = new ProvenanceProvider(1);

    public enum EventType { CREATED, PROCESSED, ROUTED, DROPPED, FAILED }

    public record Event(
            long flowFileId,
            EventType type,
            String component,
            String details,
            long timestampMillis) { }

    private final Event[] buffer;
    private final int capacity;
    private final Object lock = new Object();
    private int head; // next write slot
    private int count;
    private volatile ComponentState state = ComponentState.DISABLED;

    public ProvenanceProvider() {
        this(DEFAULT_CAPACITY);
    }

    public ProvenanceProvider(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("ProvenanceProvider capacity must be > 0, got " + capacity);
        }
        this.capacity = capacity;
        this.buffer = new Event[capacity];
    }

    @Override public String name() { return NAME; }
    @Override public String providerType() { return TYPE; }
    @Override public ComponentState state() { return state; }
    @Override public void enable() { state = ComponentState.ENABLED; }
    @Override public void disable(int drainTimeoutSeconds) { state = ComponentState.DISABLED; }
    @Override public void shutdown() { state = ComponentState.DISABLED; }

    public int capacity() { return capacity; }

    public int size() {
        synchronized (lock) { return count; }
    }

    /// Drop an event in the ring buffer. Silent no-op when disabled so
    /// callers can sprinkle {@code record(...)} calls in hot paths
    /// without a per-call enable check.
    public void record(long flowFileId, EventType type, String component, String details) {
        if (!isEnabled()) return;
        Event evt = new Event(
                flowFileId,
                type,
                component == null ? "" : component,
                details == null ? "" : details,
                System.currentTimeMillis());
        synchronized (lock) {
            buffer[head] = evt;
            head = (head + 1) % capacity;
            if (count < capacity) count++;
        }
    }

    public void record(long flowFileId, EventType type, String component) {
        record(flowFileId, type, component, "");
    }

    /// Events for a single FlowFile, oldest first. Empty if none recorded
    /// (either the id never appeared, or it was evicted).
    public List<Event> getEvents(long flowFileId) {
        List<Event> out = new ArrayList<>();
        synchronized (lock) {
            int start = count < capacity ? 0 : head;
            for (int i = 0; i < count; i++) {
                Event e = buffer[(start + i) % capacity];
                if (e != null && e.flowFileId() == flowFileId) out.add(e);
            }
        }
        return out;
    }

    /// Most recent N events across every FlowFile, oldest first within
    /// the window. If fewer than N are available all are returned.
    public List<Event> getRecent(int n) {
        if (n <= 0) return List.of();
        List<Event> out = new ArrayList<>(Math.min(n, capacity));
        synchronized (lock) {
            int take = Math.min(n, count);
            int start = ((head - take) % capacity + capacity) % capacity;
            for (int i = 0; i < take; i++) {
                Event e = buffer[(start + i) % capacity];
                if (e != null) out.add(e);
            }
        }
        return out;
    }

    public static final class Plugin implements zincflow.core.ProviderPlugin {
        @Override public String providerType() { return TYPE; }
        @Override public String description() { return "Bounded ring buffer of FlowFile lifecycle events."; }
        @Override public java.util.List<String> configKeys() { return java.util.List.of("buffer"); }
        @Override public Provider create(java.util.Map<String, Object> config) {
            Object buf = config.get("buffer");
            int capacity = buf instanceof Number n ? n.intValue() : DEFAULT_CAPACITY;
            return new ProvenanceProvider(capacity);
        }
    }
}
