package zincflow.providers;

import zincflow.core.ComponentState;
import zincflow.core.Provider;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/// Embedded, airgapped schema registry behind the {@link Provider}
/// interface. Processors that need registry-backed schemas — OCF
/// readers configured with a {@code reader_schema_subject}, record
/// codecs that auto-register new subjects — declare
/// {@code requires=["schema_registry"]} and pull the provider from
/// the {@link zincflow.core.ProcessorContext}.
///
/// Schemas live in memory and are keyed by {@code subject}; each
/// {@link #register register} call bumps the version for that subject,
/// so callers can pin to a specific version or ask for the latest.
/// Mirror of zinc-flow-csharp's SchemaRegistryProvider, minus the
/// remote-registry option which we've explicitly left out — the whole
/// point of this provider is "no cross-service dependency at runtime".
public final class SchemaRegistryProvider implements Provider {

    /// One schema entry: immutable record of what was registered.
    public record Schema(String subject, int version, String definition) {
        public Schema {
            if (subject == null || subject.isEmpty())
                throw new IllegalArgumentException("schema subject must not be blank");
            if (version < 1)
                throw new IllegalArgumentException("schema version must be >= 1");
            if (definition == null)
                throw new IllegalArgumentException("schema definition must not be null");
        }
    }

    private final ConcurrentHashMap<String, List<Schema>> bySubject = new ConcurrentHashMap<>();
    private final AtomicInteger idGenerator = new AtomicInteger();
    private volatile ComponentState state = ComponentState.DISABLED;

    @Override public String name() { return "schema_registry"; }
    @Override public String providerType() { return "schema_registry"; }
    @Override public ComponentState state() { return state; }
    @Override public void enable() { state = ComponentState.ENABLED; }
    @Override public void disable(int drainTimeoutSeconds) { state = ComponentState.DISABLED; }
    @Override public void shutdown() { state = ComponentState.DISABLED; bySubject.clear(); }

    /// Register a new schema version under {@code subject}. Version
    /// numbers start at 1 and increment per subject — the first call
    /// for {@code "order"} yields v1, the second v2, and so on.
    public Schema register(String subject, String definition) {
        List<Schema> versions = bySubject.computeIfAbsent(subject, _ -> java.util.Collections.synchronizedList(new java.util.ArrayList<>()));
        synchronized (versions) {
            int nextVersion = versions.size() + 1;
            Schema s = new Schema(subject, nextVersion, definition);
            versions.add(s);
            idGenerator.incrementAndGet();
            return s;
        }
    }

    /// Fetch a specific version. Returns null if the subject or
    /// version is unknown.
    public Schema get(String subject, int version) {
        List<Schema> versions = bySubject.get(subject);
        if (versions == null) return null;
        synchronized (versions) {
            for (Schema s : versions) if (s.version() == version) return s;
        }
        return null;
    }

    /// Latest version for a subject, or null if the subject has no
    /// registered schemas.
    public Schema latest(String subject) {
        List<Schema> versions = bySubject.get(subject);
        if (versions == null) return null;
        synchronized (versions) {
            return versions.isEmpty() ? null : versions.get(versions.size() - 1);
        }
    }

    public List<String> subjects() {
        return List.copyOf(bySubject.keySet());
    }

    public List<Schema> versions(String subject) {
        List<Schema> versions = bySubject.get(subject);
        if (versions == null) return List.of();
        synchronized (versions) {
            return List.copyOf(versions);
        }
    }

    public int size() {
        return idGenerator.get();
    }
}
