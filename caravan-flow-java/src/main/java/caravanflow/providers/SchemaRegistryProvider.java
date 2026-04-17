package caravanflow.providers;

import caravanflow.core.ComponentState;
import caravanflow.core.Provider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/// Embedded, airgapped schema registry behind the {@link Provider}
/// interface. Matches Confluent semantics closely enough that the
/// {@link caravanflow.fabric.SchemaRegistryHandler} can expose a
/// drop-in-compatible REST surface.
///
/// <h2>ID model</h2>
/// Every unique schema definition is assigned a single global integer
/// id on first registration. Registering the same definition under a
/// different subject reuses the existing id. This matches Confluent's
/// "one id per schema" model — tooling that resolves schemas by id
/// (most Avro-serializer clients) works verbatim.
///
/// Per-subject version numbers are sequential starting at 1. Deleting
/// a version doesn't renumber later ones: if you delete v2 after
/// having v1, v2, v3, the remaining versions stay v1 + v3.
public final class SchemaRegistryProvider implements Provider {

    public static final String NAME = "schema_registry";
    public static final String TYPE = "SchemaRegistryProvider";

    /// Immutable record of a registered schema.
    public record Schema(int id, String subject, int version, String definition) {
        public Schema {
            if (subject == null || subject.isEmpty())
                throw new IllegalArgumentException("schema subject must not be blank");
            if (version < 1)
                throw new IllegalArgumentException("schema version must be >= 1");
            if (id < 1)
                throw new IllegalArgumentException("schema id must be >= 1");
            if (definition == null)
                throw new IllegalArgumentException("schema definition must not be null");
        }
    }

    /// Map from subject → ordered list of {@link Schema}. Concurrent
    /// access uses a per-subject monitor (the {@link List} itself) so
    /// registrations under different subjects don't contend.
    private final ConcurrentHashMap<String, List<Schema>> bySubject = new ConcurrentHashMap<>();
    /// Definition text → global id. Lets us dedupe schemas across
    /// subjects and give back stable ids.
    private final ConcurrentHashMap<String, Integer> idByDefinition = new ConcurrentHashMap<>();
    /// Global id → one representative Schema record (for id-based
    /// lookup). The record's subject/version reflect the first
    /// subject that registered the definition.
    private final ConcurrentHashMap<Integer, Schema> byId = new ConcurrentHashMap<>();
    private final AtomicInteger nextId = new AtomicInteger();
    private volatile ComponentState state = ComponentState.DISABLED;

    @Override public String name() { return NAME; }
    @Override public String providerType() { return TYPE; }
    @Override public ComponentState state() { return state; }
    @Override public void enable() { state = ComponentState.ENABLED; }
    @Override public void disable(int drainTimeoutSeconds) { state = ComponentState.DISABLED; }
    @Override public void shutdown() {
        state = ComponentState.DISABLED;
        bySubject.clear();
        idByDefinition.clear();
        byId.clear();
    }

    /// Register a new schema version under {@code subject}. When the
    /// definition is already known (under this or any other subject)
    /// the existing global id is reused; the subject still gets a new
    /// per-subject version entry so its history is preserved.
    public Schema register(String subject, String definition) {
        if (subject == null || subject.isEmpty())
            throw new IllegalArgumentException("subject must not be blank");
        if (definition == null)
            throw new IllegalArgumentException("definition must not be null");

        // Short-circuit: same definition already present under the same
        // subject → return the existing entry rather than bumping to
        // a new version. Matches Confluent's idempotent register behavior.
        Schema existing = findBySubjectAndDefinition(subject, definition);
        if (existing != null) return existing;

        int id = idByDefinition.computeIfAbsent(definition, _ -> nextId.incrementAndGet());
        List<Schema> versions = bySubject.computeIfAbsent(subject, _ -> Collections.synchronizedList(new ArrayList<>()));
        Schema registered;
        synchronized (versions) {
            int nextVersion = versions.isEmpty() ? 1 : versions.get(versions.size() - 1).version() + 1;
            registered = new Schema(id, subject, nextVersion, definition);
            versions.add(registered);
        }
        byId.putIfAbsent(id, registered);
        return registered;
    }

    private Schema findBySubjectAndDefinition(String subject, String definition) {
        List<Schema> versions = bySubject.get(subject);
        if (versions == null) return null;
        synchronized (versions) {
            for (Schema s : versions) {
                if (s.definition().equals(definition)) return s;
            }
        }
        return null;
    }

    public Optional<Schema> getById(int id) {
        return Optional.ofNullable(byId.get(id));
    }

    public Optional<Schema> getEntry(String subject, int version) {
        List<Schema> versions = bySubject.get(subject);
        if (versions == null) return Optional.empty();
        synchronized (versions) {
            for (Schema s : versions) if (s.version() == version) return Optional.of(s);
        }
        return Optional.empty();
    }

    public Optional<Schema> latest(String subject) {
        List<Schema> versions = bySubject.get(subject);
        if (versions == null) return Optional.empty();
        synchronized (versions) {
            return versions.isEmpty() ? Optional.empty() : Optional.of(versions.get(versions.size() - 1));
        }
    }

    public List<String> listSubjects() {
        List<String> out = new ArrayList<>(bySubject.keySet());
        Collections.sort(out);
        return out;
    }

    public List<Integer> listVersions(String subject) {
        List<Schema> versions = bySubject.get(subject);
        if (versions == null) return List.of();
        synchronized (versions) {
            List<Integer> out = new ArrayList<>(versions.size());
            for (Schema s : versions) out.add(s.version());
            return out;
        }
    }

    /// Delete every version under {@code subject} and return the
    /// version numbers that were removed (so the HTTP layer can echo
    /// them back the way Confluent does). Returns empty when the
    /// subject is unknown.
    public List<Integer> deleteSubject(String subject) {
        List<Schema> versions = bySubject.remove(subject);
        if (versions == null) return List.of();
        synchronized (versions) {
            List<Integer> removed = new ArrayList<>(versions.size());
            for (Schema s : versions) removed.add(s.version());
            return removed;
        }
    }

    public boolean deleteVersion(String subject, int version) {
        List<Schema> versions = bySubject.get(subject);
        if (versions == null) return false;
        synchronized (versions) {
            return versions.removeIf(s -> s.version() == version);
        }
    }

    /// Snapshot for diagnostics — returns a subject → versions map
    /// that's safe to iterate without holding the per-subject lock.
    public Map<String, List<Integer>> snapshot() {
        Map<String, List<Integer>> out = new LinkedHashMap<>();
        for (String subject : listSubjects()) out.put(subject, listVersions(subject));
        return out;
    }

    public int size() { return nextId.get(); }

    public static final class Plugin implements caravanflow.core.ProviderPlugin {
        @Override public String providerType() { return TYPE; }
        @Override public String description() { return "Embedded Confluent-shape schema registry."; }
        @Override public Provider create(Map<String, Object> config) { return new SchemaRegistryProvider(); }
    }
}
