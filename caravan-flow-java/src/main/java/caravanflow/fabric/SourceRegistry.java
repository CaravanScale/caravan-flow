package caravanflow.fabric;

import caravanflow.core.Source;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/// Registry of source factories keyed by {@code name@version}. Mirror
/// of {@link Registry} for {@link Source} plugins. Config resolves
/// {@code type: GetFile@1.0.0} → factory → instance the same way
/// processors do; a bare {@code type: GetFile} falls through to the
/// latest registered version.
public final class SourceRegistry {

    @FunctionalInterface
    public interface Factory {
        Source create(String name, Map<String, Object> config);
    }

    public record TypeInfo(String name, String version, String description,
                           List<String> configKeys) {
        public TypeInfo {
            configKeys = List.copyOf(configKeys);
        }
        public String qualifiedName() { return TypeRefs.qualify(name, version); }
    }

    private final ConcurrentHashMap<String, Factory> versioned = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> latestVersion = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, TypeInfo> metadata = new ConcurrentHashMap<>();

    public void register(TypeInfo info, Factory factory) {
        String key = info.qualifiedName();
        versioned.put(key, factory);
        metadata.put(key, info);
        latestVersion.merge(info.name(), info.version(),
                (oldV, newV) -> TypeRefs.compareVersions(oldV, newV) >= 0 ? oldV : newV);
    }

    public void register(String type, Factory factory) {
        register(new TypeInfo(type, TypeRefs.DEFAULT_VERSION, "", List.of()), factory);
    }

    public boolean has(String type) {
        return resolveKey(type) != null;
    }

    private String resolveKey(String type) {
        if (type == null || type.isEmpty()) return null;
        if (type.contains("@")) return versioned.containsKey(type) ? type : null;
        String latest = latestVersion.get(type);
        return latest == null ? null : TypeRefs.qualify(type, latest);
    }

    public Source create(String type, String name, Map<String, Object> config) {
        String key = resolveKey(type);
        if (key == null) {
            throw new IllegalArgumentException("SourceRegistry: unknown source type '" + type + "'");
        }
        return versioned.get(key).create(name, config == null ? Map.of() : config);
    }

    public List<TypeInfo> listAll() {
        List<TypeInfo> out = new ArrayList<>(metadata.values());
        out.sort(Comparator.comparing(TypeInfo::name).thenComparing(
                TypeInfo::version, TypeRefs::compareVersions));
        return out;
    }

    public List<TypeInfo> listVersions(String type) {
        List<TypeInfo> out = new ArrayList<>();
        for (TypeInfo info : metadata.values()) {
            if (info.name().equals(type)) out.add(info);
        }
        out.sort(Comparator.comparing(TypeInfo::version, TypeRefs::compareVersions));
        return out;
    }

    public TypeInfo latest(String type) {
        String latest = latestVersion.get(type);
        return latest == null ? null : metadata.get(TypeRefs.qualify(type, latest));
    }
}
