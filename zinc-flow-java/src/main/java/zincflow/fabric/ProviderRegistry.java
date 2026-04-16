package zincflow.fabric;

import zincflow.core.Provider;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/// Registry of provider factories keyed by {@code name@version}.
/// Parallel to {@link Registry} / {@link SourceRegistry}. Callers
/// resolve a config {@code type: LoggingProvider@1.0.0} to a factory
/// at load time; a bare {@code type: LoggingProvider} picks the
/// latest version.
///
/// Providers are conceptually singletons per type within one worker,
/// but the registry supports multiple versions to make plugin-based
/// replacement work: a plugin jar shipping a newer version of the
/// same type wins the latest-version lookup without changing any
/// framework code.
public final class ProviderRegistry {

    @FunctionalInterface
    public interface Factory {
        Provider create(Map<String, Object> config);
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

    public Provider create(String type, Map<String, Object> config) {
        String key = resolveKey(type);
        if (key == null) {
            throw new IllegalArgumentException("ProviderRegistry: unknown provider type '" + type + "'");
        }
        return versioned.get(key).create(config == null ? Map.of() : config);
    }

    public List<TypeInfo> listAll() {
        List<TypeInfo> out = new ArrayList<>(metadata.values());
        out.sort(Comparator.comparing(TypeInfo::name).thenComparing(
                TypeInfo::version, TypeRefs::compareVersions));
        return out;
    }

    public TypeInfo latest(String type) {
        String latest = latestVersion.get(type);
        return latest == null ? null : metadata.get(TypeRefs.qualify(type, latest));
    }
}
