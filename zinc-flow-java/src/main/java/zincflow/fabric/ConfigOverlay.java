package zincflow.fabric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Loads a base config plus optional local / secrets overlays and
/// deep-merges them into a single effective config. Tracks which
/// layer supplied each dot-path so {@code GET /api/overlays} can
/// report provenance.
///
/// Layer order (later wins): base ← local ← secrets.
///
/// Overlay paths come from (in order):
/// 1. Explicit constructor argument (used by tests + programmatic callers)
/// 2. Environment variable (`$ZINCFLOW_CONFIG_LOCAL`, `$ZINCFLOW_SECRETS_PATH`)
/// 3. Sibling of the base config: {@code baseDir/config.local.yaml},
///    {@code baseDir/secrets.yaml}
///
/// A missing file at any layer is not an error — the layer simply
/// contributes an empty map.
///
/// The {@code secrets} layer is read-only legacy: the on-disk write
/// endpoint was retired in favor of environment variables. The read
/// path is kept so existing {@code secrets.yaml} files continue to
/// merge, but no new secrets.yaml files are produced by the worker.
public final class ConfigOverlay {

    private static final Logger log = LoggerFactory.getLogger(ConfigOverlay.class);

    public static final String DEFAULT_LOCAL_NAME = "config.local.yaml";
    public static final String DEFAULT_SECRETS_NAME = "secrets.yaml";
    public static final String ENV_LOCAL = "ZINCFLOW_CONFIG_LOCAL";
    public static final String ENV_SECRETS = "ZINCFLOW_SECRETS_PATH";

    /// A single layer in the overlay stack, with where the bytes came
    /// from, the role (base / local / secrets), and whether the file
    /// was actually present.
    public record Layer(String role, Path path, boolean present, Map<String, Object> content) {
        public Layer {
            content = content == null ? Map.of() : deepCopy(content);
        }
    }

    /// Result of a {@link #load} call: every layer, the merged map,
    /// and per-dot-path provenance.
    public record Resolved(
            Path basePath,
            List<Layer> layers,
            Map<String, Object> effective,
            Map<String, String> provenance) {
        public Resolved {
            layers = List.copyOf(layers);
            effective = deepCopy(effective);
            provenance = Map.copyOf(provenance);
        }
    }

    private ConfigOverlay() {}

    /// Default behaviour — env vars first, sibling files as fallback.
    public static Resolved load(Path basePath) throws IOException {
        return load(basePath, resolveLocalPath(basePath), resolveSecretsPath(basePath));
    }

    /// Explicit paths — used by tests and the admin API's
    /// {@code PUT /api/overlays/secrets} write-through path.
    public static Resolved load(Path basePath, Path localPath, Path secretsPath) throws IOException {
        Layer base    = readLayer("base",    basePath);
        Layer local   = readLayer("local",   localPath);
        Layer secrets = readLayer("secrets", secretsPath);

        Map<String, Object> effective = new LinkedHashMap<>();
        Map<String, String> provenance = new LinkedHashMap<>();
        merge(effective, provenance, base.content(), base.role(), "");
        merge(effective, provenance, local.content(), local.role(), "");
        merge(effective, provenance, secrets.content(), secrets.role(), "");
        return new Resolved(basePath, List.of(base, local, secrets), effective, provenance);
    }

    public static Path resolveLocalPath(Path basePath) {
        String envOverride = System.getenv(ENV_LOCAL);
        if (envOverride != null && !envOverride.isEmpty()) return Path.of(envOverride);
        return basePath == null ? Path.of(DEFAULT_LOCAL_NAME)
                : basePath.toAbsolutePath().getParent().resolve(DEFAULT_LOCAL_NAME);
    }

    public static Path resolveSecretsPath(Path basePath) {
        String envOverride = System.getenv(ENV_SECRETS);
        if (envOverride != null && !envOverride.isEmpty()) return Path.of(envOverride);
        return basePath == null ? Path.of(DEFAULT_SECRETS_NAME)
                : basePath.toAbsolutePath().getParent().resolve(DEFAULT_SECRETS_NAME);
    }

    @SuppressWarnings("unchecked")
    private static Layer readLayer(String role, Path path) throws IOException {
        if (path == null || !Files.isRegularFile(path)) {
            return new Layer(role, path, false, Map.of());
        }
        String yaml = Files.readString(path);
        if (yaml.isBlank()) return new Layer(role, path, true, Map.of());
        Object parsed = new Yaml().load(yaml);
        if (parsed == null) return new Layer(role, path, true, Map.of());
        if (!(parsed instanceof Map<?, ?> raw)) {
            throw new IllegalArgumentException("overlay '" + role + "' (" + path + ") must be a YAML map, got " + parsed.getClass().getSimpleName());
        }
        return new Layer(role, path, true, normalizeKeys((Map<Object, Object>) raw));
    }

    /// Recursive deep-merge: {@code src} onto {@code dst} with
    /// dot-path provenance tracking into {@code provenance}.
    @SuppressWarnings("unchecked")
    private static void merge(Map<String, Object> dst,
                              Map<String, String> provenance,
                              Map<String, Object> src,
                              String layerRole,
                              String parentPath) {
        if (src == null || src.isEmpty()) return;
        for (Map.Entry<String, Object> entry : src.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            String path = parentPath.isEmpty() ? key : parentPath + "." + key;

            Object existing = dst.get(key);
            if (existing instanceof Map<?, ?> existingMap
                    && value instanceof Map<?, ?> incomingMap) {
                Map<String, Object> mergedChild = new LinkedHashMap<>((Map<String, Object>) existingMap);
                merge(mergedChild, provenance, (Map<String, Object>) incomingMap, layerRole, path);
                dst.put(key, mergedChild);
            } else {
                dst.put(key, deepCopyValue(value));
                provenance.put(path, layerRole);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> normalizeKeys(Map<Object, Object> raw) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<Object, Object> entry : raw.entrySet()) {
            Object v = entry.getValue();
            Object normalized = v instanceof Map<?, ?> nested
                    ? normalizeKeys((Map<Object, Object>) nested)
                    : v;
            out.put(String.valueOf(entry.getKey()), normalized);
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> deepCopy(Map<String, Object> src) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : src.entrySet()) {
            out.put(entry.getKey(), deepCopyValue(entry.getValue()));
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private static Object deepCopyValue(Object v) {
        if (v instanceof Map<?, ?> m) return deepCopy((Map<String, Object>) m);
        if (v instanceof List<?> l) {
            List<Object> out = new ArrayList<>(l.size());
            for (Object o : l) out.add(deepCopyValue(o));
            return out;
        }
        return v;
    }

}
