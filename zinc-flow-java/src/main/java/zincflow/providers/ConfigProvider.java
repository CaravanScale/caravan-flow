package zincflow.providers;

import zincflow.core.ComponentState;
import zincflow.core.Provider;

import java.util.Map;

/// Read-only dot-path access to a configuration map — usually the full
/// config.yaml document. Processors pull typed values through
/// {@code getString/getInt/getBool} helpers; null-safe on missing keys.
public final class ConfigProvider implements Provider {

    private final Map<String, Object> config;
    private volatile ComponentState state = ComponentState.DISABLED;

    public ConfigProvider(Map<String, Object> config) {
        this.config = config == null ? Map.of() : Map.copyOf(config);
    }

    @Override public String name() { return "config"; }
    @Override public String providerType() { return "config"; }
    @Override public ComponentState state() { return state; }
    @Override public void enable() { state = ComponentState.ENABLED; }
    @Override public void disable(int drainTimeoutSeconds) { state = ComponentState.DISABLED; }
    @Override public void shutdown() { state = ComponentState.DISABLED; }

    /// Resolve a dotted path against the nested config map. Returns
    /// null for any missing segment or non-map intermediate value.
    public Object get(String dottedPath) {
        if (dottedPath == null || dottedPath.isEmpty()) return null;
        Object cur = config;
        for (String part : dottedPath.split("\\.")) {
            if (!(cur instanceof Map<?, ?> m)) return null;
            cur = m.get(part);
            if (cur == null) return null;
        }
        return cur;
    }

    public String getString(String path, String defaultValue) {
        Object v = get(path);
        return v == null ? defaultValue : String.valueOf(v);
    }

    public int getInt(String path, int defaultValue) {
        Object v = get(path);
        if (v instanceof Number n) return n.intValue();
        if (v == null) return defaultValue;
        try { return Integer.parseInt(String.valueOf(v)); }
        catch (NumberFormatException e) { return defaultValue; }
    }

    public boolean getBool(String path, boolean defaultValue) {
        Object v = get(path);
        if (v instanceof Boolean b) return b;
        if (v == null) return defaultValue;
        return Boolean.parseBoolean(String.valueOf(v));
    }
}
