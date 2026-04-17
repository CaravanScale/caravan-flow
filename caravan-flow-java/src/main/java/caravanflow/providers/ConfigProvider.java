package caravanflow.providers;

import caravanflow.core.ComponentState;
import caravanflow.core.Provider;

import java.util.Map;

/// Read-only dot-path access to a configuration map — usually the full
/// config.yaml document. Processors pull typed values through
/// {@code getString/getInt/getBool} helpers; null-safe on missing keys.
public final class ConfigProvider implements Provider {

    public static final String NAME = "config";
    public static final String TYPE = "ConfigProvider";

    private final Map<String, Object> config;
    private volatile ComponentState state = ComponentState.DISABLED;

    public ConfigProvider(Map<String, Object> config) {
        this.config = config == null ? Map.of() : Map.copyOf(config);
    }

    @Override public String name() { return NAME; }
    @Override public String providerType() { return TYPE; }
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

    public static final class Plugin implements caravanflow.core.ProviderPlugin {
        @Override public String providerType() { return TYPE; }
        @Override public String description() { return "Dot-path accessor over the layered config map."; }
        @Override public Provider create(Map<String, Object> config) { return new ConfigProvider(config); }
    }
}
