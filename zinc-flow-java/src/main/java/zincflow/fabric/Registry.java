package zincflow.fabric;

import zincflow.core.Processor;
import zincflow.processors.ConvertJSONToRecord;
import zincflow.processors.ConvertRecordToJSON;
import zincflow.processors.ExtractRecordField;
import zincflow.processors.FilterAttribute;
import zincflow.processors.LogAttribute;
import zincflow.processors.PutFile;
import zincflow.processors.PutHTTP;
import zincflow.processors.PutStdout;
import zincflow.processors.ReplaceText;
import zincflow.processors.RouteOnAttribute;
import zincflow.processors.SplitText;
import zincflow.processors.UpdateAttribute;

import java.time.Duration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/// Processor registry — maps the {@code type:} value in config.yaml to
/// a factory that instantiates the processor given its config map.
/// Stateless; one instance shared by the Fabric.
public final class Registry {

    /// A factory for a processor given its config map.
    @FunctionalInterface
    public interface Factory extends Function<Map<String, String>, Processor> { }

    private final ConcurrentHashMap<String, Factory> factories = new ConcurrentHashMap<>();

    public Registry() {
        registerBuiltins();
    }

    public void register(String type, Factory factory) {
        factories.put(type, factory);
    }

    public boolean has(String type) {
        return factories.containsKey(type);
    }

    public Processor create(String type, Map<String, String> config) {
        Factory f = factories.get(type);
        if (f == null) {
            throw new IllegalArgumentException("Registry: unknown processor type '" + type + "'");
        }
        return f.apply(config == null ? Map.of() : config);
    }

    public java.util.Set<String> types() {
        return Map.copyOf(factories).keySet();
    }

    private void registerBuiltins() {
        register("LogAttribute",     cfg -> new LogAttribute(cfg.getOrDefault("prefix", "")));
        register("UpdateAttribute",  cfg -> new UpdateAttribute(
                required(cfg, "UpdateAttribute", "key"),
                cfg.getOrDefault("value", "")));
        register("RouteOnAttribute", cfg -> new RouteOnAttribute(cfg.getOrDefault("routes", "")));
        register("FilterAttribute",  cfg -> new FilterAttribute(
                required(cfg, "FilterAttribute", "key"),
                cfg.getOrDefault("value", ""),
                !"false".equalsIgnoreCase(cfg.getOrDefault("dropOnMatch", "true"))));
        register("PutStdout",        cfg -> new PutStdout(cfg.getOrDefault("prefix", "")));
        register("PutFile",          cfg -> new PutFile(
                required(cfg, "PutFile", "directory"),
                "true".equalsIgnoreCase(cfg.getOrDefault("append", "false"))));
        register("ReplaceText",      cfg -> new ReplaceText(
                required(cfg, "ReplaceText", "regex"),
                cfg.getOrDefault("replacement", "")));
        register("SplitText",        cfg -> new SplitText(
                required(cfg, "SplitText", "delimiter"),
                "true".equalsIgnoreCase(cfg.getOrDefault("regex", "false"))));
        register("ConvertJSONToRecord", cfg -> new ConvertJSONToRecord());
        register("ConvertRecordToJSON", cfg -> new ConvertRecordToJSON(
                "true".equalsIgnoreCase(cfg.getOrDefault("singleObject", "false"))));
        register("ExtractRecordField",  cfg -> new ExtractRecordField(
                required(cfg, "ExtractRecordField", "fieldPath"),
                required(cfg, "ExtractRecordField", "attributeName")));
        register("PutHTTP", cfg -> new PutHTTP(
                required(cfg, "PutHTTP", "endpoint"),
                cfg.getOrDefault("method", "POST"),
                Duration.ofSeconds(Long.parseLong(cfg.getOrDefault("timeoutSeconds", "30"))),
                cfg.getOrDefault("contentType", "application/octet-stream")));
    }

    private static String required(Map<String, String> cfg, String processor, String key) {
        String value = cfg.get(key);
        if (value == null) {
            throw new IllegalArgumentException(processor + ": missing required config key '" + key + "'");
        }
        return value;
    }
}
