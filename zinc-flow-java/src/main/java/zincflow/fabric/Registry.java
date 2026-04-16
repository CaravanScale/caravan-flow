package zincflow.fabric;

import zincflow.core.Processor;
import zincflow.processors.ConvertAvroToRecord;
import zincflow.processors.ConvertCSVToRecord;
import zincflow.processors.ConvertJSONToRecord;
import zincflow.processors.ConvertOCFToRecord;
import zincflow.processors.ConvertRecordToAvro;
import zincflow.processors.ConvertRecordToCSV;
import zincflow.processors.ConvertRecordToJSON;
import zincflow.processors.ConvertRecordToOCF;
import zincflow.processors.EvaluateExpression;
import zincflow.processors.ExtractRecordField;
import zincflow.processors.FilterAttribute;
import zincflow.processors.LogAttribute;
import zincflow.processors.PutFile;
import zincflow.processors.PutHTTP;
import zincflow.processors.PutStdout;
import zincflow.processors.QueryRecord;
import zincflow.processors.ReplaceText;
import zincflow.processors.RouteOnAttribute;
import zincflow.processors.SplitText;
import zincflow.processors.TransformRecord;
import zincflow.processors.UpdateAttribute;

import java.time.Duration;
import java.util.List;

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
        // --- CSV ---
        register("ConvertCSVToRecord", cfg -> new ConvertCSVToRecord(
                cfg.getOrDefault("delimiter", ",").charAt(0),
                !"false".equalsIgnoreCase(cfg.getOrDefault("firstRowHeader", "true")),
                splitCSVColumns(cfg.get("columns"))));
        register("ConvertRecordToCSV", cfg -> new ConvertRecordToCSV(
                cfg.getOrDefault("delimiter", ",").charAt(0),
                !"false".equalsIgnoreCase(cfg.getOrDefault("writeHeader", "true")),
                splitCSVColumns(cfg.get("columns"))));
        // --- Avro binary (no container framing) ---
        register("ConvertAvroToRecord", cfg -> new ConvertAvroToRecord(
                required(cfg, "ConvertAvroToRecord", "schema")));
        register("ConvertRecordToAvro", cfg -> new ConvertRecordToAvro(
                required(cfg, "ConvertRecordToAvro", "schema")));
        // --- Avro OCF (Object Container File) ---
        register("ConvertOCFToRecord", cfg -> new ConvertOCFToRecord());
        register("ConvertRecordToOCF", cfg -> new ConvertRecordToOCF(
                required(cfg, "ConvertRecordToOCF", "schema"),
                cfg.getOrDefault("codec", "null")));
        // --- Expression / query (JEXL + JsonPath) ---
        register("EvaluateExpression", cfg -> new EvaluateExpression(
                required(cfg, "EvaluateExpression", "expression"),
                required(cfg, "EvaluateExpression", "targetAttribute")));
        register("TransformRecord", cfg -> new TransformRecord(
                parseTransforms(required(cfg, "TransformRecord", "transforms"))));
        register("QueryRecord", cfg -> new QueryRecord(
                required(cfg, "QueryRecord", "query")));
    }

    /// Parse a compact transforms spec like
    /// {@code "field1=expr1;field2=expr2"} into a map. Used by the
    /// TransformRecord factory so config.yaml can set multiple field
    /// transforms in a single string value (config map is {@code Map<String,String>},
    /// so we can't nest directly).
    private static java.util.Map<String, String> parseTransforms(String spec) {
        java.util.Map<String, String> out = new java.util.LinkedHashMap<>();
        for (String entry : spec.split(";")) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) continue;
            int eq = trimmed.indexOf('=');
            if (eq <= 0) {
                throw new IllegalArgumentException(
                        "TransformRecord: malformed transform '" + trimmed + "' — expected 'field=expression'");
            }
            out.put(trimmed.substring(0, eq).trim(), trimmed.substring(eq + 1).trim());
        }
        if (out.isEmpty()) {
            throw new IllegalArgumentException("TransformRecord: transforms spec produced no entries");
        }
        return out;
    }

    private static List<String> splitCSVColumns(String spec) {
        if (spec == null || spec.isBlank()) return List.of();
        String[] parts = spec.split(",");
        List<String> out = new java.util.ArrayList<>(parts.length);
        for (String p : parts) {
            String trimmed = p.trim();
            if (!trimmed.isEmpty()) out.add(trimmed);
        }
        return List.copyOf(out);
    }

    private static String required(Map<String, String> cfg, String processor, String key) {
        String value = cfg.get(key);
        if (value == null) {
            throw new IllegalArgumentException(processor + ": missing required config key '" + key + "'");
        }
        return value;
    }
}
