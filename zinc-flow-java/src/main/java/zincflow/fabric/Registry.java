package zincflow.fabric;

import zincflow.core.ContentStore;
import zincflow.core.Processor;
import zincflow.core.ProcessorContext;
import zincflow.providers.ContentProvider;
import zincflow.providers.LoggingProvider;
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
import zincflow.processors.ExtractText;
import zincflow.processors.FilterAttribute;
import zincflow.processors.LogAttribute;
import zincflow.processors.PackageFlowFileV3;
import zincflow.processors.PutFile;
import zincflow.processors.PutHTTP;
import zincflow.processors.PutStdout;
import zincflow.processors.QueryRecord;
import zincflow.processors.ReplaceText;
import zincflow.processors.RouteOnAttribute;
import zincflow.processors.SplitText;
import zincflow.processors.TransformRecord;
import zincflow.processors.UnpackageFlowFileV3;
import zincflow.processors.UpdateAttribute;

import java.time.Duration;
import java.util.List;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/// Processor registry — maps the {@code type:} value in config.yaml to
/// a factory that instantiates the processor given its config map plus
/// a {@link ProcessorContext}. Factories that need shared infrastructure
/// (content store, logger) pull it from the context; factories that
/// don't simply ignore the ctx argument.
///
/// Stateless; one instance shared by the Fabric.
public final class Registry {

    /// A factory for a processor given its config map and the surrounding
    /// processor context. The context is never null — callers that don't
    /// care can pass {@code new ProcessorContext()} and ignore it inside
    /// the factory.
    @FunctionalInterface
    public interface Factory {
        Processor create(Map<String, String> config, ProcessorContext ctx);
    }

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

    /// Context-free convenience — callers that don't have a
    /// {@link ProcessorContext} handy (e.g. ad-hoc tests) get an empty
    /// one. Factories that actually consume the context will surface
    /// their own error.
    public Processor create(String type, Map<String, String> config) {
        return create(type, config, new ProcessorContext());
    }

    public Processor create(String type, Map<String, String> config, ProcessorContext ctx) {
        Factory f = factories.get(type);
        if (f == null) {
            throw new IllegalArgumentException("Registry: unknown processor type '" + type + "'");
        }
        return f.create(config == null ? Map.of() : config, ctx == null ? new ProcessorContext() : ctx);
    }

    public java.util.Set<String> types() {
        return Map.copyOf(factories).keySet();
    }

    /// Return the content store exposed by the {@code "content"}
    /// provider, or null when no such provider is wired. Processors that
    /// only handle {@link zincflow.core.RawContent} don't need this;
    /// processors that also accept {@link zincflow.core.ClaimContent}
    /// do — they use it to resolve claims back to bytes.
    private static ContentStore storeFrom(ProcessorContext ctx) {
        ContentProvider cp = ctx.getProviderAs(ContentProvider.DEFAULT_NAME, ContentProvider.class);
        return cp == null ? null : cp.store();
    }

    private static LoggingProvider loggerFrom(ProcessorContext ctx) {
        return ctx.getProviderAs("logging", LoggingProvider.class);
    }

    private void registerBuiltins() {
        register("LogAttribute",     (cfg, ctx) -> new LogAttribute(cfg.getOrDefault("prefix", ""), loggerFrom(ctx)));
        register("UpdateAttribute",  (cfg, ctx) -> new UpdateAttribute(
                required(cfg, "UpdateAttribute", "key"),
                cfg.getOrDefault("value", "")));
        register("RouteOnAttribute", (cfg, ctx) -> new RouteOnAttribute(cfg.getOrDefault("routes", "")));
        register("FilterAttribute",  (cfg, ctx) -> new FilterAttribute(
                required(cfg, "FilterAttribute", "key"),
                cfg.getOrDefault("value", ""),
                !"false".equalsIgnoreCase(cfg.getOrDefault("dropOnMatch", "true"))));
        register("PutStdout",        (cfg, ctx) -> new PutStdout(
                cfg.getOrDefault("prefix", ""),
                cfg.getOrDefault("format", "raw"),
                storeFrom(ctx)));
        register("PutFile",          (cfg, ctx) -> new PutFile(
                required(cfg, "PutFile", "directory"),
                "true".equalsIgnoreCase(cfg.getOrDefault("append", "false")),
                cfg.getOrDefault("format", "raw"),
                storeFrom(ctx)));
        register("ReplaceText",      (cfg, ctx) -> new ReplaceText(
                required(cfg, "ReplaceText", "regex"),
                cfg.getOrDefault("replacement", "")));
        register("SplitText",        (cfg, ctx) -> new SplitText(
                required(cfg, "SplitText", "delimiter"),
                "true".equalsIgnoreCase(cfg.getOrDefault("regex", "false"))));
        register("ExtractText",      (cfg, ctx) -> new ExtractText(
                required(cfg, "ExtractText", "pattern"),
                cfg.getOrDefault("groupNames", ""),
                storeFrom(ctx)));
        register("ConvertJSONToRecord", (cfg, ctx) -> new ConvertJSONToRecord());
        register("ConvertRecordToJSON", (cfg, ctx) -> new ConvertRecordToJSON(
                "true".equalsIgnoreCase(cfg.getOrDefault("singleObject", "false"))));
        register("ExtractRecordField",  (cfg, ctx) -> new ExtractRecordField(
                required(cfg, "ExtractRecordField", "fieldPath"),
                required(cfg, "ExtractRecordField", "attributeName")));
        register("PutHTTP", (cfg, ctx) -> new PutHTTP(
                required(cfg, "PutHTTP", "endpoint"),
                cfg.getOrDefault("method", "POST"),
                Duration.ofSeconds(Long.parseLong(cfg.getOrDefault("timeoutSeconds", "30"))),
                cfg.getOrDefault("contentType", "application/octet-stream"),
                cfg.getOrDefault("format", "raw"),
                storeFrom(ctx)));
        register("PackageFlowFileV3",   (cfg, ctx) -> new PackageFlowFileV3(storeFrom(ctx)));
        register("UnpackageFlowFileV3", (cfg, ctx) -> new UnpackageFlowFileV3(storeFrom(ctx)));
        // --- CSV ---
        register("ConvertCSVToRecord", (cfg, ctx) -> new ConvertCSVToRecord(
                cfg.getOrDefault("delimiter", ",").charAt(0),
                !"false".equalsIgnoreCase(cfg.getOrDefault("firstRowHeader", "true")),
                splitCSVColumns(cfg.get("columns"))));
        register("ConvertRecordToCSV", (cfg, ctx) -> new ConvertRecordToCSV(
                cfg.getOrDefault("delimiter", ",").charAt(0),
                !"false".equalsIgnoreCase(cfg.getOrDefault("writeHeader", "true")),
                splitCSVColumns(cfg.get("columns"))));
        // --- Avro binary (no container framing) ---
        register("ConvertAvroToRecord", (cfg, ctx) -> new ConvertAvroToRecord(
                required(cfg, "ConvertAvroToRecord", "schema")));
        register("ConvertRecordToAvro", (cfg, ctx) -> new ConvertRecordToAvro(
                required(cfg, "ConvertRecordToAvro", "schema")));
        // --- Avro OCF (Object Container File) ---
        register("ConvertOCFToRecord", (cfg, ctx) -> new ConvertOCFToRecord());
        register("ConvertRecordToOCF", (cfg, ctx) -> new ConvertRecordToOCF(
                required(cfg, "ConvertRecordToOCF", "schema"),
                cfg.getOrDefault("codec", "null")));
        // --- Expression / query (JEXL + JsonPath) ---
        register("EvaluateExpression", (cfg, ctx) -> new EvaluateExpression(
                required(cfg, "EvaluateExpression", "expression"),
                required(cfg, "EvaluateExpression", "targetAttribute")));
        register("TransformRecord", (cfg, ctx) -> new TransformRecord(
                parseTransforms(required(cfg, "TransformRecord", "transforms"))));
        register("QueryRecord", (cfg, ctx) -> new QueryRecord(
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
