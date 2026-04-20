package caravanflow.fabric;

import caravanflow.core.ContentStore;
import caravanflow.core.Processor;
import caravanflow.core.ProcessorContext;
import caravanflow.providers.ContentProvider;
import caravanflow.providers.LoggingProvider;
import caravanflow.processors.ConvertAvroToRecord;
import caravanflow.processors.ConvertCSVToRecord;
import caravanflow.processors.ConvertJSONToRecord;
import caravanflow.processors.ConvertOCFToRecord;
import caravanflow.processors.ConvertRecordToAvro;
import caravanflow.processors.ConvertRecordToCSV;
import caravanflow.processors.ConvertRecordToJSON;
import caravanflow.processors.ConvertRecordToOCF;
import caravanflow.processors.EvaluateExpression;
import caravanflow.processors.ExtractRecordField;
import caravanflow.processors.ExtractText;
import caravanflow.processors.FilterAttribute;
import caravanflow.processors.LogAttribute;
import caravanflow.processors.PackageFlowFileV3;
import caravanflow.processors.PutFile;
import caravanflow.processors.PutHTTP;
import caravanflow.processors.PutStdout;
import caravanflow.processors.QueryRecord;
import caravanflow.processors.ReplaceText;
import caravanflow.processors.RouteOnAttribute;
import caravanflow.processors.RouteRecord;
import caravanflow.processors.SplitRecord;
import caravanflow.processors.SplitText;
import caravanflow.processors.TransformRecord;
import caravanflow.processors.UnpackageFlowFileV3;
import caravanflow.processors.UpdateAttribute;
import caravanflow.processors.UpdateRecord;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/// Processor registry — maps the {@code type:} value in config.yaml to
/// a factory that instantiates the processor given its config map plus
/// a {@link ProcessorContext}. Factories that need shared infrastructure
/// (content store, logger) pull it from the context; factories that
/// don't simply ignore the ctx argument.
///
/// <h2>Versioning</h2>
/// Every factory is registered with both a {@code name} and a
/// {@code version}. Config can pin a specific version via
/// {@code type: Foo@1.2.0}; omitting the version picks the latest
/// registered version for that name. Default version is
/// {@value #DEFAULT_VERSION}.
///
/// Stateless; one instance shared by the Fabric.
public final class Registry {

    /** @deprecated use {@link TypeRefs#DEFAULT_VERSION}. */
    @Deprecated
    public static final String DEFAULT_VERSION = TypeRefs.DEFAULT_VERSION;

    /// A factory for a processor given its config map and the surrounding
    /// processor context. The context is never null — callers that don't
    /// care can pass {@code new ProcessorContext()} and ignore it inside
    /// the factory.
    @FunctionalInterface
    public interface Factory {
        Processor create(Map<String, String> config, ProcessorContext ctx);
    }

    /// Metadata about a registered processor type. The UI renders the
    /// typed {@link #parameters} list in the "add processor" form; older
    /// clients fall back to {@link #configKeys}. Category groups the
    /// dropdown visually.
    public record TypeInfo(String name, String version, String description,
                           List<String> configKeys, List<String> relationships,
                           String category, List<ParamInfo> parameters) {
        public TypeInfo {
            configKeys = List.copyOf(configKeys);
            relationships = List.copyOf(relationships);
            if (category == null || category.isEmpty()) category = "Other";
            parameters = parameters == null ? List.of() : List.copyOf(parameters);
        }

        /// Legacy constructor — pre-ParamInfo callers (third-party plugins)
        /// still compile. configKeys become String-kind ParamInfo entries;
        /// category defaults to "Other".
        public TypeInfo(String name, String version, String description,
                        List<String> configKeys, List<String> relationships) {
            this(name, version, description, configKeys, relationships, "Other",
                 configKeys.stream()
                         .map(k -> ParamInfo.of(k).build())
                         .toList());
        }

        public String qualifiedName() { return name + "@" + version; }
    }

    /// Keyed by "name@version" for pinned lookup.
    private final ConcurrentHashMap<String, Factory> versioned = new ConcurrentHashMap<>();
    /// Keyed by "name" → latest version string; lets unqualified
    /// lookups resolve to the most recent release.
    private final ConcurrentHashMap<String, String> latestVersion = new ConcurrentHashMap<>();
    /// Metadata mirror of {@link #versioned}, keyed the same way.
    private final ConcurrentHashMap<String, TypeInfo> metadata = new ConcurrentHashMap<>();

    public Registry() {
        registerBuiltins();
    }

    /// Unversioned register — convenience for third-party plugins
    /// that don't care about versioning. Defaults to
    /// {@value #DEFAULT_VERSION}.
    public void register(String type, Factory factory) {
        register(type, DEFAULT_VERSION, factory);
    }

    public void register(String type, String version, Factory factory) {
        register(new TypeInfo(type, version, "", List.of(), List.of()), factory);
    }

    /// Full register — caller supplies the metadata the UI needs.
    public void register(TypeInfo info, Factory factory) {
        String key = info.qualifiedName();
        versioned.put(key, factory);
        metadata.put(key, info);
        latestVersion.merge(info.name(), info.version(),
                (oldV, newV) -> TypeRefs.compareVersions(oldV, newV) >= 0 ? oldV : newV);
    }

    public boolean has(String type) {
        return resolveKey(type) != null;
    }

    /// Resolve a user-facing type string (either {@code "Foo"} or
    /// {@code "Foo@1.2.3"}) to the internal map key, or null when
    /// the type is unknown.
    private String resolveKey(String type) {
        if (type == null || type.isEmpty()) return null;
        if (type.contains("@")) {
            return versioned.containsKey(type) ? type : null;
        }
        String latest = latestVersion.get(type);
        if (latest == null) return null;
        return type + "@" + latest;
    }

    /** @deprecated use {@link TypeRefs#compareVersions(String, String)}. */
    @Deprecated
    public static int compareVersions(String a, String b) {
        return TypeRefs.compareVersions(a, b);
    }

    /// Context-free convenience — callers that don't have a
    /// {@link ProcessorContext} handy (e.g. ad-hoc tests) get an empty
    /// one. Factories that actually consume the context will surface
    /// their own error.
    public Processor create(String type, Map<String, String> config) {
        return create(type, config, new ProcessorContext());
    }

    public Processor create(String type, Map<String, String> config, ProcessorContext ctx) {
        String key = resolveKey(type);
        if (key == null) {
            throw new IllegalArgumentException("Registry: unknown processor type '" + type + "'");
        }
        Factory f = versioned.get(key);
        return f.create(config == null ? Map.of() : config, ctx == null ? new ProcessorContext() : ctx);
    }

    /// All registered unqualified type names (latest only per name).
    public java.util.Set<String> types() {
        return java.util.Set.copyOf(latestVersion.keySet());
    }

    /// Every registered (type, version) pair, sorted by name then
    /// ascending version, with metadata. Used by
    /// {@code GET /api/processor-types}.
    public List<TypeInfo> listAll() {
        List<TypeInfo> out = new ArrayList<>(metadata.values());
        out.sort(Comparator.comparing(TypeInfo::name).thenComparing(
                TypeInfo::version, TypeRefs::compareVersions));
        return out;
    }

    /// Every version registered under a given unqualified name, or
    /// empty when the name is unknown.
    public List<TypeInfo> listVersions(String type) {
        List<TypeInfo> out = new ArrayList<>();
        for (TypeInfo info : metadata.values()) {
            if (info.name().equals(type)) out.add(info);
        }
        out.sort(Comparator.comparing(TypeInfo::version, TypeRefs::compareVersions));
        return out;
    }

    /// Latest {@link TypeInfo} for a name, null if unknown.
    public TypeInfo latest(String type) {
        String latest = latestVersion.get(type);
        if (latest == null) return null;
        return metadata.get(type + "@" + latest);
    }

    /** @deprecated use {@link TypeRefs.TypeRef}. */
    @Deprecated
    public static TypeRefs.TypeRef parseTypeRef(String raw) {
        return TypeRefs.TypeRef.parse(raw);
    }

    /// Return the content store exposed by the {@code "content"}
    /// provider, or null when no such provider is wired. Processors that
    /// only handle {@link caravanflow.core.RawContent} don't need this;
    /// processors that also accept {@link caravanflow.core.ClaimContent}
    /// do — they use it to resolve claims back to bytes.
    private static ContentStore storeFrom(ProcessorContext ctx) {
        ContentProvider cp = ctx.getProviderAs(ContentProvider.NAME, ContentProvider.class);
        return cp == null ? null : cp.store();
    }

    private static LoggingProvider loggerFrom(ProcessorContext ctx) {
        return ctx.getProviderAs(LoggingProvider.NAME, LoggingProvider.class);
    }

    /// Rich register — caller supplies a built TypeInfo (with category +
    /// typed ParamInfo). Version defaults to {@value TypeRefs#DEFAULT_VERSION}.
    private void registerTyped(String name, String description, String category,
                               List<ParamInfo> params, List<String> relationships,
                               Factory factory) {
        var keys = params.stream().map(ParamInfo::name).toList();
        register(new TypeInfo(name, TypeRefs.DEFAULT_VERSION, description, keys,
                              relationships, category, params), factory);
    }

    private void registerBuiltins() {
        // --- Attribute ---
        registerTyped("LogAttribute", "Logs FlowFile attributes and passes through", "Attribute",
                List.of(ParamInfo.of("prefix").description("Log line prefix").defaultValue("").build()),
                List.of("success"),
                (cfg, ctx) -> new LogAttribute(cfg.getOrDefault("prefix", ""), loggerFrom(ctx)));

        registerTyped("UpdateAttribute", "Sets key=value attribute on FlowFiles", "Attribute",
                List.of(
                        ParamInfo.of("key").required().placeholder("env").build(),
                        ParamInfo.of("value").required().placeholder("prod").build()),
                List.of("success"),
                (cfg, ctx) -> new UpdateAttribute(
                        required(cfg, "UpdateAttribute", "key"),
                        cfg.getOrDefault("value", "")));

        registerTyped("RouteOnAttribute", "Route FlowFiles based on attribute predicates", "Routing",
                List.of(ParamInfo.of("routes").kind(ParamKind.MULTILINE).required()
                        .placeholder("premium: tier EQ premium; bulk: tier EQ bulk")
                        .description("semicolon-delimited 'name: attr OP value' entries; operators: EQ, NEQ, CONTAINS, STARTSWITH, ENDSWITH, EXISTS, GT, LT")
                        .build()),
                List.of("unmatched"),
                (cfg, ctx) -> new RouteOnAttribute(cfg.getOrDefault("routes", "")));

        registerTyped("FilterAttribute", "Remove or keep specific attributes", "Attribute",
                List.of(
                        ParamInfo.of("mode").kind(ParamKind.ENUM).required().defaultValue("remove")
                                .choices("remove", "keep")
                                .description("remove = drop listed attributes; keep = drop everything else").build(),
                        ParamInfo.of("attributes").kind(ParamKind.STRING_LIST).required()
                                .placeholder("http.headers;internal.tmp")
                                .description("attribute names to remove or keep").build()),
                List.of("success"),
                (cfg, ctx) -> new FilterAttribute(
                        cfg.getOrDefault("mode", "remove"),
                        cfg.getOrDefault("attributes", "")));

        // --- Sink ---
        registerTyped("PutStdout", "Write FlowFile content to stdout", "Sink",
                List.of(
                        ParamInfo.of("prefix").defaultValue("").build(),
                        ParamInfo.of("format").kind(ParamKind.ENUM).defaultValue("raw")
                                .choices("raw", "v3").build()),
                List.of("success"),
                (cfg, ctx) -> new PutStdout(
                        cfg.getOrDefault("prefix", ""),
                        cfg.getOrDefault("format", "raw"),
                        storeFrom(ctx)));

        registerTyped("PutFile", "Write FlowFile content to directory", "Sink",
                List.of(
                        ParamInfo.of("directory").required().placeholder("/var/lib/caravan/out").build(),
                        ParamInfo.of("append").kind(ParamKind.BOOLEAN).defaultValue("false").build(),
                        ParamInfo.of("format").kind(ParamKind.ENUM).defaultValue("raw")
                                .choices("raw", "v3").build()),
                List.of("success"),
                (cfg, ctx) -> new PutFile(
                        required(cfg, "PutFile", "directory"),
                        "true".equalsIgnoreCase(cfg.getOrDefault("append", "false")),
                        cfg.getOrDefault("format", "raw"),
                        storeFrom(ctx)));

        registerTyped("PutHTTP", "POST FlowFile to downstream HTTP endpoint", "Sink",
                List.of(
                        ParamInfo.of("endpoint").required().placeholder("http://localhost:8080/ingest").build(),
                        ParamInfo.of("method").kind(ParamKind.ENUM).defaultValue("POST")
                                .choices("POST", "PUT").build(),
                        ParamInfo.of("timeoutSeconds").kind(ParamKind.INTEGER).defaultValue("30").build(),
                        ParamInfo.of("contentType").defaultValue("application/octet-stream").build(),
                        ParamInfo.of("format").kind(ParamKind.ENUM).defaultValue("raw")
                                .choices("raw", "v3").build()),
                List.of("success", "failure"),
                (cfg, ctx) -> new PutHTTP(
                        required(cfg, "PutHTTP", "endpoint"),
                        cfg.getOrDefault("method", "POST"),
                        Duration.ofSeconds(Long.parseLong(cfg.getOrDefault("timeoutSeconds", "30"))),
                        cfg.getOrDefault("contentType", "application/octet-stream"),
                        cfg.getOrDefault("format", "raw"),
                        storeFrom(ctx)));

        // --- V3 framing ---
        registerTyped("PackageFlowFileV3", "Wrap (attributes + content) into NiFi V3 binary content", "V3",
                List.of(), List.of("success"),
                (cfg, ctx) -> new PackageFlowFileV3(storeFrom(ctx)));

        registerTyped("UnpackageFlowFileV3", "Decode V3 binary content into one or more FlowFiles", "V3",
                List.of(), List.of("success"),
                (cfg, ctx) -> new UnpackageFlowFileV3(storeFrom(ctx)));

        // --- Text ---
        registerTyped("ReplaceText", "Regex find/replace on content", "Text",
                List.of(
                        ParamInfo.of("pattern").required().placeholder("\\berror\\b").build(),
                        ParamInfo.of("replacement").defaultValue("").build(),
                        ParamInfo.of("mode").kind(ParamKind.ENUM).defaultValue("all")
                                .choices("all", "first").build()),
                List.of("success"),
                (cfg, ctx) -> new ReplaceText(
                        required(cfg, "ReplaceText", "pattern"),
                        cfg.getOrDefault("replacement", ""),
                        cfg.getOrDefault("mode", "all")));

        registerTyped("SplitText", "Split content by delimiter into multiple FlowFiles", "Text",
                List.of(
                        ParamInfo.of("delimiter").required().placeholder("\\n\\n").build(),
                        ParamInfo.of("headerLines").kind(ParamKind.INTEGER).defaultValue("0").build()),
                List.of("success"),
                (cfg, ctx) -> new SplitText(
                        required(cfg, "SplitText", "delimiter"),
                        parseInt(cfg.getOrDefault("headerLines", "0"), 0)));

        registerTyped("ExtractText", "Regex capture groups → attributes", "Text",
                List.of(
                        ParamInfo.of("pattern").required().placeholder("(?<user>\\w+)@(?<host>\\w+)").build(),
                        ParamInfo.of("groupNames").placeholder("user,host")
                                .description("comma-separated names for positional groups").build()),
                List.of("success"),
                (cfg, ctx) -> new ExtractText(
                        required(cfg, "ExtractText", "pattern"),
                        cfg.getOrDefault("groupNames", ""),
                        storeFrom(ctx)));

        // --- Conversion ---
        registerTyped("ConvertJSONToRecord", "Parses JSON content into records", "Conversion",
                List.of(ParamInfo.of("schemaName").defaultValue("").build()),
                List.of("success"),
                (cfg, ctx) -> new ConvertJSONToRecord(cfg.getOrDefault("schemaName", "")));

        registerTyped("ConvertRecordToJSON", "Serializes records back to JSON", "Conversion",
                List.of(ParamInfo.of("singleObject").kind(ParamKind.BOOLEAN).defaultValue("false").build()),
                List.of("success"),
                (cfg, ctx) -> new ConvertRecordToJSON(
                        "true".equalsIgnoreCase(cfg.getOrDefault("singleObject", "false"))));

        registerTyped("ConvertCSVToRecord", "Parse CSV content into records", "Conversion",
                List.of(
                        ParamInfo.of("schemaName").defaultValue("").build(),
                        ParamInfo.of("delimiter").defaultValue(",").build(),
                        ParamInfo.of("hasHeader").kind(ParamKind.BOOLEAN).defaultValue("true").build(),
                        ParamInfo.of("fields").placeholder("id:long,name:string").build()),
                List.of("success"),
                (cfg, ctx) -> new ConvertCSVToRecord(
                        cfg.getOrDefault("schemaName", ""),
                        cfg.getOrDefault("delimiter", ",").charAt(0),
                        !"false".equalsIgnoreCase(cfg.getOrDefault("hasHeader", "true")),
                        cfg.getOrDefault("fields", "")));

        registerTyped("ConvertRecordToCSV", "Serialize records to CSV", "Conversion",
                List.of(
                        ParamInfo.of("delimiter").defaultValue(",").build(),
                        ParamInfo.of("includeHeader").kind(ParamKind.BOOLEAN).defaultValue("true").build()),
                List.of("success"),
                (cfg, ctx) -> new ConvertRecordToCSV(
                        cfg.getOrDefault("delimiter", ",").charAt(0),
                        !"false".equalsIgnoreCase(cfg.getOrDefault("includeHeader", "true"))));

        registerTyped("ConvertAvroToRecord", "Decode Avro binary into records", "Conversion",
                List.of(
                        ParamInfo.of("schemaName").defaultValue("").build(),
                        ParamInfo.of("fields").placeholder("id:long,name:string,amount:double")
                                .description("comma-separated name:type pairs").build()),
                List.of("success"),
                (cfg, ctx) -> new ConvertAvroToRecord(
                        cfg.getOrDefault("schemaName", ""),
                        cfg.getOrDefault("fields", "")));

        registerTyped("ConvertRecordToAvro", "Encode records to Avro binary", "Conversion",
                List.of(), List.of("success"),
                (cfg, ctx) -> new ConvertRecordToAvro());

        registerTyped("ConvertOCFToRecord", "Decode Avro OCF (.avro file) into records", "Conversion",
                List.of(), List.of("success"),
                (cfg, ctx) -> new ConvertOCFToRecord());

        registerTyped("ConvertRecordToOCF", "Encode records to Avro OCF (.avro file)", "Conversion",
                List.of(ParamInfo.of("codec").kind(ParamKind.ENUM).defaultValue("null")
                        .choices("null", "deflate", "snappy", "bzip2", "xz", "zstandard").build()),
                List.of("success"),
                (cfg, ctx) -> new ConvertRecordToOCF(cfg.getOrDefault("codec", "null")));

        // --- Transform ---
        registerTyped("EvaluateExpression", "Compute attributes from expressions", "Transform",
                List.of(ParamInfo.of("expressions").kind(ParamKind.KEY_VALUE_LIST).required()
                        .valueKind(ParamKind.EXPRESSION).entryDelim(";").pairDelim("=")
                        .placeholder("tax=amount*0.07; label=upper(region)")
                        .description("attr=expression pairs").build()),
                List.of("success"),
                (cfg, ctx) -> new EvaluateExpression(
                        parseTransforms(required(cfg, "EvaluateExpression", "expressions"))));

        registerTyped("TransformRecord", "Field-level operations on records", "Transform",
                List.of(ParamInfo.of("operations").kind(ParamKind.MULTILINE).required()
                        .placeholder("rename:oldName:newName; remove:badField; compute:total:amount*1.07")
                        .description("semicolon-delimited op:arg1[:arg2] directives").build()),
                List.of("success"),
                (cfg, ctx) -> new TransformRecord(
                        required(cfg, "TransformRecord", "operations")));

        registerTyped("UpdateRecord", "Set or derive record fields via expressions", "Transform",
                List.of(ParamInfo.of("updates").kind(ParamKind.KEY_VALUE_LIST).required()
                        .valueKind(ParamKind.EXPRESSION).entryDelim(";").pairDelim("=")
                        .placeholder("tax=amount*0.07; total=amount+tax")
                        .description("field=expression pairs; later pairs see earlier writes").build()),
                List.of("success"),
                (cfg, ctx) -> new UpdateRecord(cfg.getOrDefault("updates", "")));

        // --- Record ---
        registerTyped("ExtractRecordField", "Extract record fields into FlowFile attributes", "Record",
                List.of(
                        ParamInfo.of("fields").kind(ParamKind.KEY_VALUE_LIST).required()
                                .entryDelim(";").pairDelim(":")
                                .placeholder("amount:order.amount;region:tenant.region")
                                .description("fieldPath:attrName pairs").build(),
                        ParamInfo.of("recordIndex").kind(ParamKind.INTEGER).defaultValue("0").build()),
                List.of("success"),
                (cfg, ctx) -> new ExtractRecordField(
                        required(cfg, "ExtractRecordField", "fields"),
                        parseInt(cfg.getOrDefault("recordIndex", "0"), 0)));

        registerTyped("QueryRecord", "Filter records using a JsonPath query", "Record",
                List.of(ParamInfo.of("query").kind(ParamKind.EXPRESSION).required()
                        .placeholder("$[?(@.amount > 100)]")
                        .description("JsonPath filter against the record batch").build()),
                List.of("success"),
                (cfg, ctx) -> new QueryRecord(required(cfg, "QueryRecord", "query")));

        registerTyped("SplitRecord", "Fan out a RecordContent FlowFile into one FlowFile per record", "Record",
                List.of(), List.of("success"),
                (cfg, ctx) -> new SplitRecord());

        // --- Routing (record-level) ---
        registerTyped("RouteRecord",
                "Partition records across routes via per-route expression predicates", "Routing",
                List.of(ParamInfo.of("routes").kind(ParamKind.KEY_VALUE_LIST).required()
                        .valueKind(ParamKind.EXPRESSION).entryDelim(";").pairDelim(":")
                        .placeholder("premium: tier == \"gold\"; minors: age < 18")
                        .description("name:expression pairs; first-match wins; non-matching records go to 'unmatched'")
                        .build()),
                List.of("unmatched"),
                (cfg, ctx) -> new RouteRecord(cfg.getOrDefault("routes", "")));
    }

    /// Parse a compact key=value spec like
    /// {@code "field1=expr1;field2=expr2"} into a map. Used by the
    /// TransformRecord and EvaluateExpression factories so config.yaml
    /// can express multi-target settings in one string (the config map
    /// is {@code Map<String,String>}, can't nest directly).
    private static java.util.Map<String, String> parseTransforms(String spec) {
        java.util.Map<String, String> out = new java.util.LinkedHashMap<>();
        for (String entry : spec.split(";")) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) continue;
            int eq = trimmed.indexOf('=');
            if (eq <= 0) {
                throw new IllegalArgumentException(
                        "malformed entry '" + trimmed + "' — expected 'target=expression'");
            }
            out.put(trimmed.substring(0, eq).trim(), trimmed.substring(eq + 1).trim());
        }
        if (out.isEmpty()) {
            throw new IllegalArgumentException("spec produced no entries");
        }
        return out;
    }

    private static int parseInt(String s, int fallback) {
        if (s == null || s.isBlank()) return fallback;
        try { return Integer.parseInt(s.trim()); }
        catch (NumberFormatException e) { return fallback; }
    }

    private static String required(Map<String, String> cfg, String processor, String key) {
        String value = cfg.get(key);
        if (value == null) {
            throw new IllegalArgumentException(processor + ": missing required config key '" + key + "'");
        }
        return value;
    }
}
