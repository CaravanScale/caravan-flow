using CaravanFlow.Core;
using CaravanFlow.StdLib;

namespace CaravanFlow.Fabric;

/// <summary>
/// Registers all built-in processors from StdLib with the registry.
/// </summary>
public static class BuiltinProcessors
{
    // Concise ParamInfo builder for the registration block. Defaults keep the
    // common "simple string param" call short; pass named args for the rest.
    private static ParamInfo P(string name, ParamKind kind = ParamKind.String,
        bool required = false, string? def = null, string? placeholder = null,
        string? description = null, string[]? choices = null,
        ParamKind? valueKind = null, string entry = ";", string pair = "=")
        => new()
        {
            Name = name,
            Label = name,
            Kind = kind,
            Required = required,
            Default = def,
            Placeholder = placeholder,
            Description = description ?? "",
            Choices = choices,
            ValueKind = valueKind,
            EntryDelim = entry,
            PairDelim = pair,
        };

    public static void RegisterAll(Registry reg)
    {
        // --- Attribute ---

        reg.Register(
            new ProcessorInfo("UpdateAttribute", "Sets key=value attribute on FlowFiles", "Attribute",
                [P("key", required: true, placeholder: "env"),
                 P("value", required: true, placeholder: "prod")]),
            (ctx, config) => new UpdateAttribute(
                ConfigHelpers.RequireString(config, "key"),
                ConfigHelpers.RequireString(config, "value")));

        reg.Register(
            new ProcessorInfo("LogAttribute", "Logs FlowFile attributes and passes through", "Attribute",
                [P("prefix", def: "flow", description: "Log line prefix")]),
            (ctx, config) =>
            {
                ctx.TryGetProvider<LoggingProvider>("logging", out var log);
                return new LogAttribute(config.GetValueOrDefault("prefix", "flow"), log);
            });

        reg.Register(
            new ProcessorInfo("FilterAttribute", "Remove or keep specific attributes", "Attribute",
                [P("mode", kind: ParamKind.Enum, required: true, def: "remove",
                   choices: ["remove", "keep"],
                   description: "remove = drop listed attributes; keep = drop everything else"),
                 P("attributes", kind: ParamKind.StringList, required: true,
                   placeholder: "http.headers;internal.tmp",
                   description: "attribute names to remove or keep")]),
            (ctx, config) => new FilterAttribute(
                config.GetValueOrDefault("mode", "remove"),
                config.GetValueOrDefault("attributes", "")));

        // --- Sink ---

        reg.Register(
            new ProcessorInfo("PutHTTP", "POST FlowFile to downstream HTTP endpoint", "Sink",
                [P("endpoint", required: true, placeholder: "http://localhost:8080/ingest"),
                 P("format", kind: ParamKind.Enum, def: "raw", choices: ["raw", "v3"],
                   description: "raw = content bytes; v3 = NiFi V3 framing (preserves attrs)")]),
            (ctx, config) => new PutHTTP(
                ConfigHelpers.RequireString(config, "endpoint"),
                ConfigHelpers.GetString(config, "format", "raw"),
                ctx.GetContentStoreOrDefault()));

        reg.Register(
            new ProcessorInfo("PutFile", "Write FlowFile content to directory", "Sink",
                [P("outputDir", required: true, placeholder: "/var/lib/caravan/out"),
                 P("namingAttribute", def: "filename",
                   description: "FlowFile attribute that supplies the output filename"),
                 P("prefix", def: ""),
                 P("suffix", def: ".dat"),
                 P("format", kind: ParamKind.Enum, def: "raw", choices: ["raw", "v3"])]),
            (ctx, config) => new PutFile(
                ConfigHelpers.RequireString(config, "outputDir"),
                ConfigHelpers.GetString(config, "namingAttribute", "filename"),
                ConfigHelpers.GetString(config, "prefix"),
                ConfigHelpers.GetString(config, "suffix"),
                ctx.GetContentStoreOrDefault(),
                ConfigHelpers.GetString(config, "format", "raw")));

        reg.Register(
            new ProcessorInfo("PutStdout", "Write FlowFile content to stdout", "Sink",
                [P("format", kind: ParamKind.Enum, required: true, def: "text",
                   choices: ["attrs", "raw", "text", "v3", "hex"])]),
            (ctx, config) => new PutStdout(
                ConfigHelpers.RequireOneOf(config, "format", "text",
                    new[] { "attrs", "raw", "text", "v3", "hex" }),
                ctx.GetContentStoreOrDefault()));

        // --- V3 framing ---

        reg.Register(
            new ProcessorInfo("PackageFlowFileV3", "Wrap (attributes + content) into NiFi V3 binary content", "V3",
                []),
            (ctx, config) => new PackageFlowFileV3(ctx.GetContentStoreOrDefault()));

        reg.Register(
            new ProcessorInfo("UnpackageFlowFileV3", "Decode V3 binary content into one or more FlowFiles", "V3",
                []),
            (ctx, config) => new UnpackageFlowFileV3(ctx.GetContentStoreOrDefault()));

        // --- Text ---

        reg.Register(
            new ProcessorInfo("ReplaceText", "Regex find/replace on content", "Text",
                [P("pattern", required: true, placeholder: @"\berror\b", description: "regex to match"),
                 P("replacement", def: ""),
                 P("mode", kind: ParamKind.Enum, def: "all", choices: ["all", "first"])]),
            (ctx, config) => new ReplaceText(
                ConfigHelpers.RequireString(config, "pattern"),
                ConfigHelpers.GetString(config, "replacement"),
                ConfigHelpers.GetString(config, "mode", "all"),
                ctx.GetContentStoreOrDefault()));

        reg.Register(
            new ProcessorInfo("ExtractText", "Regex capture groups → attributes", "Text",
                [P("pattern", required: true, placeholder: @"(?<user>\w+)@(?<host>\w+)"),
                 P("groupNames", placeholder: "user,host",
                   description: "comma-separated names for positional groups")]),
            (ctx, config) => new ExtractText(
                ConfigHelpers.RequireString(config, "pattern"),
                ConfigHelpers.GetString(config, "groupNames"),
                ctx.GetContentStoreOrDefault()));

        reg.Register(
            new ProcessorInfo("SplitText", "Split content by delimiter into multiple FlowFiles", "Text",
                [P("delimiter", required: true, placeholder: @"\n\n"),
                 P("headerLines", kind: ParamKind.Integer, def: "0")]),
            (ctx, config) =>
            {
                var delim = ConfigHelpers.RequireString(config, "delimiter");
                var headerLines = ConfigHelpers.ParseInt(config.GetValueOrDefault("headerLines"), "headerLines", 0);
                return new SplitText(delim, headerLines, ctx.GetContentStoreOrDefault());
            });

        // --- Conversion ---

        reg.Register(
            new ProcessorInfo("ConvertJSONToRecord", "Parses JSON content into records", "Conversion",
                [P("schemaName", def: "default", description: "schema name for the inferred record type")]),
            (ctx, config) => new ConvertJSONToRecord(
                config.GetValueOrDefault("schemaName", "default"),
                ctx.GetContentStoreOrDefault()));

        reg.Register(
            new ProcessorInfo("ConvertRecordToJSON", "Serializes records back to JSON", "Conversion",
                []),
            (ctx, config) => new ConvertRecordToJSON());

        reg.Register(
            new ProcessorInfo("ConvertAvroToRecord", "Decode Avro binary into records", "Conversion",
                [P("fields", placeholder: "id:long,name:string,amount:double",
                   description: "comma-separated name:type pairs describing the record schema"),
                 P("schemaName", def: "default")]),
            (ctx, config) => new ConvertAvroToRecord(
                config.GetValueOrDefault("schemaName", "default"),
                config.GetValueOrDefault("fields", ""),
                ctx.GetContentStoreOrDefault()));

        reg.Register(
            new ProcessorInfo("ConvertRecordToAvro", "Encode records to Avro binary", "Conversion", []),
            (ctx, config) => new ConvertRecordToAvro());

        reg.Register(
            new ProcessorInfo("ConvertOCFToRecord", "Decode Avro OCF (.avro file) into records", "Conversion",
                [P("readerSchema", kind: ParamKind.Multiline,
                   placeholder: "{\"type\":\"record\",...}",
                   description: "inline Avro JSON schema; takes priority over registry lookup"),
                 P("readerSchemaSubject",
                   description: "schema registry subject (used if readerSchema is empty)"),
                 P("readerSchemaVersion", def: "latest", placeholder: "latest"),
                 P("autoRegisterSubject",
                   description: "subject under which to auto-register the writer schema")]),
            (ctx, config) =>
            {
                Schema? staticSchema = null;
                SchemaRegistryProvider? registry = null;
                string? subject = null;
                var version = config.GetValueOrDefault("readerSchemaVersion", "latest");
                string? autoRegister = config.GetValueOrDefault("autoRegisterSubject", "");
                if (string.IsNullOrWhiteSpace(autoRegister)) autoRegister = null;

                if (ctx.TryGetProvider<SchemaRegistryProvider>("schema_registry", out var srProvider))
                    registry = srProvider;

                if (config.TryGetValue("readerSchema", out var rsJson) && !string.IsNullOrWhiteSpace(rsJson))
                {
                    staticSchema = AvroSchemaJson.Parse(rsJson);
                }
                else if (config.TryGetValue("readerSchemaSubject", out var subj) && !string.IsNullOrWhiteSpace(subj))
                {
                    if (registry is null)
                        throw new InvalidOperationException("readerSchemaSubject set but no schema_registry provider available");
                    subject = subj;
                }

                if (autoRegister is not null && registry is null)
                    throw new InvalidOperationException("autoRegisterSubject set but no schema_registry provider available");

                return new ConvertOCFToRecord(
                    ctx.GetContentStoreOrDefault(),
                    staticSchema, registry, subject, version,
                    autoRegister);
            });

        reg.Register(
            new ProcessorInfo("ConvertRecordToOCF", "Encode records to Avro OCF (.avro file)", "Conversion",
                [P("codec", kind: ParamKind.Enum, required: true, def: AvroOCF.CodecNull,
                   choices: [AvroOCF.CodecNull, AvroOCF.CodecDeflate, AvroOCF.CodecZstandard])]),
            (ctx, config) => new ConvertRecordToOCF(
                ConfigHelpers.RequireOneOf(config, "codec", AvroOCF.CodecNull,
                    new[] { AvroOCF.CodecNull, AvroOCF.CodecDeflate, AvroOCF.CodecZstandard })));

        reg.Register(
            new ProcessorInfo("ConvertCSVToRecord", "Parse CSV content into records", "Conversion",
                [P("delimiter", def: ",", description: "single-character column delimiter"),
                 P("hasHeader", kind: ParamKind.Boolean, def: "true"),
                 P("fields", placeholder: "id:long,name:string",
                   description: "comma-separated name:type pairs; overrides header-inferred names"),
                 P("schemaName", def: "default")]),
            (ctx, config) =>
            {
                var delim = ConfigHelpers.ParseSingleChar(config.GetValueOrDefault("delimiter"), "delimiter", ',');
                var hasHeader = ConfigHelpers.ParseBool(config.GetValueOrDefault("hasHeader"), "hasHeader", true);
                return new ConvertCSVToRecord(
                    ConfigHelpers.GetString(config, "schemaName", "default"),
                    delim,
                    hasHeader,
                    ctx.GetContentStoreOrDefault(),
                    ConfigHelpers.GetString(config, "fields"));
            });

        reg.Register(
            new ProcessorInfo("ConvertRecordToCSV", "Serialize records to CSV", "Conversion",
                [P("delimiter", def: ","),
                 P("includeHeader", kind: ParamKind.Boolean, def: "true")]),
            (ctx, config) =>
            {
                var delim = ConfigHelpers.ParseSingleChar(config.GetValueOrDefault("delimiter"), "delimiter", ',');
                var includeHeader = ConfigHelpers.ParseBool(config.GetValueOrDefault("includeHeader"), "includeHeader", true);
                return new ConvertRecordToCSV(delim, includeHeader);
            });

        // --- Transform ---

        reg.Register(
            new ProcessorInfo("EvaluateExpression", "Compute attributes from expressions", "Transform",
                [P("expressions", kind: ParamKind.KeyValueList, required: true,
                   valueKind: ParamKind.Expression,
                   entry: ";", pair: "=",
                   placeholder: "tax=amount*0.07; label=upper(region)",
                   description: "attr=expression pairs; later pairs see earlier writes")]),
            (ctx, config) =>
            {
                var exprs = new Dictionary<string, string>();
                var raw = config.GetValueOrDefault("expressions", "");
                foreach (var pair in raw.Split(';', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
                {
                    var eq = pair.IndexOf('=');
                    if (eq <= 0)
                        throw new ConfigException(
                            $"EvaluateExpression: malformed pair '{pair}' — expected 'attr=expression'");
                    exprs[pair[..eq]] = pair[(eq + 1)..];
                }
                return new EvaluateExpression(exprs);
            });

        reg.Register(
            new ProcessorInfo("TransformRecord", "Field-level operations on records", "Transform",
                [P("operations", kind: ParamKind.Multiline, required: true,
                   placeholder: "rename:oldName:newName; remove:badField; compute:total:amount*1.07",
                   description: "semicolon-delimited op:arg1[:arg2] directives: rename, remove, add, copy, toUpper, toLower, default, compute")]),
            (ctx, config) => new TransformRecord(config.GetValueOrDefault("operations", "")));

        reg.Register(
            new ProcessorInfo("UpdateRecord", "Set or derive record fields via expressions", "Transform",
                [P("updates", kind: ParamKind.KeyValueList, required: true,
                   valueKind: ParamKind.Expression,
                   entry: ";", pair: "=",
                   placeholder: "tax=amount*0.07; total=amount+tax",
                   description: "field=expression pairs; later pairs see earlier writes")]),
            (ctx, config) => new UpdateRecord(config.GetValueOrDefault("updates", "")));

        // --- Record ---

        reg.Register(
            new ProcessorInfo("SplitRecord", "Fan out a RecordContent FlowFile into one FlowFile per record", "Record",
                []),
            (ctx, config) => new SplitRecord());

        reg.Register(
            new ProcessorInfo("ExtractRecordField", "Extract record fields into FlowFile attributes", "Record",
                [P("fields", kind: ParamKind.KeyValueList, required: true,
                   entry: ";", pair: ":",
                   placeholder: "amount:order.amount;region:tenant.region",
                   description: "fieldPath:attrName pairs"),
                 P("recordIndex", kind: ParamKind.Integer, def: "0",
                   description: "which record in the batch to extract from")]),
            (ctx, config) => new ExtractRecordField(
                ConfigHelpers.RequireString(config, "fields"),
                ConfigHelpers.ParseInt(config.GetValueOrDefault("recordIndex"), "recordIndex", 0)));

        reg.Register(
            new ProcessorInfo("QueryRecord", "Filter records using a JsonPath query", "Record",
                [P("query", kind: ParamKind.Expression, required: true,
                   placeholder: "$[?(@.amount > 100)]",
                   description: "JsonPath filter against the record batch")]),
            (ctx, config) => new QueryRecord(config.GetValueOrDefault("query", "$")));

        // --- Routing ---

        reg.Register(
            new ProcessorInfo("RouteOnAttribute", "Route FlowFiles based on attribute predicates", "Routing",
                [P("routes", kind: ParamKind.Multiline, required: true,
                   placeholder: "premium: tier EQ premium; bulk: tier EQ bulk",
                   description: "semicolon-delimited 'name: attr OP value' entries; operators: EQ, NEQ, CONTAINS, STARTSWITH, ENDSWITH, EXISTS, GT, LT")]),
            (ctx, config) => new RouteOnAttribute(config.GetValueOrDefault("routes", "")));

        reg.Register(
            new ProcessorInfo("RouteRecord", "Partition records across routes via per-route expression predicates", "Routing",
                [P("routes", kind: ParamKind.KeyValueList, required: true,
                   valueKind: ParamKind.Expression,
                   entry: ";", pair: ":",
                   placeholder: "premium: tier == \"gold\"; minors: age < 18",
                   description: "name:expression pairs; first-match wins; non-matching records go to 'unmatched'")]),
            (ctx, config) => new RouteRecord(config.GetValueOrDefault("routes", "")));
    }
}
