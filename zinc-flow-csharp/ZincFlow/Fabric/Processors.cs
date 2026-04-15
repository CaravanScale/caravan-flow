using ZincFlow.Core;
using ZincFlow.StdLib;

namespace ZincFlow.Fabric;

/// <summary>
/// Registers all built-in processors from StdLib with the registry.
/// </summary>
public static class BuiltinProcessors
{
    public static void RegisterAll(Registry reg)
    {
        reg.Register(
            new ProcessorInfo("UpdateAttribute", "Sets key=value attribute on FlowFiles", ["key", "value"]),
            (ctx, config) => new UpdateAttribute(
                ConfigHelpers.RequireString(config, "key"),
                ConfigHelpers.RequireString(config, "value")));

        reg.Register(
            new ProcessorInfo("LogAttribute", "Logs FlowFile attributes and passes through", ["prefix"]),
            (ctx, config) =>
            {
                ctx.TryGetProvider<LoggingProvider>("logging", out var log);
                return new LogAttribute(config.GetValueOrDefault("prefix", "flow"), log);
            });

        reg.Register(
            new ProcessorInfo("ConvertJSONToRecord", "Parses JSON content into records", ["schema_name"]),
            (ctx, config) => new ConvertJSONToRecord(
                config.GetValueOrDefault("schema_name", "default"),
                ctx.GetContentStoreOrDefault()));

        reg.Register(
            new ProcessorInfo("ConvertRecordToJSON", "Serializes records back to JSON", []),
            (ctx, config) => new ConvertRecordToJSON());

        reg.Register(
            new ProcessorInfo("PutHTTP", "POST FlowFile to downstream HTTP endpoint",
                ["endpoint", "format"]),
            (ctx, config) => new PutHTTP(
                ConfigHelpers.RequireString(config, "endpoint"),
                ConfigHelpers.GetString(config, "format", "raw"),
                ctx.GetContentStoreOrDefault()));

        reg.Register(
            new ProcessorInfo("PutFile", "Write FlowFile content to directory",
                ["output_dir", "naming_attribute", "prefix", "suffix", "format"]),
            (ctx, config) => new PutFile(
                ConfigHelpers.RequireString(config, "output_dir"),
                ConfigHelpers.GetString(config, "naming_attribute", "filename"),
                ConfigHelpers.GetString(config, "prefix"),
                ConfigHelpers.GetString(config, "suffix"),
                ctx.GetContentStoreOrDefault(),
                ConfigHelpers.GetString(config, "format", "raw")));

        reg.Register(
            new ProcessorInfo("PutStdout", "Write FlowFile content to stdout", ["format"]),
            (ctx, config) => new PutStdout(
                ConfigHelpers.RequireOneOf(config, "format", "text",
                    new[] { "attrs", "raw", "text", "v3", "hex" }),
                ctx.GetContentStoreOrDefault()));

        reg.Register(
            new ProcessorInfo("PackageFlowFileV3", "Wrap (attributes + content) into NiFi V3 binary content", []),
            (ctx, config) => new PackageFlowFileV3(ctx.GetContentStoreOrDefault()));

        reg.Register(
            new ProcessorInfo("UnpackageFlowFileV3", "Decode V3 binary content into one or more FlowFiles", []),
            (ctx, config) => new UnpackageFlowFileV3(ctx.GetContentStoreOrDefault()));

        // --- Text processors ---

        reg.Register(
            new ProcessorInfo("ReplaceText", "Regex find/replace on content",
                ["pattern", "replacement", "mode"]),
            (ctx, config) => new ReplaceText(
                ConfigHelpers.RequireString(config, "pattern"),
                ConfigHelpers.GetString(config, "replacement"),
                ConfigHelpers.GetString(config, "mode", "all"),
                ctx.GetContentStoreOrDefault()));

        reg.Register(
            new ProcessorInfo("ExtractText", "Regex capture groups → attributes",
                ["pattern", "group_names"]),
            (ctx, config) => new ExtractText(
                ConfigHelpers.RequireString(config, "pattern"),
                ConfigHelpers.GetString(config, "group_names"),
                ctx.GetContentStoreOrDefault()));

        reg.Register(
            new ProcessorInfo("SplitText", "Split content by delimiter into multiple FlowFiles",
                ["delimiter", "header_lines"]),
            (ctx, config) =>
            {
                var delim = ConfigHelpers.RequireString(config, "delimiter");
                var headerLines = ConfigHelpers.ParseInt(config.GetValueOrDefault("header_lines"), "header_lines", 0);
                return new SplitText(delim, headerLines, ctx.GetContentStoreOrDefault());
            });

        // --- Record conversion ---

        reg.Register(
            new ProcessorInfo("ConvertAvroToRecord", "Decode Avro binary into records",
                ["fields", "schema_name"]),
            (ctx, config) => new ConvertAvroToRecord(
                config.GetValueOrDefault("schema_name", "default"),
                config.GetValueOrDefault("fields", ""),
                ctx.GetContentStoreOrDefault()));

        reg.Register(
            new ProcessorInfo("ConvertRecordToAvro", "Encode records to Avro binary", []),
            (ctx, config) => new ConvertRecordToAvro());

        reg.Register(
            new ProcessorInfo("ConvertOCFToRecord", "Decode Avro OCF (.avro file) into records",
                ["reader_schema", "reader_schema_subject", "reader_schema_version", "auto_register_subject"]),
            (ctx, config) =>
            {
                Schema? staticSchema = null;
                SchemaRegistryProvider? registry = null;
                string? subject = null;
                var version = config.GetValueOrDefault("reader_schema_version", "latest");
                string? autoRegister = config.GetValueOrDefault("auto_register_subject", "");
                if (string.IsNullOrWhiteSpace(autoRegister)) autoRegister = null;

                // Resolve registry provider once — needed for both reader_schema_subject
                // and auto_register_subject.
                if (ctx.TryGetProvider<SchemaRegistryProvider>("schema_registry", out var srProvider))
                    registry = srProvider;

                // Inline JSON schema takes priority over registry lookup.
                if (config.TryGetValue("reader_schema", out var rsJson) && !string.IsNullOrWhiteSpace(rsJson))
                {
                    staticSchema = AvroSchemaJson.Parse(rsJson);
                }
                else if (config.TryGetValue("reader_schema_subject", out var subj) && !string.IsNullOrWhiteSpace(subj))
                {
                    if (registry is null)
                        throw new InvalidOperationException("reader_schema_subject set but no schema_registry provider available");
                    subject = subj;
                }

                if (autoRegister is not null && registry is null)
                    throw new InvalidOperationException("auto_register_subject set but no schema_registry provider available");

                return new ConvertOCFToRecord(
                    ctx.GetContentStoreOrDefault(),
                    staticSchema, registry, subject, version,
                    autoRegister);
            });

        reg.Register(
            new ProcessorInfo("ConvertRecordToOCF", "Encode records to Avro OCF (.avro file)", ["codec"]),
            (ctx, config) => new ConvertRecordToOCF(
                ConfigHelpers.RequireOneOf(config, "codec", AvroOCF.CodecNull,
                    new[] { AvroOCF.CodecNull, AvroOCF.CodecDeflate, AvroOCF.CodecZstandard })));

        reg.Register(
            new ProcessorInfo("ConvertCSVToRecord", "Parse CSV content into records",
                ["delimiter", "has_header", "fields", "schema_name"]),
            (ctx, config) =>
            {
                var delim = ConfigHelpers.ParseSingleChar(config.GetValueOrDefault("delimiter"), "delimiter", ',');
                var hasHeader = ConfigHelpers.ParseBool(config.GetValueOrDefault("has_header"), "has_header", true);
                return new ConvertCSVToRecord(
                    ConfigHelpers.GetString(config, "schema_name", "default"),
                    delim,
                    hasHeader,
                    ctx.GetContentStoreOrDefault(),
                    ConfigHelpers.GetString(config, "fields"));
            });

        reg.Register(
            new ProcessorInfo("ConvertRecordToCSV", "Serialize records to CSV",
                ["delimiter", "include_header"]),
            (ctx, config) =>
            {
                var delim = ConfigHelpers.ParseSingleChar(config.GetValueOrDefault("delimiter"), "delimiter", ',');
                var includeHeader = ConfigHelpers.ParseBool(config.GetValueOrDefault("include_header"), "include_header", true);
                return new ConvertRecordToCSV(delim, includeHeader);
            });

        // --- Expression / Transform ---

        reg.Register(
            new ProcessorInfo("EvaluateExpression", "Compute attributes from expressions", ["expressions"]),
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
            new ProcessorInfo("TransformRecord", "Field-level operations on records", ["operations"]),
            (ctx, config) => new TransformRecord(config.GetValueOrDefault("operations", "")));

        // --- Routing ---

        reg.Register(
            new ProcessorInfo("RouteOnAttribute", "Route FlowFiles based on attribute predicates", ["routes"]),
            (ctx, config) => new RouteOnAttribute(config.GetValueOrDefault("routes", "")));

        // --- Record field extraction / query ---

        reg.Register(
            new ProcessorInfo("ExtractRecordField", "Extract record fields into FlowFile attributes", ["fields", "record_index"]),
            (ctx, config) => new ExtractRecordField(
                ConfigHelpers.RequireString(config, "fields"),
                ConfigHelpers.ParseInt(config.GetValueOrDefault("record_index"), "record_index", 0)));

        reg.Register(
            new ProcessorInfo("QueryRecord", "Filter records by predicate", ["where"]),
            (ctx, config) => new QueryRecord(config.GetValueOrDefault("where", "")));

        // --- Attribute filtering ---

        reg.Register(
            new ProcessorInfo("FilterAttribute", "Remove or keep specific attributes", ["mode", "attributes"]),
            (ctx, config) => new FilterAttribute(
                config.GetValueOrDefault("mode", "remove"),
                config.GetValueOrDefault("attributes", "")));
    }
}
