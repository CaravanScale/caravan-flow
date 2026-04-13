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
            (ctx, config) => new UpdateAttribute(config["key"], config["value"]));

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
            new ProcessorInfo("PutHTTP", "POST FlowFile to downstream HTTP endpoint", ["endpoint"]),
            (ctx, config) => new PutHTTP(
                config["endpoint"],
                config.GetValueOrDefault("format", "raw"),
                ctx.GetContentStoreOrDefault()));

        reg.Register(
            new ProcessorInfo("PutFile", "Write FlowFile content to directory", ["output_dir"]),
            (ctx, config) => new PutFile(
                config["output_dir"],
                config.GetValueOrDefault("naming_attribute", "filename"),
                config.GetValueOrDefault("prefix", ""),
                config.GetValueOrDefault("suffix", ""),
                ctx.GetContentStoreOrDefault()));

        reg.Register(
            new ProcessorInfo("PutStdout", "Write FlowFile content to stdout", []),
            (ctx, config) => new PutStdout(
                config.GetValueOrDefault("format", "text"),
                ctx.GetContentStoreOrDefault()));

        // --- Text processors ---

        reg.Register(
            new ProcessorInfo("ReplaceText", "Regex find/replace on content", ["pattern", "replacement"]),
            (ctx, config) => new ReplaceText(
                config["pattern"],
                config.GetValueOrDefault("replacement", ""),
                config.GetValueOrDefault("mode", "all"),
                ctx.GetContentStoreOrDefault()));

        reg.Register(
            new ProcessorInfo("ExtractText", "Regex capture groups → attributes", ["pattern"]),
            (ctx, config) => new ExtractText(
                config["pattern"],
                config.GetValueOrDefault("group_names", ""),
                ctx.GetContentStoreOrDefault()));

        reg.Register(
            new ProcessorInfo("SplitText", "Split content by delimiter into multiple FlowFiles", ["delimiter"]),
            (ctx, config) =>
            {
                int headerLines = int.TryParse(config.GetValueOrDefault("header_lines", "0"), out var h) ? h : 0;
                return new SplitText(config["delimiter"], headerLines, ctx.GetContentStoreOrDefault());
            });

        // --- Record conversion ---

        reg.Register(
            new ProcessorInfo("ConvertAvroToRecord", "Decode Avro binary into records", ["fields"]),
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
            (ctx, config) => new ConvertRecordToOCF(config.GetValueOrDefault("codec", AvroOCF.CodecNull)));

        reg.Register(
            new ProcessorInfo("ConvertCSVToRecord", "Parse CSV content into records", []),
            (ctx, config) =>
            {
                var delim = config.GetValueOrDefault("delimiter", ",");
                var hasHeader = config.GetValueOrDefault("has_header", "true") != "false";
                return new ConvertCSVToRecord(
                    config.GetValueOrDefault("schema_name", "default"),
                    delim.Length > 0 ? delim[0] : ',',
                    hasHeader,
                    ctx.GetContentStoreOrDefault());
            });

        reg.Register(
            new ProcessorInfo("ConvertRecordToCSV", "Serialize records to CSV", []),
            (ctx, config) =>
            {
                var delim = config.GetValueOrDefault("delimiter", ",");
                var includeHeader = config.GetValueOrDefault("include_header", "true") != "false";
                return new ConvertRecordToCSV(delim.Length > 0 ? delim[0] : ',', includeHeader);
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
                    if (eq > 0)
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
                config.GetValueOrDefault("fields", ""),
                int.TryParse(config.GetValueOrDefault("record_index", "0"), out var ri) ? ri : 0));

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
