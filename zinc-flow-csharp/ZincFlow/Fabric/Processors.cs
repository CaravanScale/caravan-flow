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
                LoggingProvider? log = null;
                try { log = ctx.GetProvider("logging") as LoggingProvider; } catch { }
                return new LogAttribute(config.GetValueOrDefault("prefix", "flow"), log);
            });

        reg.Register(
            new ProcessorInfo("ConvertJSONToRecord", "Parses JSON content into records", ["schema_name"]),
            (ctx, config) =>
            {
                var schemaName = config.GetValueOrDefault("schema_name", "default");
                IContentStore store;
                try { store = ((ContentProvider)ctx.GetProvider("content")).Store; }
                catch { store = new MemoryContentStore(); }
                return new ConvertJSONToRecord(schemaName, store);
            });

        reg.Register(
            new ProcessorInfo("ConvertRecordToJSON", "Serializes records back to JSON", []),
            (ctx, config) => new ConvertRecordToJSON());

        reg.Register(
            new ProcessorInfo("PutHTTP", "POST FlowFile to downstream HTTP endpoint", ["endpoint"]),
            (ctx, config) =>
            {
                IContentStore store;
                try { store = ((ContentProvider)ctx.GetProvider("content")).Store; }
                catch { store = new MemoryContentStore(); }
                return new PutHTTP(config["endpoint"], config.GetValueOrDefault("format", "raw"), store);
            });

        reg.Register(
            new ProcessorInfo("PutFile", "Write FlowFile content to directory", ["output_dir"]),
            (ctx, config) =>
            {
                IContentStore store;
                try { store = ((ContentProvider)ctx.GetProvider("content")).Store; }
                catch { store = new MemoryContentStore(); }
                return new PutFile(
                    config["output_dir"],
                    config.GetValueOrDefault("naming_attribute", "filename"),
                    config.GetValueOrDefault("prefix", ""),
                    config.GetValueOrDefault("suffix", ""),
                    store);
            });

        reg.Register(
            new ProcessorInfo("PutStdout", "Write FlowFile content to stdout", []),
            (ctx, config) =>
            {
                IContentStore store;
                try { store = ((ContentProvider)ctx.GetProvider("content")).Store; }
                catch { store = new MemoryContentStore(); }
                return new PutStdout(config.GetValueOrDefault("format", "text"), store);
            });

        // --- Text processors ---

        reg.Register(
            new ProcessorInfo("ReplaceText", "Regex find/replace on content", ["pattern", "replacement"]),
            (ctx, config) =>
            {
                IContentStore store;
                try { store = ((ContentProvider)ctx.GetProvider("content")).Store; }
                catch { store = new MemoryContentStore(); }
                return new ReplaceText(
                    config["pattern"],
                    config.GetValueOrDefault("replacement", ""),
                    config.GetValueOrDefault("mode", "all"),
                    store);
            });

        reg.Register(
            new ProcessorInfo("ExtractText", "Regex capture groups → attributes", ["pattern"]),
            (ctx, config) =>
            {
                IContentStore store;
                try { store = ((ContentProvider)ctx.GetProvider("content")).Store; }
                catch { store = new MemoryContentStore(); }
                return new ExtractText(
                    config["pattern"],
                    config.GetValueOrDefault("group_names", ""),
                    store);
            });

        reg.Register(
            new ProcessorInfo("SplitText", "Split content by delimiter into multiple FlowFiles", ["delimiter"]),
            (ctx, config) =>
            {
                IContentStore store;
                try { store = ((ContentProvider)ctx.GetProvider("content")).Store; }
                catch { store = new MemoryContentStore(); }
                int headerLines = int.TryParse(config.GetValueOrDefault("header_lines", "0"), out var h) ? h : 0;
                return new SplitText(config["delimiter"], headerLines, store);
            });

        // --- Record conversion ---

        reg.Register(
            new ProcessorInfo("ConvertAvroToRecord", "Decode Avro binary into records", ["fields"]),
            (ctx, config) =>
            {
                IContentStore store;
                try { store = ((ContentProvider)ctx.GetProvider("content")).Store; }
                catch { store = new MemoryContentStore(); }
                return new ConvertAvroToRecord(
                    config.GetValueOrDefault("schema_name", "default"),
                    config.GetValueOrDefault("fields", ""),
                    store);
            });

        reg.Register(
            new ProcessorInfo("ConvertRecordToAvro", "Encode records to Avro binary", []),
            (ctx, config) => new ConvertRecordToAvro());

        reg.Register(
            new ProcessorInfo("ConvertCSVToRecord", "Parse CSV content into records", []),
            (ctx, config) =>
            {
                IContentStore store;
                try { store = ((ContentProvider)ctx.GetProvider("content")).Store; }
                catch { store = new MemoryContentStore(); }
                var delim = config.GetValueOrDefault("delimiter", ",");
                var hasHeader = config.GetValueOrDefault("has_header", "true") != "false";
                return new ConvertCSVToRecord(
                    config.GetValueOrDefault("schema_name", "default"),
                    delim.Length > 0 ? delim[0] : ',',
                    hasHeader,
                    store);
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
                // Parse expressions: "target1=expr1;target2=expr2"
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
    }
}
