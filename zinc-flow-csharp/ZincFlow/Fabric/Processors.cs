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
    }
}
