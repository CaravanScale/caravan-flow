using CaravanFlow.Core;
using CaravanFlow.StdLib;

namespace CaravanFlow.Fabric;

/// <summary>
/// Registers all built-in source types with a <see cref="SourceRegistry"/>.
/// Parallels <see cref="BuiltinProcessors"/> — Program.cs calls this once
/// at startup then iterates config.yaml's <c>sources:</c> block to
/// instantiate named instances.
/// </summary>
public static class BuiltinSources
{
    // Same concise ParamInfo builder shape as BuiltinProcessors, kept
    // local so sources don't depend on the processor registration file.
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

    public static void RegisterAll(SourceRegistry reg)
    {
        reg.Register(
            new SourceInfo("GetFile",
                "Polls a directory; emits one FlowFile per file (V3 bundles unpacked on sight).",
                [P("inputDir", required: true, placeholder: "/tmp/caravan-in"),
                 P("pattern", def: "*", description: "glob pattern for files to pick up"),
                 P("pollIntervalMs", kind: ParamKind.Integer, def: "1000"),
                 P("unpackV3", kind: ParamKind.Boolean, def: "true",
                   description: "if true, NiFi V3-framed files are decoded into (attributes + content)")]),
            (name, config, store) =>
            {
                var inputDir = config.GetValueOrDefault("inputDir", "");
                if (string.IsNullOrEmpty(inputDir)) return null; // disabled — factory-null == skip
                var pattern = config.GetValueOrDefault("pattern", "*");
                var pollMs = ConfigHelpers.ParseInt(config.GetValueOrDefault("pollIntervalMs"), "pollIntervalMs", 1000);
                var unpackV3 = config.GetValueOrDefault("unpackV3", "true") != "false";
                return new GetFile(name, inputDir, pattern, pollMs, store, unpackV3);
            });

        reg.Register(
            new SourceInfo("GenerateFlowFile",
                "Timer-driven FlowFile generator for heartbeats and load tests.",
                [P("content", required: true, placeholder: "heartbeat"),
                 P("contentType", def: "", placeholder: "text/plain"),
                 P("attributes", kind: ParamKind.KeyValueList,
                   entry: ";", pair: ":",
                   placeholder: "env:dev;source:generator",
                   description: "key:value attribute pairs copied onto every emitted FlowFile"),
                 P("batchSize", kind: ParamKind.Integer, def: "1",
                   description: "FlowFiles emitted per tick"),
                 P("pollIntervalMs", kind: ParamKind.Integer, def: "1000")]),
            (name, config, store) =>
            {
                var content = config.GetValueOrDefault("content", "");
                if (string.IsNullOrEmpty(content)) return null; // disabled
                var contentType = config.GetValueOrDefault("contentType", "");
                var attrs = config.GetValueOrDefault("attributes", "");
                var batchSize = ConfigHelpers.ParseInt(config.GetValueOrDefault("batchSize"), "batchSize", 1);
                var pollMs = ConfigHelpers.ParseInt(config.GetValueOrDefault("pollIntervalMs"), "pollIntervalMs", 1000);
                return new GenerateFlowFile(name, pollMs, content, contentType, attrs, batchSize);
            });

        reg.Register(
            new SourceInfo("ListenHTTP",
                "Binds an HTTP listener on {port}{path}; each POST body becomes a FlowFile.",
                [P("port", kind: ParamKind.Integer, required: true, placeholder: "9100"),
                 P("path", def: "/", placeholder: "/ingest"),
                 P("maxBodyBytes", kind: ParamKind.Integer, def: "16777216",
                   description: "reject requests larger than this (default 16 MiB)")]),
            (name, config, store) =>
            {
                var port = ConfigHelpers.ParseInt(config.GetValueOrDefault("port"), "port", 0);
                if (port <= 0) return null; // disabled — must opt in with a port
                var path = config.GetValueOrDefault("path", "/");
                var maxBytes = ConfigHelpers.ParseInt(config.GetValueOrDefault("maxBodyBytes"), "maxBodyBytes", 16 * 1024 * 1024);
                return new ListenHTTP(name, port, path, maxBytes);
            });
    }
}
