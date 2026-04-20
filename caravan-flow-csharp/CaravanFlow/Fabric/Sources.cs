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
    public static void RegisterAll(SourceRegistry reg)
    {
        reg.Register(
            new SourceInfo("GetFile",
                "Polls a directory; emits one FlowFile per file (V3 bundles unpacked on sight).",
                ["inputDir", "pattern", "pollIntervalMs", "unpackV3"]),
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
                ["content", "contentType", "attributes", "batchSize", "pollIntervalMs"]),
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
                ["port", "path", "maxBodyBytes"]),
            (name, config, store) =>
            {
                var port = ConfigHelpers.ParseInt(config.GetValueOrDefault("port"), "port", 0);
                if (port <= 0) return null; // disabled — must opt in with a port
                var path = config.GetValueOrDefault("path", "/");
                // Default 16 MiB keeps an unauthenticated endpoint from being
                // turned into an OOM vector. Override per-source via config.
                var maxBytes = ConfigHelpers.ParseInt(config.GetValueOrDefault("maxBodyBytes"), "maxBodyBytes", 16 * 1024 * 1024);
                return new ListenHTTP(name, port, path, maxBytes);
            });
    }
}
