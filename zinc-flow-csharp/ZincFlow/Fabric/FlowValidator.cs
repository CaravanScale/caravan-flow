using ZincFlow.Core;
using ZincFlow.StdLib;

namespace ZincFlow.Fabric;

/// <summary>
/// End-to-end config validator. Wraps the existing structural ConfigValidator
/// (registered types, connection targets, DAG cycles) and additionally tries to
/// construct every processor against a synthetic context so factory-side errors
/// (missing config keys, malformed regexes, bad expressions, etc.) surface up
/// front instead of crashing the running server.
///
/// Used by the `validate` CLI subcommand to gate CI/pre-commit on a clean config.
/// </summary>
public static class FlowValidator
{
    public sealed class Issue
    {
        public string Severity { get; }   // "error" | "warning"
        public string Path { get; }
        public string Message { get; }
        public Issue(string severity, string path, string message)
        {
            Severity = severity;
            Path = path;
            Message = message;
        }
        public override string ToString() => $"[{Severity}] {Path}: {Message}";
    }

    public sealed class Result
    {
        public List<Issue> Issues { get; } = new();
        public int ErrorCount => Issues.Count(i => i.Severity == "error");
        public int WarningCount => Issues.Count(i => i.Severity == "warning");
        public bool HasErrors => ErrorCount > 0;
    }

    public static Result Validate(Dictionary<string, object?> config, Registry registry)
    {
        var result = new Result();

        // Step 1: structural validation (sections present, types registered, connections resolve, DAG acyclic).
        foreach (var err in ConfigValidator.Validate(config, registry))
            result.Issues.Add(new("error", err.Path, err.Message));

        // Step 2: factory construction. Tries to instantiate each processor against a
        // synthetic context that has all standard providers enabled. Catches anything
        // the factory throws — bad regex, missing required key, malformed expression.
        var flowDict = Fabric.AsStringDict(config.GetValueOrDefault("flow"));
        if (flowDict is null) return result;
        var procDefs = Fabric.AsStringDict(flowDict.GetValueOrDefault("processors"));
        if (procDefs is null) return result;

        var ctx = BuildSyntheticContext(config);

        foreach (var (name, defObj) in procDefs)
        {
            var def = Fabric.AsStringDict(defObj);
            if (def is null) continue;
            var typeName = Fabric.GetStr(def, "type");
            if (string.IsNullOrEmpty(typeName) || !registry.Has(typeName)) continue;

            var procConfig = new Dictionary<string, string>();
            var cfgDict = Fabric.AsStringDict(def.GetValueOrDefault("config"));
            if (cfgDict is not null)
                foreach (var (k, v) in cfgDict)
                    procConfig[k] = v?.ToString() ?? "";

            try
            {
                _ = registry.Create(typeName, ctx, procConfig);
            }
            catch (KeyNotFoundException ex)
            {
                result.Issues.Add(new("error", $"flow.processors.{name}.config",
                    $"missing required config key (constructor threw: {ex.Message})"));
            }
            catch (Exception ex)
            {
                result.Issues.Add(new("error", $"flow.processors.{name}",
                    $"construction failed ({ex.GetType().Name}): {ex.Message}"));
            }
        }

        return result;
    }

    private static ScopedContext BuildSyntheticContext(Dictionary<string, object?> config)
    {
        var providers = new Dictionary<string, IProvider>
        {
            ["content"] = new ContentProvider("content", new MemoryContentStore()),
            ["logging"] = new LoggingProvider(),
            ["provenance"] = new ProvenanceProvider(),
            ["config"] = new ConfigProvider(new())
        };

        // Mirror Program.cs: if the config declares a schema registry, register
        // a stub provider so processors that `requires: [schema_registry]`
        // can be constructed during validation. The actual fetch is lazy,
        // so we don't need a reachable URL — just a provider of the right type.
        var srUrl = GetConfigString(config, "schema_registry.url");
        if (!string.IsNullOrEmpty(srUrl))
        {
            var srProvider = new SchemaRegistryProvider(new SchemaRegistryClient(srUrl));
            srProvider.Enable();
            providers["schema_registry"] = srProvider;
        }

        foreach (var p in providers.Values) p.Enable();
        return new ScopedContext(providers);
    }

    private static string GetConfigString(Dictionary<string, object?> config, string dotPath)
    {
        var parts = dotPath.Split('.');
        object? cur = config;
        foreach (var part in parts)
        {
            if (Fabric.TryGetDictValue(cur, part, out cur)) continue;
            return "";
        }
        return cur?.ToString() ?? "";
    }
}
