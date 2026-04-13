using System.Text;
using ZincFlow.Core;

namespace ZincFlow.Fabric;

/// <summary>
/// Prometheus /metrics endpoint — text exposition format.
/// No external dependencies; generates metrics from Fabric state.
/// </summary>
public sealed class MetricsHandler
{
    private readonly Fabric _fab;

    public MetricsHandler(Fabric fab) => _fab = fab;

    public Task HandleMetrics(HttpContext ctx)
    {
        ctx.Response.ContentType = "text/plain; version=0.0.4; charset=utf-8";
        var sb = new StringBuilder(2048);

        // Processed count
        var stats = _fab.GetStats();
        sb.AppendLine("# HELP zinc_flow_processed_total Total FlowFiles processed");
        sb.AppendLine("# TYPE zinc_flow_processed_total counter");
        sb.Append("zinc_flow_processed_total ").AppendLine(stats.GetValueOrDefault("processed", (object)0)?.ToString() ?? "0");

        // Active executions
        sb.AppendLine("# HELP zinc_flow_active_executions Currently in-flight pipeline executions");
        sb.AppendLine("# TYPE zinc_flow_active_executions gauge");
        sb.Append("zinc_flow_active_executions ").AppendLine(stats.GetValueOrDefault("active_executions", (object)0)?.ToString() ?? "0");

        // Per-processor stats
        var procStats = _fab.GetProcessorStats();
        sb.AppendLine("# HELP zinc_flow_processor_processed_total FlowFiles processed per processor");
        sb.AppendLine("# TYPE zinc_flow_processor_processed_total counter");
        foreach (var (name, ps) in procStats)
            sb.Append("zinc_flow_processor_processed_total{processor=\"").Append(name).Append("\"} ").AppendLine(ps.GetValueOrDefault("processed", 0).ToString());

        sb.AppendLine("# HELP zinc_flow_processor_errors_total Errors per processor");
        sb.AppendLine("# TYPE zinc_flow_processor_errors_total counter");
        foreach (var (name, ps) in procStats)
            sb.Append("zinc_flow_processor_errors_total{processor=\"").Append(name).Append("\"} ").AppendLine(ps.GetValueOrDefault("errors", 0).ToString());

        // Source connectors
        var sources = _fab.GetSources();
        sb.AppendLine("# HELP zinc_flow_source_running Whether connector source is running (1/0)");
        sb.AppendLine("# TYPE zinc_flow_source_running gauge");
        foreach (var src in sources)
            sb.Append("zinc_flow_source_running{name=\"").Append(src.Name).Append("\",type=\"").Append(src.Type).Append("\"} ").AppendLine(src.Running ? "1" : "0");

        // Uptime
        sb.AppendLine("# HELP zinc_flow_uptime_seconds Time since process start");
        sb.AppendLine("# TYPE zinc_flow_uptime_seconds gauge");
        sb.Append("zinc_flow_uptime_seconds ").AppendLine(((Environment.TickCount64 - _startTick) / 1000.0).ToString("F1"));

        return ctx.Response.WriteAsync(sb.ToString());
    }

    private static readonly long _startTick = Environment.TickCount64;
}

/// <summary>
/// Config validation — check config.yaml for common errors at startup.
/// </summary>
public static class ConfigValidator
{
    public sealed class ValidationError
    {
        public string Path { get; }
        public string Message { get; }
        public bool IsWarning { get; }
        public ValidationError(string path, string message, bool isWarning = false)
        { Path = path; Message = message; IsWarning = isWarning; }
        public override string ToString()
            => $"  {(IsWarning ? "[warn] " : "")}{Path}: {Message}";
    }

    public static List<ValidationError> Validate(Dictionary<string, object?> config, Registry registry)
    {
        var errors = new List<ValidationError>();

        // Check flow.processors
        var flowDict = Fabric.AsStringDict(config.GetValueOrDefault("flow"));
        if (flowDict is null)
        {
            errors.Add(new("flow", "missing flow section"));
            return errors;
        }

        var procDefs = Fabric.AsStringDict(flowDict.GetValueOrDefault("processors"));
        if (procDefs is null || procDefs.Count == 0)
        {
            errors.Add(new("flow.processors", "no processors defined"));
            return errors;
        }

        var procNames = new HashSet<string>();
        foreach (var (name, defObj) in procDefs)
        {
            procNames.Add(name);
            var def = Fabric.AsStringDict(defObj);
            if (def is null)
            {
                errors.Add(new($"flow.processors.{name}", "invalid processor definition"));
                continue;
            }

            var typeName = Fabric.GetStr(def, "type");
            if (string.IsNullOrEmpty(typeName))
                errors.Add(new($"flow.processors.{name}.type", "missing processor type"));
            else if (!registry.Has(typeName))
                errors.Add(new($"flow.processors.{name}.type", $"unknown processor type: {typeName}"));
            else
            {
                // Warn on user config keys that don't match any documented key for
                // this processor type. Catches typos like `ouput_dir` at validate
                // time instead of letting the value silently default later.
                var info = registry.GetInfo(typeName);
                var cfgDict = Fabric.AsStringDict(def.GetValueOrDefault("config"));
                if (info is not null && cfgDict is not null && info.ConfigKeys.Count > 0)
                {
                    var known = new HashSet<string>(info.ConfigKeys, StringComparer.Ordinal);
                    foreach (var key in cfgDict.Keys)
                    {
                        if (!known.Contains(key))
                            errors.Add(new($"flow.processors.{name}.config.{key}",
                                $"unrecognized config key for {typeName}; known keys: [{string.Join(", ", info.ConfigKeys)}]",
                                isWarning: true));
                    }
                }
            }
        }

        // Check connections reference valid destinations
        foreach (var (name, defObj) in procDefs)
        {
            var def = Fabric.AsStringDict(defObj);
            if (def is null) continue;
            var connDefs = Fabric.AsStringDict(def.GetValueOrDefault("connections"));
            if (connDefs is null) continue;
            foreach (var (rel, destObj) in connDefs)
            {
                var dests = Fabric.GetStringList(connDefs, rel);
                if (dests is null) continue;
                foreach (var dest in dests)
                {
                    if (!procNames.Contains(dest))
                        errors.Add(new($"flow.processors.{name}.connections.{rel}", $"target '{dest}' is not a defined processor"));
                }
            }
        }

        // DAG validation
        var connections = new Dictionary<string, Dictionary<string, List<string>>>();
        foreach (var (name, defObj) in procDefs)
        {
            var def = Fabric.AsStringDict(defObj);
            if (def is null) continue;
            var parsed = Fabric.ParseConnections(def);
            connections[name] = parsed;
        }
        var dagResult = DagValidator.Validate(connections);
        foreach (var err in dagResult.Errors)
            errors.Add(new("flow.dag", err));
        foreach (var warn in dagResult.Warnings)
            errors.Add(new("flow.dag", warn));

        return errors;
    }
}

/// <summary>
/// DAG validator: validates the processor connection graph for cycles,
/// invalid targets, unreachable processors, and computes entry points.
/// </summary>
public static class DagValidator
{
    public sealed class DagResult
    {
        public List<string> Errors { get; } = new();
        public List<string> Warnings { get; } = new();
        public List<string> EntryPoints { get; } = new();
    }

    public static DagResult Validate(Dictionary<string, Dictionary<string, List<string>>> processorConnections)
    {
        var result = new DagResult();
        var allProcessors = new HashSet<string>(processorConnections.Keys);

        // Build adjacency list (flatten all relationships)
        var adjacency = new Dictionary<string, List<string>>();
        var referenced = new HashSet<string>();
        foreach (var (proc, connections) in processorConnections)
        {
            var targets = new List<string>();
            foreach (var (_, dests) in connections)
            {
                foreach (var dest in dests)
                {
                    targets.Add(dest);
                    referenced.Add(dest);
                    if (!allProcessors.Contains(dest))
                        result.Errors.Add($"processor '{proc}' connects to unknown target '{dest}'");
                }
            }
            adjacency[proc] = targets;
        }

        // Entry points: processors not referenced as a target by any other processor
        foreach (var proc in allProcessors)
        {
            if (!referenced.Contains(proc))
                result.EntryPoints.Add(proc);
        }

        if (result.EntryPoints.Count == 0 && allProcessors.Count > 0)
            result.Warnings.Add("no entry-point processors detected (all processors are downstream targets)");

        // Cycle detection: DFS 3-color (0=white, 1=gray, 2=black)
        var color = new Dictionary<string, int>();
        foreach (var proc in allProcessors)
            color[proc] = 0;

        foreach (var proc in allProcessors)
        {
            if (color[proc] == 0)
                DetectCycles(proc, adjacency, color, new List<string>(), result, allProcessors);
        }

        // Unreachable detection: BFS from entry points
        if (result.EntryPoints.Count > 0)
        {
            var reachable = new HashSet<string>();
            var queue = new Queue<string>(result.EntryPoints);
            while (queue.Count > 0)
            {
                var current = queue.Dequeue();
                if (!reachable.Add(current)) continue;
                if (adjacency.TryGetValue(current, out var targets))
                {
                    foreach (var t in targets)
                    {
                        if (allProcessors.Contains(t) && !reachable.Contains(t))
                            queue.Enqueue(t);
                    }
                }
            }
            foreach (var proc in allProcessors)
            {
                if (!reachable.Contains(proc))
                    result.Warnings.Add($"processor '{proc}' is not reachable from any entry point");
            }
        }

        return result;
    }

    private static void DetectCycles(string node, Dictionary<string, List<string>> adjacency,
        Dictionary<string, int> color, List<string> path, DagResult result, HashSet<string> allProcessors)
    {
        color[node] = 1; // gray
        path.Add(node);

        if (adjacency.TryGetValue(node, out var targets))
        {
            foreach (var target in targets)
            {
                if (!allProcessors.Contains(target)) continue;
                if (!color.TryGetValue(target, out var c)) continue;
                if (c == 1) // gray = back edge = cycle
                {
                    var cycleStart = path.IndexOf(target);
                    var cycle = string.Join(" → ", path.Skip(cycleStart)) + " → " + target;
                    result.Warnings.Add($"cycle detected: {cycle}");
                }
                else if (c == 0)
                {
                    DetectCycles(target, adjacency, color, path, result, allProcessors);
                }
            }
        }

        path.RemoveAt(path.Count - 1);
        color[node] = 2; // black
    }
}
