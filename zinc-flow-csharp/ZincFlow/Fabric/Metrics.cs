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
        sb.Append("zinc_flow_processed_total ").AppendLine(stats.GetValueOrDefault("processed", 0).ToString());

        // DLQ
        sb.AppendLine("# HELP zinc_flow_dlq_entries Current DLQ entry count");
        sb.AppendLine("# TYPE zinc_flow_dlq_entries gauge");
        sb.Append("zinc_flow_dlq_entries ").AppendLine(stats.GetValueOrDefault("dlq", 0).ToString());

        // Queue depths per processor
        var queueStats = _fab.GetQueueStats();
        sb.AppendLine("# HELP zinc_flow_queue_visible Visible items in processor queue");
        sb.AppendLine("# TYPE zinc_flow_queue_visible gauge");
        foreach (var (name, qs) in queueStats)
            sb.Append("zinc_flow_queue_visible{processor=\"").Append(name).Append("\"} ").AppendLine(qs.GetValueOrDefault("visible", 0).ToString());

        sb.AppendLine("# HELP zinc_flow_queue_invisible Claimed/invisible items in processor queue");
        sb.AppendLine("# TYPE zinc_flow_queue_invisible gauge");
        foreach (var (name, qs) in queueStats)
            sb.Append("zinc_flow_queue_invisible{processor=\"").Append(name).Append("\"} ").AppendLine(qs.GetValueOrDefault("invisible", 0).ToString());

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
        public ValidationError(string path, string message) { Path = path; Message = message; }
        public override string ToString() => $"  {Path}: {Message}";
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
        }

        // Check routes reference valid destinations
        var routeDefs = Fabric.AsStringDict(flowDict.GetValueOrDefault("routes"));
        if (routeDefs is not null)
        {
            foreach (var (ruleName, rObj) in routeDefs)
            {
                var rDef = Fabric.AsStringDict(rObj);
                if (rDef is null) continue;

                var dest = Fabric.GetStr(rDef, "destination");
                if (string.IsNullOrEmpty(dest))
                    errors.Add(new($"flow.routes.{ruleName}.destination", "missing destination"));
                else if (!procNames.Contains(dest))
                    errors.Add(new($"flow.routes.{ruleName}.destination", $"destination '{dest}' is not a defined processor"));

                var condDict = Fabric.AsStringDict(rDef.GetValueOrDefault("condition"));
                if (condDict is null)
                    errors.Add(new($"flow.routes.{ruleName}.condition", "missing condition"));
            }
        }

        return errors;
    }
}
