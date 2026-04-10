using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;
using ZincFlow.Core;

namespace ZincFlow.Fabric;

/// <summary>
/// Management API — REST endpoints for monitoring and controlling zinc-flow.
/// Uses ASP.NET Minimal API delegates.
/// </summary>
public sealed class ApiHandler
{
    private readonly Fabric _fab;
    private string? _configPath;

    public ApiHandler(Fabric fab) => _fab = fab;

    public void SetConfigPath(string path) => _configPath = path;

    public void MapRoutes(WebApplication app)
    {
        app.MapGet("/api/stats", Stats);
        app.MapGet("/api/processors", Processors);
        app.MapGet("/api/processor-stats", ProcessorStats);
        app.MapGet("/api/registry", RegistryList);
        app.MapGet("/api/flow", Flow);
        app.MapGet("/api/connections", Connections);
        app.MapPost("/api/processors/add", AddProcessor);
        app.MapDelete("/api/processors/remove", RemoveProcessor);
        app.MapPost("/api/processors/enable", EnableProcessor);
        app.MapPost("/api/processors/disable", DisableProcessor);
        app.MapPost("/api/processors/state", ProcessorState);
        app.MapGet("/api/providers", Providers);
        app.MapPost("/api/providers/enable", EnableProvider);
        app.MapPost("/api/providers/disable", DisableProvider);

        app.MapGet("/health", Health);
        app.MapGet("/api/provenance", ProvenanceRecent);
        app.MapGet("/api/provenance/{id}", ProvenanceById);

        // Hot reload
        app.MapPost("/api/reload", Reload);

        // Connector source lifecycle
        app.MapGet("/api/sources", Sources);
        app.MapPost("/api/sources/start", StartSource);
        app.MapPost("/api/sources/stop", StopSource);
    }

    // --- Stats ---

    private IResult Stats() => Results.Json(_fab.GetStats());
    private IResult Processors() => Results.Json(_fab.GetProcessorNames());
    private IResult ProcessorStats() => Results.Json(_fab.GetProcessorStats());

    private IResult RegistryList()
    {
        var infos = _fab.GetRegistry().List();
        return Results.Json(infos.Select(i => new
        {
            name = i.Name,
            description = i.Description,
            configKeys = i.ConfigKeys
        }));
    }

    private IResult Connections()
    {
        return Results.Json(_fab.GetConnections());
    }

    private IResult Flow()
    {
        var procStats = _fab.GetProcessorStats();
        return Results.Json(new
        {
            processors = _fab.GetProcessorNames().Select(name => new
            {
                name,
                state = _fab.GetProcessorState(name).ToString().ToUpperInvariant(),
                stats = procStats.GetValueOrDefault(name),
                connections = _fab.GetConnections().GetValueOrDefault(name)
            }),
            entryPoints = _fab.GetEntryPoints(),
            sources = _fab.GetSources().Select(s => new { name = s.Name, type = s.Type, running = s.Running }),
            providers = _fab.GetContext().ListProviders().Select(name =>
            {
                var p = _fab.GetContext().GetProvider(name);
                return new { name, type = p?.ProviderType ?? "unknown", state = p?.State.ToString().ToUpperInvariant() ?? "UNKNOWN" };
            }),
            stats = _fab.GetStats()
        });
    }

    // --- Processor management ---

    private async Task<IResult> AddProcessor(HttpContext ctx)
    {
        var req = await ctx.Request.ReadFromJsonAsync<Dictionary<string, object?>>();
        if (req is null) return Results.BadRequest(new { error = "invalid json" });
        var name = req.GetValueOrDefault("name")?.ToString() ?? "";
        var type = req.GetValueOrDefault("type")?.ToString() ?? "";
        if (name == "" || type == "") return Results.BadRequest(new { error = "name and type required" });

        var config = new Dictionary<string, string>();
        if (req.TryGetValue("config", out var cfgObj) && cfgObj is System.Text.Json.JsonElement je)
        {
            foreach (var prop in je.EnumerateObject())
                config[prop.Name] = prop.Value.ToString();
        }

        return _fab.AddProcessor(name, type, config)
            ? Results.Created($"/api/processors/{name}", new { status = "created", name })
            : Results.Conflict(new { error = "processor already exists or unknown type" });
    }

    private async Task<IResult> RemoveProcessor(HttpContext ctx)
    {
        var req = await ctx.Request.ReadFromJsonAsync<Dictionary<string, string>>();
        var name = req?.GetValueOrDefault("name") ?? "";
        if (name == "") return Results.BadRequest(new { error = "name required" });
        return _fab.RemoveProcessor(name)
            ? Results.Json(new { status = "removed", name })
            : Results.NotFound(new { error = "processor not found" });
    }

    private async Task<IResult> EnableProcessor(HttpContext ctx)
    {
        var req = await ctx.Request.ReadFromJsonAsync<Dictionary<string, string>>();
        var name = req?.GetValueOrDefault("name") ?? "";
        if (name == "") return Results.BadRequest(new { error = "name required" });
        return _fab.EnableProcessor(name)
            ? Results.Json(new { status = "enabled", name })
            : Results.NotFound(new { error = "processor not found" });
    }

    private async Task<IResult> DisableProcessor(HttpContext ctx)
    {
        var req = await ctx.Request.ReadFromJsonAsync<Dictionary<string, string>>();
        var name = req?.GetValueOrDefault("name") ?? "";
        if (name == "") return Results.BadRequest(new { error = "name required" });
        return _fab.DisableProcessor(name)
            ? Results.Json(new { status = "disabled", name })
            : Results.NotFound(new { error = "processor not found" });
    }

    private async Task<IResult> ProcessorState(HttpContext ctx)
    {
        var req = await ctx.Request.ReadFromJsonAsync<Dictionary<string, string>>();
        var name = req?.GetValueOrDefault("name") ?? "";
        if (name == "") return Results.BadRequest(new { error = "name required" });
        return Results.Json(new { name, state = _fab.GetProcessorState(name).ToString().ToUpperInvariant() });
    }

    // --- Providers ---

    private IResult Providers()
    {
        var ctx = _fab.GetContext();
        return Results.Json(ctx.ListProviders().Select(name =>
        {
            var p = ctx.GetProvider(name);
            return new { name, type = p?.ProviderType ?? "unknown", state = p?.State.ToString().ToUpperInvariant() ?? "UNKNOWN" };
        }));
    }

    private async Task<IResult> EnableProvider(HttpContext ctx)
    {
        var req = await ctx.Request.ReadFromJsonAsync<Dictionary<string, string>>();
        var name = req?.GetValueOrDefault("name") ?? "";
        if (name == "") return Results.BadRequest(new { error = "name required" });
        return _fab.EnableProvider(name)
            ? Results.Json(new { status = "enabled", name })
            : Results.NotFound(new { error = "provider not found" });
    }

    private async Task<IResult> DisableProvider(HttpContext ctx)
    {
        var req = await ctx.Request.ReadFromJsonAsync<Dictionary<string, string>>();
        var name = req?.GetValueOrDefault("name") ?? "";
        if (name == "") return Results.BadRequest(new { error = "name required" });
        return _fab.DisableProvider(name)
            ? Results.Json(new { status = "disabled", name })
            : Results.NotFound(new { error = "provider not found" });
    }

    // --- Health ---

    private IResult Health()
    {
        var sources = _fab.GetSources();
        return Results.Json(new
        {
            status = "healthy",
            sources = sources.Select(s => new { name = s.Name, type = s.Type, running = s.Running })
        });
    }

    // --- Hot reload ---

    private IResult Reload()
    {
        if (_configPath is null || !File.Exists(_configPath))
            return Results.Json(new { error = "config path not set or file missing" });
        try
        {
            var yaml = File.ReadAllText(_configPath);
            var deserializer = new DeserializerBuilder()
                .WithNamingConvention(UnderscoredNamingConvention.Instance)
                .Build();
            var config = deserializer.Deserialize<Dictionary<string, object?>>(yaml) ?? new();
            var (added, removed, updated, connectionsChanged) = _fab.ReloadFlow(config);
            return Results.Json(new { status = "reloaded", added, removed, updated, connectionsChanged });
        }
        catch (Exception ex)
        {
            return Results.Json(new { error = $"reload failed: {ex.Message}" });
        }
    }

    // --- Provenance ---

    private IResult ProvenanceRecent(HttpContext ctx)
    {
        var prov = _fab.GetProvenance();
        if (prov is null) return Results.Json(new { error = "provenance provider not enabled" });
        var n = 50;
        if (ctx.Request.Query.TryGetValue("n", out var nStr) && int.TryParse(nStr, out var parsed))
            n = parsed;
        return Results.Json(prov.GetRecent(n).Select(e => new
        {
            flowfile = $"ff-{e.FlowFileId}",
            type = e.EventType.ToString(),
            component = e.Component,
            details = e.Details,
            timestamp = e.Timestamp
        }));
    }

    private IResult ProvenanceById(long id)
    {
        var prov = _fab.GetProvenance();
        if (prov is null) return Results.Json(new { error = "provenance provider not enabled" });
        return Results.Json(prov.GetEvents(id).Select(e => new
        {
            flowfile = $"ff-{e.FlowFileId}",
            type = e.EventType.ToString(),
            component = e.Component,
            details = e.Details,
            timestamp = e.Timestamp
        }));
    }

    // --- Connector sources ---

    private IResult Sources()
    {
        return Results.Json(_fab.GetSources().Select(s => new { name = s.Name, type = s.Type, running = s.Running }));
    }

    private async Task<IResult> StartSource(HttpContext ctx)
    {
        var req = await ctx.Request.ReadFromJsonAsync<Dictionary<string, string>>();
        var name = req?.GetValueOrDefault("name") ?? "";
        if (name == "") return Results.BadRequest(new { error = "name required" });
        return _fab.StartSource(name)
            ? Results.Json(new { status = "started", name })
            : Results.NotFound(new { error = "source not found" });
    }

    private async Task<IResult> StopSource(HttpContext ctx)
    {
        var req = await ctx.Request.ReadFromJsonAsync<Dictionary<string, string>>();
        var name = req?.GetValueOrDefault("name") ?? "";
        if (name == "") return Results.BadRequest(new { error = "name required" });
        return _fab.StopSource(name)
            ? Results.Json(new { status = "stopped", name })
            : Results.NotFound(new { error = "source not found" });
    }
}
