using ZincFlow.Core;

namespace ZincFlow.Fabric;

/// <summary>
/// Management API — REST endpoints for monitoring and controlling zinc-flow.
/// Uses ASP.NET Minimal API delegates.
/// </summary>
public sealed class ApiHandler
{
    private readonly Fabric _fab;

    public ApiHandler(Fabric fab) => _fab = fab;

    public void MapRoutes(WebApplication app)
    {
        app.MapGet("/api/stats", Stats);
        app.MapGet("/api/processors", Processors);
        app.MapGet("/api/registry", RegistryList);
        app.MapGet("/api/routes", Routes);
        app.MapGet("/api/flow", Flow);
        app.MapGet("/api/queues", Queues);
        app.MapGet("/api/dlq", DlqList);
        app.MapPost("/api/dlq/replay", DlqReplay);
        app.MapPost("/api/dlq/replay-all", DlqReplayAll);
        app.MapDelete("/api/dlq/delete", DlqDelete);
        app.MapPost("/api/processors/add", AddProcessor);
        app.MapDelete("/api/processors/remove", RemoveProcessor);
        app.MapPost("/api/processors/enable", EnableProcessor);
        app.MapPost("/api/processors/disable", DisableProcessor);
        app.MapPost("/api/processors/state", ProcessorState);
        app.MapPost("/api/routes/add", AddRoute);
        app.MapDelete("/api/routes/remove", RemoveRoute);
        app.MapPut("/api/routes/toggle", ToggleRoute);
        app.MapGet("/api/providers", Providers);
        app.MapPost("/api/providers/enable", EnableProvider);
        app.MapPost("/api/providers/disable", DisableProvider);
    }

    // --- Stats ---

    private IResult Stats() => Results.Json(_fab.GetStats());
    private IResult Processors() => Results.Json(_fab.GetProcessorNames());
    private IResult Queues() => Results.Json(_fab.GetQueueStats());

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

    private IResult Routes()
    {
        var rules = _fab.GetEngine().GetAllRules();
        return Results.Json(rules.Select(r => new
        {
            name = r.Name,
            enabled = r.Enabled,
            destination = r.Destination,
            attribute = r.Attribute,
            @operator = r.Operator.ToString()
        }));
    }

    private IResult Flow() => Results.Json(new
    {
        processors = _fab.GetProcessorNames(),
        routes = _fab.GetEngine().GetAllRules().Select(r => new
        {
            name = r.Name,
            enabled = r.Enabled,
            destination = r.Destination
        }),
        stats = _fab.GetStats()
    });

    // --- DLQ ---

    private IResult DlqList()
    {
        var dlq = _fab.GetDLQ();
        var entries = dlq.ListEntries();
        return Results.Json(new
        {
            count = dlq.Count,
            entries = entries.Select(e => new
            {
                id = e.Id,
                sourceProcessor = e.SourceProcessor,
                sourceQueue = e.SourceQueue,
                attemptCount = e.AttemptCount,
                lastError = e.LastError,
                flowFileId = e.FlowFile.Id
            })
        });
    }

    private async Task<IResult> DlqReplay(HttpContext ctx)
    {
        var req = await ctx.Request.ReadFromJsonAsync<Dictionary<string, string>>();
        var id = req?.GetValueOrDefault("id") ?? "";
        if (id == "") return Results.BadRequest(new { error = "id is required" });

        var dlq = _fab.GetDLQ();
        var entry = dlq.Get(id);
        if (entry is null) return Results.NotFound(new { error = "dlq entry not found" });

        var ff = dlq.Replay(id);
        if (ff is not null) _fab.ReplayToQueue(entry.SourceQueue, ff);
        return Results.Json(new { status = "replayed", id, flowFileId = ff?.Id, queue = entry.SourceQueue });
    }

    private IResult DlqReplayAll()
    {
        var entries = _fab.GetDLQ().ReplayAll();
        foreach (var entry in entries)
            _fab.ReplayToQueue(entry.SourceQueue, entry.FlowFile);
        return Results.Json(new { status = "replayed", count = entries.Count });
    }

    private async Task<IResult> DlqDelete(HttpContext ctx)
    {
        var req = await ctx.Request.ReadFromJsonAsync<Dictionary<string, string>>();
        var id = req?.GetValueOrDefault("id") ?? "";
        if (id == "") return Results.BadRequest(new { error = "id is required" });
        _fab.GetDLQ().Remove(id);
        return Results.Json(new { status = "removed", id });
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
        return _fab.DisableProcessor(name, _fab.GetStats().GetValueOrDefault("drain_timeout", 60))
            ? Results.Json(new { status = "draining", name })
            : Results.NotFound(new { error = "processor not found" });
    }

    private async Task<IResult> ProcessorState(HttpContext ctx)
    {
        var req = await ctx.Request.ReadFromJsonAsync<Dictionary<string, string>>();
        var name = req?.GetValueOrDefault("name") ?? "";
        if (name == "") return Results.BadRequest(new { error = "name required" });
        return Results.Json(new { name, state = _fab.GetProcessorState(name).ToString().ToUpperInvariant() });
    }

    // --- Route management ---

    private async Task<IResult> AddRoute(HttpContext ctx)
    {
        var req = await ctx.Request.ReadFromJsonAsync<Dictionary<string, string>>();
        if (req is null) return Results.BadRequest(new { error = "invalid json" });
        var name = req.GetValueOrDefault("name") ?? "";
        var attr = req.GetValueOrDefault("attribute") ?? "";
        var op = req.GetValueOrDefault("operator") ?? "";
        var val = req.GetValueOrDefault("value") ?? "";
        var dest = req.GetValueOrDefault("destination") ?? "";
        if (name == "" || attr == "" || dest == "")
            return Results.BadRequest(new { error = "name, attribute, and destination required" });
        _fab.AddRoute(name, attr, op, val, dest);
        return Results.Created($"/api/routes/{name}", new { status = "created", name });
    }

    private async Task<IResult> RemoveRoute(HttpContext ctx)
    {
        var req = await ctx.Request.ReadFromJsonAsync<Dictionary<string, string>>();
        var name = req?.GetValueOrDefault("name") ?? "";
        if (name == "") return Results.BadRequest(new { error = "name required" });
        return _fab.RemoveRoute(name)
            ? Results.Json(new { status = "removed", name })
            : Results.NotFound(new { error = "route not found" });
    }

    private async Task<IResult> ToggleRoute(HttpContext ctx)
    {
        var req = await ctx.Request.ReadFromJsonAsync<Dictionary<string, string>>();
        var name = req?.GetValueOrDefault("name") ?? "";
        if (name == "") return Results.BadRequest(new { error = "name required" });
        return _fab.ToggleRoute(name)
            ? Results.Json(new { status = "toggled", name })
            : Results.NotFound(new { error = "route not found" });
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
        return _fab.DisableProvider(name, 60)
            ? Results.Json(new { status = "disabled", name })
            : Results.NotFound(new { error = "provider not found" });
    }
}
