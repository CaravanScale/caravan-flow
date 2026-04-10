using System.Text.Json;
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

    // AOT-safe JSON serialization — use this instead of Json()
    private static IResult Json(object value)
        => Results.Content(JsonSerializer.Serialize(value, JsonOpts.Default), "application/json");

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

    private IResult Stats() => Json(_fab.GetStats());
    private IResult Processors() => Json(_fab.GetProcessorNames());
    private IResult ProcessorStats() => Json(_fab.GetProcessorStats());

    private IResult RegistryList()
    {
        var infos = _fab.GetRegistry().List();
        return Json(infos.Select(i => new Dictionary<string, object?>
        {
            ["name"] = i.Name,
            ["description"] = i.Description,
            ["configKeys"] = i.ConfigKeys
        }));
    }

    private IResult Connections()
    {
        return Json(_fab.GetConnections());
    }

    private IResult Flow()
    {
        var procStats = _fab.GetProcessorStats();
        return Json(new Dictionary<string, object?>
        {
            ["processors"] = _fab.GetProcessorNames().Select(name => new Dictionary<string, object?>
            {
                ["name"] = name,
                ["type"] = _fab.GetProcessorType(name),
                ["state"] = _fab.GetProcessorState(name).ToString().ToUpperInvariant(),
                ["stats"] = procStats.GetValueOrDefault(name),
                ["connections"] = _fab.GetConnections().GetValueOrDefault(name)
            }).ToList(),
            ["entryPoints"] = _fab.GetEntryPoints(),
            ["sources"] = _fab.GetSources().Select(s => new Dictionary<string, object?> { ["name"] = s.Name, ["type"] = s.Type, ["running"] = s.Running }).ToList(),
            ["providers"] = _fab.GetContext().ListProviders().Select(name =>
            {
                var p = _fab.GetContext().GetProvider(name);
                return new Dictionary<string, object?> { ["name"] = name, ["type"] = p?.ProviderType ?? "unknown", ["state"] = p?.State.ToString().ToUpperInvariant() ?? "UNKNOWN" };
            }).ToList(),
            ["stats"] = _fab.GetStats()
        });
    }

    // --- Processor management ---

    private async Task<IResult> AddProcessor(HttpContext ctx)
    {
        var req = await ctx.Request.ReadFromJsonAsync<Dictionary<string, object?>>(JsonOpts.Default);
        if (req is null) return Json(new Dictionary<string, object?> { ["error"] = "invalid json" });
        var name = req.GetValueOrDefault("name")?.ToString() ?? "";
        var type = req.GetValueOrDefault("type")?.ToString() ?? "";
        if (name == "" || type == "") return Json(new Dictionary<string, object?> { ["error"] = "name and type required" });

        var config = new Dictionary<string, string>();
        if (req.TryGetValue("config", out var cfgObj) && cfgObj is System.Text.Json.JsonElement je)
        {
            foreach (var prop in je.EnumerateObject())
                config[prop.Name] = prop.Value.ToString();
        }

        // Parse connections: { "success": ["next"], "failure": ["error"] }
        Dictionary<string, List<string>>? connections = null;
        if (req.TryGetValue("connections", out var connObj) && connObj is System.Text.Json.JsonElement connEl
            && connEl.ValueKind == System.Text.Json.JsonValueKind.Object)
        {
            connections = new();
            foreach (var prop in connEl.EnumerateObject())
            {
                var targets = new List<string>();
                if (prop.Value.ValueKind == System.Text.Json.JsonValueKind.Array)
                    foreach (var item in prop.Value.EnumerateArray())
                        targets.Add(item.GetString() ?? "");
                if (targets.Count > 0)
                    connections[prop.Name] = targets;
            }
        }

        // Parse requires: ["content", "logging"]
        List<string>? requires = null;
        if (req.TryGetValue("requires", out var reqObj) && reqObj is System.Text.Json.JsonElement reqEl
            && reqEl.ValueKind == System.Text.Json.JsonValueKind.Array)
        {
            requires = new();
            foreach (var item in reqEl.EnumerateArray())
                requires.Add(item.GetString() ?? "");
        }

        return _fab.AddProcessor(name, type, config, requires, connections)
            ? Json(new Dictionary<string, object?> { ["status"] = "created", ["name"] = name })
            : Json(new Dictionary<string, object?> { ["error"] = "processor already exists or unknown type" });
    }

    private async Task<IResult> RemoveProcessor(HttpContext ctx)
    {
        var req = await ctx.Request.ReadFromJsonAsync<Dictionary<string, string>>(JsonOpts.Default);
        var name = req?.GetValueOrDefault("name") ?? "";
        if (name == "") return Json(new Dictionary<string, object?> { ["error"] = "name required" });
        return _fab.RemoveProcessor(name)
            ? Json(new Dictionary<string, object?> { ["status"] = "removed", ["name"] = name })
            : Json(new Dictionary<string, object?> { ["error"] = "processor not found" });
    }

    private async Task<IResult> EnableProcessor(HttpContext ctx)
    {
        var req = await ctx.Request.ReadFromJsonAsync<Dictionary<string, string>>(JsonOpts.Default);
        var name = req?.GetValueOrDefault("name") ?? "";
        if (name == "") return Json(new Dictionary<string, object?> { ["error"] = "name required" });
        return _fab.EnableProcessor(name)
            ? Json(new Dictionary<string, object?> { ["status"] = "enabled", ["name"] = name })
            : Json(new Dictionary<string, object?> { ["error"] = "processor not found" });
    }

    private async Task<IResult> DisableProcessor(HttpContext ctx)
    {
        var req = await ctx.Request.ReadFromJsonAsync<Dictionary<string, string>>(JsonOpts.Default);
        var name = req?.GetValueOrDefault("name") ?? "";
        if (name == "") return Json(new Dictionary<string, object?> { ["error"] = "name required" });
        return _fab.DisableProcessor(name)
            ? Json(new Dictionary<string, object?> { ["status"] = "disabled", ["name"] = name })
            : Json(new Dictionary<string, object?> { ["error"] = "processor not found" });
    }

    private async Task<IResult> ProcessorState(HttpContext ctx)
    {
        var req = await ctx.Request.ReadFromJsonAsync<Dictionary<string, string>>(JsonOpts.Default);
        var name = req?.GetValueOrDefault("name") ?? "";
        if (name == "") return Json(new Dictionary<string, object?> { ["error"] = "name required" });
        return Json(new Dictionary<string, object?> { ["name"] = name, ["state"] = _fab.GetProcessorState(name).ToString().ToUpperInvariant() });
    }

    // --- Providers ---

    private IResult Providers()
    {
        var ctx = _fab.GetContext();
        return Json(ctx.ListProviders().Select(name =>
        {
            var p = ctx.GetProvider(name);
            return new Dictionary<string, object?> { ["name"] = name, ["type"] = p?.ProviderType ?? "unknown", ["state"] = p?.State.ToString().ToUpperInvariant() ?? "UNKNOWN" };
        }).ToList());
    }

    private async Task<IResult> EnableProvider(HttpContext ctx)
    {
        var req = await ctx.Request.ReadFromJsonAsync<Dictionary<string, string>>(JsonOpts.Default);
        var name = req?.GetValueOrDefault("name") ?? "";
        if (name == "") return Json(new Dictionary<string, object?> { ["error"] = "name required" });
        return _fab.EnableProvider(name)
            ? Json(new Dictionary<string, object?> { ["status"] = "enabled", ["name"] = name })
            : Json(new Dictionary<string, object?> { ["error"] = "provider not found" });
    }

    private async Task<IResult> DisableProvider(HttpContext ctx)
    {
        var req = await ctx.Request.ReadFromJsonAsync<Dictionary<string, string>>(JsonOpts.Default);
        var name = req?.GetValueOrDefault("name") ?? "";
        if (name == "") return Json(new Dictionary<string, object?> { ["error"] = "name required" });
        return _fab.DisableProvider(name)
            ? Json(new Dictionary<string, object?> { ["status"] = "disabled", ["name"] = name })
            : Json(new Dictionary<string, object?> { ["error"] = "provider not found" });
    }

    // --- Health ---

    private IResult Health()
    {
        var sources = _fab.GetSources();
        return Json(new Dictionary<string, object?>
        {
            ["status"] = "healthy",
            ["sources"] = sources.Select(s => new Dictionary<string, object?> { ["name"] = s.Name, ["type"] = s.Type, ["running"] = s.Running }).ToList()
        });
    }

    // --- Hot reload ---

    private IResult Reload()
    {
        if (_configPath is null || !File.Exists(_configPath))
            return Json(new Dictionary<string, object?> { ["error"] = "config path not set or file missing" });
        try
        {
            var yaml = File.ReadAllText(_configPath);
            var config = YamlParser.Parse(yaml);
            var (added, removed, updated, connectionsChanged) = _fab.ReloadFlow(config);
            return Json(new Dictionary<string, object?> { ["status"] = "reloaded", ["added"] = added, ["removed"] = removed, ["updated"] = updated, ["connectionsChanged"] = connectionsChanged });
        }
        catch (Exception ex)
        {
            return Json(new Dictionary<string, object?> { ["error"] = $"reload failed: {ex.Message}" });
        }
    }

    // --- Provenance ---

    private IResult ProvenanceRecent(HttpContext ctx)
    {
        var prov = _fab.GetProvenance();
        if (prov is null) return Json(new Dictionary<string, object?> { ["error"] = "provenance provider not enabled" });
        var n = 50;
        if (ctx.Request.Query.TryGetValue("n", out var nStr) && int.TryParse(nStr, out var parsed))
            n = parsed;
        return Json(prov.GetRecent(n).Select(e => new Dictionary<string, object?>
        {
            ["flowfile"] = $"ff-{e.FlowFileId}",
            ["type"] = e.EventType.ToString(),
            ["component"] = e.Component,
            ["details"] = e.Details,
            ["timestamp"] = e.Timestamp
        }).ToList());
    }

    private IResult ProvenanceById(long id)
    {
        var prov = _fab.GetProvenance();
        if (prov is null) return Json(new Dictionary<string, object?> { ["error"] = "provenance provider not enabled" });
        return Json(prov.GetEvents(id).Select(e => new Dictionary<string, object?>
        {
            ["flowfile"] = $"ff-{e.FlowFileId}",
            ["type"] = e.EventType.ToString(),
            ["component"] = e.Component,
            ["details"] = e.Details,
            ["timestamp"] = e.Timestamp
        }).ToList());
    }

    // --- Connector sources ---

    private IResult Sources()
    {
        return Json(_fab.GetSources().Select(s => new Dictionary<string, object?> { ["name"] = s.Name, ["type"] = s.Type, ["running"] = s.Running }).ToList());
    }

    private async Task<IResult> StartSource(HttpContext ctx)
    {
        var req = await ctx.Request.ReadFromJsonAsync<Dictionary<string, string>>(JsonOpts.Default);
        var name = req?.GetValueOrDefault("name") ?? "";
        if (name == "") return Json(new Dictionary<string, object?> { ["error"] = "name required" });
        return _fab.StartSource(name)
            ? Json(new Dictionary<string, object?> { ["status"] = "started", ["name"] = name })
            : Json(new Dictionary<string, object?> { ["error"] = "source not found" });
    }

    private async Task<IResult> StopSource(HttpContext ctx)
    {
        var req = await ctx.Request.ReadFromJsonAsync<Dictionary<string, string>>(JsonOpts.Default);
        var name = req?.GetValueOrDefault("name") ?? "";
        if (name == "") return Json(new Dictionary<string, object?> { ["error"] = "name required" });
        return _fab.StopSource(name)
            ? Json(new Dictionary<string, object?> { ["status"] = "stopped", ["name"] = name })
            : Json(new Dictionary<string, object?> { ["error"] = "source not found" });
    }
}
