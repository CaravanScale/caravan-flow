using Microsoft.AspNetCore.Mvc;
using CaravanFlow.Core;

namespace CaravanFlow.Fabric;

/// <summary>
/// Management API — REST endpoints for monitoring and controlling caravan-flow.
/// Uses ASP.NET Minimal API delegates.
/// </summary>
public sealed class ApiHandler
{
    private readonly Fabric _fab;
    private string? _configPath;
    private Overlay.Resolved? _resolvedOverlay;

    private static IResult Json(object value)
        => Results.Content(CaravanJson.SerializeToString(value), "application/json");

    public ApiHandler(Fabric fab) => _fab = fab;

    public void SetConfigPath(string path) => _configPath = path;

    /// <summary>
    /// Installs the overlay snapshot resolved at startup so the
    /// <c>/api/overlays*</c> endpoints can surface layer provenance
    /// and write secrets through to disk. Null when the worker booted
    /// without a config file.
    /// </summary>
    public void SetResolvedOverlay(Overlay.Resolved? overlay) => _resolvedOverlay = overlay;

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

        // Overlay stack: base ← config.local.yaml ← secrets.yaml
        app.MapGet("/api/overlays", Overlays);
        app.MapPut("/api/overlays/secrets", WriteSecretsOverlay);

        // Version control (opt-in via vc.enabled=true in config.yaml)
        app.MapGet("/api/vc/status", VcStatus);
        app.MapPost("/api/vc/commit", VcCommit);
        app.MapPost("/api/vc/push", VcPush);

        // Save runtime graph to config.yaml (VC-aware — commits+pushes
        // when VersionControlProvider is enabled)
        app.MapPost("/api/flow/save", FlowSave);

        // Recent failed provenance events (errors view)
        app.MapGet("/api/provenance/failures", ProvenanceFailures);
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

    private IResult AddProcessor([FromBody] Dictionary<string, object?>? req)
    {
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

    private IResult RemoveProcessor([FromBody] Dictionary<string, string>? req)
    {
        var name = req?.GetValueOrDefault("name") ?? "";
        if (name == "") return Json(new Dictionary<string, object?> { ["error"] = "name required" });
        return _fab.RemoveProcessor(name)
            ? Json(new Dictionary<string, object?> { ["status"] = "removed", ["name"] = name })
            : Json(new Dictionary<string, object?> { ["error"] = "processor not found" });
    }

    private IResult EnableProcessor([FromBody] Dictionary<string, string>? req)
    {
        var name = req?.GetValueOrDefault("name") ?? "";
        if (name == "") return Json(new Dictionary<string, object?> { ["error"] = "name required" });
        return _fab.EnableProcessor(name)
            ? Json(new Dictionary<string, object?> { ["status"] = "enabled", ["name"] = name })
            : Json(new Dictionary<string, object?> { ["error"] = "processor not found" });
    }

    private IResult DisableProcessor([FromBody] Dictionary<string, string>? req)
    {
        var name = req?.GetValueOrDefault("name") ?? "";
        if (name == "") return Json(new Dictionary<string, object?> { ["error"] = "name required" });
        return _fab.DisableProcessor(name)
            ? Json(new Dictionary<string, object?> { ["status"] = "disabled", ["name"] = name })
            : Json(new Dictionary<string, object?> { ["error"] = "processor not found" });
    }

    private IResult ProcessorState([FromBody] Dictionary<string, string>? req)
    {
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

    private IResult EnableProvider([FromBody] Dictionary<string, string>? req)
    {
        var name = req?.GetValueOrDefault("name") ?? "";
        if (name == "") return Json(new Dictionary<string, object?> { ["error"] = "name required" });
        return _fab.EnableProvider(name)
            ? Json(new Dictionary<string, object?> { ["status"] = "enabled", ["name"] = name })
            : Json(new Dictionary<string, object?> { ["error"] = "provider not found" });
    }

    private IResult DisableProvider([FromBody] Dictionary<string, string>? req)
    {
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

    // --- Overlays ---

    private IResult Overlays()
    {
        if (_resolvedOverlay is null)
            return Json(new Dictionary<string, object?> { ["error"] = "overlay info unavailable — worker booted without a config loader" });

        var layers = new List<Dictionary<string, object?>>();
        foreach (var layer in _resolvedOverlay.Layers)
        {
            layers.Add(new Dictionary<string, object?>
            {
                ["role"] = layer.Role,
                ["path"] = layer.Path,
                ["present"] = layer.Present,
                ["size"] = layer.Content.Count
            });
        }
        return Json(new Dictionary<string, object?>
        {
            ["base"] = _resolvedOverlay.BasePath,
            ["layers"] = layers,
            ["effective"] = _resolvedOverlay.Effective,
            ["provenance"] = _resolvedOverlay.Provenance
        });
    }

    private async Task<IResult> WriteSecretsOverlay([FromBody] Dictionary<string, object?>? body)
    {
        if (_resolvedOverlay is null)
            return Json(new Dictionary<string, object?> { ["error"] = "overlay info unavailable — worker booted without a config loader" });
        if (body is null)
            return Json(new Dictionary<string, object?> { ["error"] = "request body must be a JSON object" });

        var secretsLayer = _resolvedOverlay.Layers.FirstOrDefault(l => l.Role == "secrets");
        var secretsPath = secretsLayer?.Path;
        if (string.IsNullOrEmpty(secretsPath))
            return Json(new Dictionary<string, object?> { ["error"] = "no secrets path resolved" });

        try
        {
            Overlay.WriteSecrets(secretsPath, body);
            return Json(new Dictionary<string, object?>
            {
                ["status"] = "written",
                ["path"] = secretsPath,
                ["keys"] = body.Count
            });
        }
        catch (Exception ex)
        {
            return Json(new Dictionary<string, object?> { ["error"] = $"failed to write secrets: {ex.Message}" });
        }
        finally { await Task.CompletedTask; }
    }

    // --- Version control (opt-in via vc.enabled=true in config.yaml) ---

    private IResult VcStatus()
    {
        var vc = ResolveVcProvider();
        if (vc is null)
            return Json(new Dictionary<string, object?> { ["enabled"] = false });
        return Json(vc.StatusJson());
    }

    private IResult VcCommit([FromBody] Dictionary<string, object?>? body)
    {
        var vc = ResolveVcProvider();
        if (vc is null)
            return Json(new Dictionary<string, object?> { ["error"] = "vc provider not enabled" });

        var message = body is not null && body.TryGetValue("message", out var m) ? m?.ToString() ?? "" : "";
        if (string.IsNullOrWhiteSpace(message))
            return Json(new Dictionary<string, object?> { ["error"] = "commit message must not be blank" });

        var path = body is not null && body.TryGetValue("path", out var p) ? p?.ToString() : null;
        var result = vc.Commit(path, message);
        return CommandResultJson(result);
    }

    private IResult VcPush()
    {
        var vc = ResolveVcProvider();
        if (vc is null)
            return Json(new Dictionary<string, object?> { ["error"] = "vc provider not enabled" });
        var result = vc.Push();
        return CommandResultJson(result);
    }

    /// <summary>Write runtime graph to config.yaml, optionally commit via VC.</summary>
    private IResult FlowSave([FromBody] Dictionary<string, object?>? body)
    {
        if (string.IsNullOrEmpty(_configPath))
            return Json(new Dictionary<string, object?> { ["error"] = "config path not set — worker booted without a config file" });

        // Serialize the runtime graph to the canonical separated YAML shape.
        byte[] bytes;
        try
        {
            var graphMap = _fab.ExportToConfig();
            bytes = YamlEmitter.Emit(graphMap);
        }
        catch (Exception ex)
        {
            return Json(new Dictionary<string, object?> { ["error"] = $"emit failed: {ex.Message}" });
        }

        try { File.WriteAllBytes(_configPath, bytes); }
        catch (Exception ex)
        {
            return Json(new Dictionary<string, object?> { ["error"] = $"write failed: {ex.Message}" });
        }

        var response = new Dictionary<string, object?>
        {
            ["status"] = "saved",
            ["path"] = _configPath,
            ["bytes"] = bytes.Length,
            ["committed"] = false,
            ["pushed"] = false,
        };

        // VC-aware: if a VersionControlProvider is enabled, also commit +
        // push. Push defaults to true; body can opt out with {push: false}
        // or override the commit message with {message: "..."}.
        var vc = ResolveVcProvider();
        if (vc is not null)
        {
            var message = body is not null && body.TryGetValue("message", out var m) && m is not null
                ? m.ToString()! : "flow: update via UI";
            var pushFlag = !(body is not null && body.TryGetValue("push", out var pObj)
                             && pObj is bool pb && pb == false);

            var commitResult = vc.Commit(Path.GetFileName(_configPath), message);
            response["committed"] = commitResult.Ok;
            response["commitExitCode"] = commitResult.ExitCode;
            response["commitStdout"] = commitResult.Stdout;
            if (!commitResult.Ok) response["commitStderr"] = commitResult.Stderr;

            if (commitResult.Ok && pushFlag)
            {
                var pushResult = vc.Push();
                response["pushed"] = pushResult.Ok;
                response["pushExitCode"] = pushResult.ExitCode;
                response["pushStdout"] = pushResult.Stdout;
                if (!pushResult.Ok) response["pushStderr"] = pushResult.Stderr;
            }
        }

        return Json(response);
    }

    private VersionControlProvider? ResolveVcProvider()
    {
        var provider = _fab.GetContext().GetProvider(VersionControlProvider.NameConst);
        return provider is VersionControlProvider vc && vc.IsEnabled ? vc : null;
    }

    private static IResult CommandResultJson(VersionControlProvider.CommandResult r)
    {
        return Json(new Dictionary<string, object?>
        {
            ["ok"] = r.Ok,
            ["exitCode"] = r.ExitCode,
            ["stdout"] = r.Stdout,
            ["stderr"] = r.Stderr,
        });
    }

    // --- Provenance ---

    private IResult ProvenanceRecent(HttpContext ctx)
    {
        var prov = _fab.GetProvenance();
        if (prov is null) return Json(new Dictionary<string, object?> { ["error"] = "provenance provider not enabled" });
        var n = 50;
        if (ctx.Request.Query.TryGetValue("n", out var nStr) && !string.IsNullOrEmpty(nStr))
        {
            if (!int.TryParse(nStr, out var parsed))
                return Results.BadRequest(new Dictionary<string, object?> { ["error"] = $"query param 'n' is not an integer: '{nStr}'" });
            n = parsed;
        }
        return Json(prov.GetRecent(n).Select(e => new Dictionary<string, object?>
        {
            ["flowfile"] = $"ff-{e.FlowFileId}",
            ["type"] = e.EventType.ToString(),
            ["component"] = e.Component,
            ["details"] = e.Details,
            ["timestamp"] = e.Timestamp
        }).ToList());
    }

    /// <summary>
    /// Filtered view of recent provenance events limited to the
    /// Failed type. Over-fetches the ring buffer so small n values
    /// still return a meaningful window when most events are
    /// successful. Powers the Errors view.
    /// </summary>
    private IResult ProvenanceFailures(HttpContext ctx)
    {
        var prov = _fab.GetProvenance();
        if (prov is null)
            return Json(new Dictionary<string, object?> { ["error"] = "provenance provider not enabled" });
        var n = 50;
        if (ctx.Request.Query.TryGetValue("n", out var nStr) && !string.IsNullOrEmpty(nStr))
        {
            if (!int.TryParse(nStr, out var parsed))
                return Results.BadRequest(new Dictionary<string, object?> { ["error"] = $"query param 'n' is not an integer: '{nStr}'" });
            n = parsed;
        }

        // Scan a larger window so low n still finds failures when the
        // success:failure ratio is skewed (typical operational case).
        var window = Math.Max(n * 20, 500);
        var failures = new List<Dictionary<string, object?>>(n);
        foreach (var e in prov.GetRecent(window))
        {
            if (e.EventType != ProvenanceEventType.Failed) continue;
            failures.Add(new Dictionary<string, object?>
            {
                ["flowfile"] = $"ff-{e.FlowFileId}",
                ["type"] = e.EventType.ToString(),
                ["component"] = e.Component,
                ["details"] = e.Details,
                ["timestamp"] = e.Timestamp,
            });
            if (failures.Count >= n) break;
        }
        return Json(failures);
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

    private IResult StartSource([FromBody] Dictionary<string, string>? req)
    {
        var name = req?.GetValueOrDefault("name") ?? "";
        if (name == "") return Json(new Dictionary<string, object?> { ["error"] = "name required" });
        return _fab.StartSource(name)
            ? Json(new Dictionary<string, object?> { ["status"] = "started", ["name"] = name })
            : Json(new Dictionary<string, object?> { ["error"] = "source not found" });
    }

    private IResult StopSource([FromBody] Dictionary<string, string>? req)
    {
        var name = req?.GetValueOrDefault("name") ?? "";
        if (name == "") return Json(new Dictionary<string, object?> { ["error"] = "name required" });
        return _fab.StopSource(name)
            ? Json(new Dictionary<string, object?> { ["status"] = "stopped", ["name"] = name })
            : Json(new Dictionary<string, object?> { ["error"] = "source not found" });
    }
}
