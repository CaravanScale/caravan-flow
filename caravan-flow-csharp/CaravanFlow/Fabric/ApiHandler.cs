using Microsoft.AspNetCore.Mvc;
using CaravanFlow.Core;
using CaravanFlow.StdLib;

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
    private SourceRegistry? _sourceRegistry;
    private IContentStore? _contentStore;

    private static IResult Json(object value)
        => Results.Content(CaravanJson.SerializeToString(value), "application/json");

    public ApiHandler(Fabric fab) => _fab = fab;

    public void SetConfigPath(string path) => _configPath = path;

    /// <summary>
    /// Wire the source registry + content store so the API can list
    /// source types alongside processors in <c>/api/registry</c> and add
    /// new source instances at runtime via <c>/api/sources/add</c>.
    /// </summary>
    public void SetSourceRegistry(SourceRegistry sourceRegistry, IContentStore contentStore)
    {
        _sourceRegistry = sourceRegistry;
        _contentStore = contentStore;
    }

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
        app.MapGet("/readyz", Readyz);
        app.MapGet("/api/provenance", ProvenanceRecent);
        app.MapGet("/api/provenance/{id}", ProvenanceById);

        // Hot reload
        app.MapPost("/api/reload", Reload);

        // Connector source lifecycle
        app.MapGet("/api/sources", Sources);
        app.MapPost("/api/sources/start", StartSource);
        app.MapPost("/api/sources/stop", StopSource);

        // Overlay stack: base ← config.local.yaml ← (legacy) secrets.yaml.
        // The secrets layer is read-only here — secrets move to env vars;
        // the on-disk write endpoint was retired.
        app.MapGet("/api/overlays", Overlays);

        // Version control (opt-in via vc.enabled=true in config.yaml)
        app.MapGet("/api/vc/status", VcStatus);
        app.MapPost("/api/vc/commit", VcCommit);
        app.MapPost("/api/vc/push", VcPush);

        // Save runtime graph to config.yaml (VC-aware — commits+pushes
        // when VersionControlProvider is enabled)
        app.MapPost("/api/flow/save", FlowSave);
        app.MapGet("/api/flow/status", FlowStatus);

        // Live-motion edge stats (caravan-in-motion)
        app.MapGet("/api/edge-stats", EdgeStats);

        // Per-processor sample ring (Peek — feeds drawer sample tab + wizards)
        app.MapGet("/api/processors/{name}/samples", ProcessorSamples);

        // Expression builder live-parse endpoint (Phase D of the wizard push).
        // Single source of truth — wizards evaluate via the same engine
        // processors use, so "it preview-ran green" guarantees runtime parity.
        app.MapPost("/api/expression/parse", ExpressionParse);

        // Sibling layout file: per-deployment shared view (node positions).
        // Stored alongside config.yaml — loaded by the UI as a fallback when
        // localStorage is empty (new browser / teammate). Individual users
        // can still override by dragging, and their localStorage wins.
        app.MapGet("/api/layout", GetLayout);
        app.MapPost("/api/layout", SaveLayout);

        // Recent failed provenance events (errors view)
        app.MapGet("/api/provenance/failures", ProvenanceFailures);

        // Graph mutation — runtime only; POST /api/flow/save persists
        app.MapPut("/api/processors/{name}/config", UpdateProcessorConfig);
        app.MapPut("/api/processors/{name}/connections", SetProcessorConnections);
        app.MapPost("/api/connections", AddConnection);
        app.MapDelete("/api/connections", DeleteConnection);
        app.MapPut("/api/connections/{from}", SetConnections);
        // /api/entrypoints removed — explicit sources with explicit
        // outbound connections replace the entry-points list entirely.

        // Provider config edits (new on both tracks)
        app.MapPut("/api/providers/{name}/config", UpdateProviderConfig);

        // Per-processor stats reset
        app.MapPost("/api/processors/{name}/stats/reset", ResetProcessorStats);

        // Test flowfile injection (push a synthetic FlowFile at an entry point)
        app.MapPost("/api/flowfiles/ingest", IngestFlowFile);

        // Add a new source instance at runtime (palette drop target)
        app.MapPost("/api/sources/add", AddSource);
        // Update a running source's config (drawer save)
        app.MapPut("/api/sources/{name}/config", UpdateSourceConfig);
        // Remove a source instance (drawer delete)
        app.MapDelete("/api/sources/remove", RemoveSource);
    }

    private IResult RemoveSource(HttpRequest request, [FromBody] Dictionary<string, object?>? req)
    {
        var name = request.Query["name"].FirstOrDefault() ?? req?.GetValueOrDefault("name")?.ToString() ?? "";
        if (string.IsNullOrEmpty(name))
            return Json(new Dictionary<string, object?> { ["error"] = "name required" });
        return _fab.RemoveSource(name)
            ? Json(new Dictionary<string, object?> { ["status"] = "removed", ["name"] = name })
            : Json(new Dictionary<string, object?> { ["error"] = $"source '{name}' not found" });
    }

    // --- Stats ---

    private IResult Stats() => Json(_fab.GetStats());
    private IResult Processors() => Json(_fab.GetProcessorNames());
    private IResult ProcessorStats() => Json(_fab.GetProcessorStats());

    private IResult RegistryList()
    {
        var results = new List<Dictionary<string, object?>>();

        foreach (var i in _fab.GetRegistry().List())
        {
            results.Add(new Dictionary<string, object?>
            {
                ["name"] = i.Name,
                ["description"] = i.Description,
                ["category"] = i.Category,
                ["kind"] = "processor",
                ["wizardComponent"] = i.WizardComponent,
                ["configKeys"] = i.ConfigKeys,
                ["parameters"] = i.Parameters.Select(p => new Dictionary<string, object?>
                {
                    ["name"] = p.Name,
                    ["label"] = p.Label,
                    ["description"] = p.Description,
                    ["kind"] = p.Kind.ToString(),
                    ["required"] = p.Required,
                    ["default"] = p.Default,
                    ["placeholder"] = p.Placeholder,
                    ["choices"] = p.Choices,
                    ["valueKind"] = p.ValueKind?.ToString(),
                    ["entryDelim"] = p.EntryDelim,
                    ["pairDelim"] = p.PairDelim,
                }).ToList()
            });
        }

        // Source types share the palette — in a visual-FP tool a source
        // is just a processor with no inbound edges. We tag the entry
        // kind="source" so the UI drop handler picks the right add endpoint.
        if (_sourceRegistry is not null)
        {
            foreach (var s in _sourceRegistry.List())
            {
                results.Add(new Dictionary<string, object?>
                {
                    ["name"] = s.TypeName,
                    ["description"] = s.Description,
                    ["category"] = "Source",
                    ["kind"] = "source",
                    ["configKeys"] = s.ConfigKeys,
                    ["parameters"] = s.Parameters.Select(p => new Dictionary<string, object?>
                    {
                        ["name"] = p.Name,
                        ["label"] = p.Label,
                        ["description"] = p.Description,
                        ["kind"] = p.Kind.ToString(),
                        ["required"] = p.Required,
                        ["default"] = p.Default,
                        ["placeholder"] = p.Placeholder,
                        ["choices"] = p.Choices,
                        ["valueKind"] = p.ValueKind?.ToString(),
                        ["entryDelim"] = p.EntryDelim,
                        ["pairDelim"] = p.PairDelim,
                    }).ToList()
                });
            }
        }

        return Json(results);
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
            ["sources"] = _fab.GetSources().Select(s => new Dictionary<string, object?>
            {
                ["name"] = s.Name,
                ["type"] = s.Type,
                ["running"] = s.Running,
                ["connections"] = _fab.GetSourceConnections().GetValueOrDefault(s.Name),
                ["config"] = _fab.GetSourceConfig(s.Name),
            }).ToList(),
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

        if (_fab.TryAddProcessor(name, type, config, requires, connections, out var err))
            return Json(new Dictionary<string, object?> { ["status"] = "created", ["name"] = name });
        return Results.Content(
            CaravanJson.SerializeToString(new Dictionary<string, object?> { ["error"] = err ?? "add failed" }),
            "application/json", statusCode: 400);
    }

    private IResult RemoveProcessor(HttpRequest request, [FromBody] Dictionary<string, string>? req)
    {
        // Accept both ?name= query (what the React client sends for DELETE)
        // and {"name": "..."} body (what command-line curl users do). DELETE
        // with a body is spec-valid but a handful of clients strip it.
        var name = request.Query["name"].FirstOrDefault() ?? req?.GetValueOrDefault("name") ?? "";
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
        // Liveness: the process is alive. Distinct from /readyz which
        // checks whether the pipeline is loaded + sources started.
        var sources = _fab.GetSources();
        return Json(new Dictionary<string, object?>
        {
            ["status"] = "healthy",
            ["sources"] = sources.Select(s => new Dictionary<string, object?> { ["name"] = s.Name, ["type"] = s.Type, ["running"] = s.Running }).ToList()
        });
    }

    private IResult Readyz()
    {
        // Readiness = process alive + graph loaded (≥1 processor) + every
        // registered source is actually running. K8s uses this to gate
        // service traffic until the worker is in a position to do work.
        var procCount = _fab.GetProcessorNames().Count;
        var sources = _fab.GetSources();
        var notRunning = sources.Where(s => !s.Running).Select(s => s.Name).ToList();
        var ready = procCount > 0 && notRunning.Count == 0;
        var body = new Dictionary<string, object?>
        {
            ["ready"] = ready,
            ["processors"] = procCount,
            ["sourcesTotal"] = sources.Count,
            ["sourcesNotRunning"] = notRunning,
        };
        if (ready)
            return Results.Content(CaravanJson.SerializeToString(body), "application/json", statusCode: 200);
        return Results.Content(CaravanJson.SerializeToString(body), "application/json", statusCode: 503);
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

        // Writing to disk clears the dirty flag regardless of commit outcome.
        _fab.MarkSaved();

        var response = new Dictionary<string, object?>
        {
            ["status"] = "saved",
            ["path"] = _configPath,
            ["bytes"] = bytes.Length,
            ["committed"] = false,
            ["pushed"] = false,
        };

        // Default behavior preserves back-compat: commit+push when VC is
        // enabled. New {commit: false} in the request body opts out so
        // the UI's debounced auto-save doesn't spam git history.
        var commitRequested = !(body is not null && body.TryGetValue("commit", out var cObj)
                                && cObj is bool cb && cb == false);
        if (!commitRequested)
            return Json(response);

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

    private string LayoutPath() =>
        string.IsNullOrEmpty(_configPath)
            ? ""
            : Path.Combine(Path.GetDirectoryName(_configPath) ?? "", "layout.yaml");

    private IResult GetLayout()
    {
        var path = LayoutPath();
        if (string.IsNullOrEmpty(path) || !File.Exists(path))
            return Json(new Dictionary<string, object?> { ["positions"] = new Dictionary<string, object?>() });
        try
        {
            var yaml = File.ReadAllText(path);
            var parsed = YamlParser.Parse(yaml);
            // Expected shape: { positions: { name: { x: <num>, y: <num> } } }
            var positions = new Dictionary<string, object?>();
            if (parsed.TryGetValue("positions", out var raw) && raw is Dictionary<string, object?> posMap)
            {
                foreach (var (name, val) in posMap)
                {
                    if (val is Dictionary<string, object?> p
                        && p.TryGetValue("x", out var xo) && xo is not null
                        && p.TryGetValue("y", out var yo) && yo is not null
                        && double.TryParse(xo.ToString(), out var x)
                        && double.TryParse(yo.ToString(), out var y))
                    {
                        positions[name] = new Dictionary<string, object?> { ["x"] = x, ["y"] = y };
                    }
                }
            }
            return Json(new Dictionary<string, object?> { ["positions"] = positions, ["path"] = path });
        }
        catch (Exception ex)
        {
            return Json(new Dictionary<string, object?> { ["error"] = $"layout read failed: {ex.Message}" });
        }
    }

    private IResult SaveLayout([FromBody] Dictionary<string, object?>? body)
    {
        var path = LayoutPath();
        if (string.IsNullOrEmpty(path))
            return Json(new Dictionary<string, object?> { ["error"] = "config path not set — nowhere to write layout.yaml" });
        if (body is null || !body.TryGetValue("positions", out var posObj))
            return Json(new Dictionary<string, object?> { ["error"] = "body must be { positions: { name: { x, y } } }" });

        // [FromBody] deserializes nested objects as JsonElement inside the
        // outer Dictionary<string, object?>. Same shape fix we applied to
        // AddSource/AddProcessor: walk the JSON tree by hand.
        var clean = new Dictionary<string, object?>();
        if (posObj is System.Text.Json.JsonElement posEl
            && posEl.ValueKind == System.Text.Json.JsonValueKind.Object)
        {
            foreach (var prop in posEl.EnumerateObject())
            {
                if (prop.Value.ValueKind != System.Text.Json.JsonValueKind.Object) continue;
                double? x = null, y = null;
                foreach (var coord in prop.Value.EnumerateObject())
                {
                    if (coord.Name == "x" && coord.Value.TryGetDouble(out var xv)) x = xv;
                    else if (coord.Name == "y" && coord.Value.TryGetDouble(out var yv)) y = yv;
                }
                if (x is not null && y is not null)
                    clean[prop.Name] = new Dictionary<string, object?> { ["x"] = Math.Round(x.Value, 2), ["y"] = Math.Round(y.Value, 2) };
            }
        }
        else if (posObj is Dictionary<string, object?> legacy)
        {
            // Back-compat for any caller that sends a pre-materialized dict.
            foreach (var (name, val) in legacy)
            {
                if (val is Dictionary<string, object?> p
                    && p.TryGetValue("x", out var xo) && xo is not null
                    && p.TryGetValue("y", out var yo) && yo is not null
                    && double.TryParse(xo.ToString(), out var x)
                    && double.TryParse(yo.ToString(), out var y))
                {
                    clean[name] = new Dictionary<string, object?> { ["x"] = Math.Round(x, 2), ["y"] = Math.Round(y, 2) };
                }
            }
        }

        try
        {
            var bytes = YamlEmitter.Emit(new Dictionary<string, object?> { ["positions"] = clean });
            File.WriteAllBytes(path, bytes);
            return Json(new Dictionary<string, object?>
            {
                ["status"] = "saved",
                ["path"] = path,
                ["bytes"] = bytes.Length,
                ["positions"] = clean.Count,
            });
        }
        catch (Exception ex)
        {
            return Json(new Dictionary<string, object?> { ["error"] = $"layout write failed: {ex.Message}" });
        }
    }

    /// Parse + optional-evaluate an expression. Body:
    ///   { expression: "...", context?: { attributes: {...}, record: {...} } }
    /// Returns { ok, error?, kind?, value? }. If context.record is present,
    /// its fields merge with context.attributes into a DictValueResolver
    /// so builder previews can show the value against a real sample.
    private IResult ExpressionParse([FromBody] Dictionary<string, object?>? body)
    {
        if (body is null)
            return Json(new Dictionary<string, object?> { ["ok"] = false, ["error"] = "body required" });

        var expression = body.GetValueOrDefault("expression") as string ?? "";
        if (string.IsNullOrEmpty(expression))
            return Json(new Dictionary<string, object?> { ["ok"] = false, ["error"] = "expression must not be blank" });

        CompiledExpression compiled;
        try { compiled = ExpressionEngine.Compile(expression); }
        catch (Exception ex)
        {
            return Json(new Dictionary<string, object?>
            {
                ["ok"] = false,
                ["error"] = ex.Message,
            });
        }

        var merged = new Dictionary<string, object?>();
        if (body.GetValueOrDefault("context") is Dictionary<string, object?> ctx)
        {
            if (ctx.GetValueOrDefault("attributes") is Dictionary<string, object?> attrs)
                foreach (var (k, v) in attrs) merged[k] = v;
            if (ctx.GetValueOrDefault("record") is Dictionary<string, object?> rec)
                foreach (var (k, v) in rec) merged[k] = v;
        }

        EvalValue value;
        try { value = compiled.Eval(new DictValueResolver(merged)); }
        catch (Exception ex)
        {
            return Json(new Dictionary<string, object?>
            {
                ["ok"] = true,
                ["parse"] = "ok",
                ["eval"] = "error",
                ["error"] = ex.Message,
            });
        }

        return Json(new Dictionary<string, object?>
        {
            ["ok"] = true,
            ["kind"] = value.Type.ToString(),
            ["value"] = value.AsString(),
        });
    }

    private IResult EdgeStats()
    {
        // Keyed "from|rel|to" to match React Flow's "from::rel::to" edge ids
        // after the UI normalizes the delimiter. Single flat map so the client
        // can diff against its previous poll for motion animation.
        var shaped = new Dictionary<string, object?>();
        foreach (var (key, processed) in _fab.GetEdgeCounts())
            shaped[key] = new Dictionary<string, object?> { ["processed"] = processed };
        return Json(shaped);
    }

    private IResult ProcessorSamples(string name)
    {
        if (string.IsNullOrEmpty(name))
            return Json(new Dictionary<string, object?> { ["error"] = "name required" });
        var samples = _fab.GetSamples(name);
        var shaped = samples.Select(s => new Dictionary<string, object?>
        {
            ["timestamp"] = s.Timestamp,
            ["flowfile"] = $"ff-{s.FlowFileId}",
            ["contentType"] = s.ContentType,
            // UTF-8 preview when safe, base64 for binary. The UI decides
            // how to render based on contentType.
            ["preview"] = PreviewAsString(s.Preview, s.ContentType),
            ["previewBase64"] = s.ContentType == "records"
                ? null
                : Convert.ToBase64String(s.Preview),
            ["attributes"] = s.Attributes,
        }).ToList();
        return Json(new Dictionary<string, object?>
        {
            ["name"] = name,
            ["sampling"] = _fab.IsSamplingEnabled(),
            ["samples"] = shaped,
        });
    }

    private static string? PreviewAsString(byte[] bytes, string contentType)
    {
        if (bytes.Length == 0) return "";
        if (contentType == "records" || contentType == "claim")
            return System.Text.Encoding.UTF8.GetString(bytes);
        try { return System.Text.Encoding.UTF8.GetString(bytes); }
        catch { return null; }
    }

    private IResult FlowStatus()
    {
        var (dirty, muts, saved, savedTick) = _fab.GetDirtyState();
        return Json(new Dictionary<string, object?>
        {
            ["dirty"] = dirty,
            ["mutationCounter"] = muts,
            ["lastSavedCounter"] = saved,
            ["lastSavedAgoMs"] = savedTick == 0 ? (long?)null : Environment.TickCount64 - savedTick,
        });
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

    // --- Graph mutation: runtime state only; /api/flow/save persists to YAML ---

    private IResult UpdateProcessorConfig(string name, [FromBody] Dictionary<string, object?>? body)
    {
        if (body is null) return BadRequest("invalid json body");
        var type = body.TryGetValue("type", out var t) ? t?.ToString() : null;
        var config = ExtractStringMap(body, "config");
        return EditResultJson(
            _fab.UpdateProcessorConfig(name, type, config),
            new() { ["status"] = "updated", ["name"] = name });
    }

    private IResult SetProcessorConnections(string name, [FromBody] Dictionary<string, object?>? body)
    {
        if (body is null) return BadRequest("invalid json body");
        var rels = ExtractRelationshipMap(body);
        if (rels is null) return BadRequest("each relationship value must be a list of target names");
        return EditResultJson(
            _fab.SetConnections(name, rels),
            new() { ["status"] = "replaced", ["from"] = name, ["relationships"] = rels });
    }

    private IResult AddConnection([FromBody] Dictionary<string, object?>? body)
    {
        if (body is null) return BadRequest("invalid json body");
        var from = body.TryGetValue("from", out var f) ? f?.ToString() ?? "" : "";
        var rel = body.TryGetValue("relationship", out var r) ? r?.ToString() ?? "" : "";
        var to = body.TryGetValue("to", out var t) ? t?.ToString() ?? "" : "";
        // If `from` names a source, dispatch to the per-source connection
        // map rather than the processor connection map.
        if (_fab.GetSources().Any(s => s.Name == from))
        {
            return EditResultJson(
                _fab.AddSourceConnection(from, rel, to),
                new() { ["status"] = "added", ["from"] = from, ["relationship"] = rel, ["to"] = to });
        }
        return EditResultJson(
            _fab.AddConnection(from, rel, to),
            new() { ["status"] = "added", ["from"] = from, ["relationship"] = rel, ["to"] = to });
    }

    private IResult DeleteConnection([FromBody] Dictionary<string, object?>? body)
    {
        if (body is null) return BadRequest("invalid json body");
        var from = body.TryGetValue("from", out var f) ? f?.ToString() ?? "" : "";
        var rel = body.TryGetValue("relationship", out var r) ? r?.ToString() ?? "" : "";
        var to = body.TryGetValue("to", out var t) ? t?.ToString() ?? "" : "";
        if (_fab.GetSources().Any(s => s.Name == from))
        {
            return EditResultJson(
                _fab.RemoveSourceConnection(from, rel, to),
                new() { ["status"] = "removed", ["from"] = from, ["relationship"] = rel, ["to"] = to });
        }
        return EditResultJson(
            _fab.RemoveConnection(from, rel, to),
            new() { ["status"] = "removed", ["from"] = from, ["relationship"] = rel, ["to"] = to });
    }

    private IResult SetConnections(string from, [FromBody] Dictionary<string, object?>? body)
    {
        if (body is null) return BadRequest("invalid json body");
        var rels = ExtractRelationshipMap(body);
        if (rels is null) return BadRequest("each relationship value must be a list of target names");
        return EditResultJson(
            _fab.SetConnections(from, rels),
            new() { ["status"] = "replaced", ["from"] = from, ["relationships"] = rels });
    }

    // SetEntryPoints removed — see comment on the old route registration.

    /// <summary>
    /// Replace a provider's config. Re-creates the provider instance with
    /// the new config map; state (enabled/disabled) is preserved across
    /// the swap. Provider type is identified by name.
    /// </summary>
    private IResult UpdateProviderConfig(string name, [FromBody] Dictionary<string, object?>? body)
    {
        if (body is null) return BadRequest("invalid json body");
        var config = ExtractStringMap(body, "config");
        var result = _fab.UpdateProviderConfig(name, config);
        return EditResultJson(result, new() { ["status"] = "updated", ["name"] = name });
    }

    private static IResult EditResultJson(Fabric.EditResult result, Dictionary<string, object?> okBody)
    {
        if (result.Ok) return Json(okBody);
        var status = result.Reason.Contains("not found")
                     || result.Reason.Contains("already exists")
                     || result.Reason.Contains("unknown")
            ? 409 : 400;
        return JsonStatus(status, new Dictionary<string, object?> { ["error"] = result.Reason });
    }

    private static IResult BadRequest(string message)
        => JsonStatus(400, new Dictionary<string, object?> { ["error"] = message });

    /// Wrap CaravanJson.SerializeToString (AOT-safe source-gen) with a
    /// chosen HTTP status. Results.Json with reflection-based
    /// serialization would trip IL2026/IL3050 under AOT.
    private static IResult JsonStatus(int status, object value)
        => Results.Content(CaravanJson.SerializeToString(value), "application/json", null, status);

    // System.Text.Json leaves nested object values as JsonElement when
    // the outer container is Dictionary<string, object?>. We handle
    // both JsonElement (the common case) and already-materialized
    // collections (e.g. from tests constructing the dict directly).

    private static Dictionary<string, string> ExtractStringMap(Dictionary<string, object?> body, string key)
    {
        var result = new Dictionary<string, string>();
        if (!body.TryGetValue(key, out var raw) || raw is null) return result;
        switch (raw)
        {
            case System.Text.Json.JsonElement je when je.ValueKind == System.Text.Json.JsonValueKind.Object:
                foreach (var prop in je.EnumerateObject()) result[prop.Name] = JsonValueToString(prop.Value);
                return result;
            case Dictionary<string, object?> dict:
                foreach (var (k, v) in dict) result[k] = v?.ToString() ?? "";
                return result;
        }
        return result;
    }

    private static Dictionary<string, List<string>>? ExtractRelationshipMap(Dictionary<string, object?> body)
    {
        var rels = new Dictionary<string, List<string>>();
        foreach (var (key, value) in body)
        {
            List<string>? targets = value switch
            {
                System.Text.Json.JsonElement je when je.ValueKind == System.Text.Json.JsonValueKind.Array
                    => EnumerateStringArray(je),
                System.Collections.IEnumerable enumerable when value is not string
                    => EnumerateStrings(enumerable),
                _ => null
            };
            if (targets is null) return null;
            rels[key] = targets;
        }
        return rels;
    }

    private static List<string> EnumerateStringArray(System.Text.Json.JsonElement arr)
    {
        var out_ = new List<string>();
        foreach (var el in arr.EnumerateArray()) out_.Add(JsonValueToString(el));
        return out_;
    }

    private static List<string> EnumerateStrings(System.Collections.IEnumerable enumerable)
    {
        var out_ = new List<string>();
        foreach (var item in enumerable) if (item is not null) out_.Add(item.ToString()!);
        return out_;
    }

    private static string JsonValueToString(System.Text.Json.JsonElement value)
        => value.ValueKind switch
        {
            System.Text.Json.JsonValueKind.String => value.GetString() ?? "",
            System.Text.Json.JsonValueKind.Number => value.ToString(),
            System.Text.Json.JsonValueKind.True => "true",
            System.Text.Json.JsonValueKind.False => "false",
            System.Text.Json.JsonValueKind.Null => "",
            _ => value.GetRawText()
        };

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

    // --- Per-processor stats reset ---

    /// Runtime source creation. Accepts {name, type, config} and registers
    /// the source with the fabric. Started immediately if the fabric is
    /// already running (matches how GetFile/GenerateFlowFile boot when the
    /// worker starts after reading config.yaml).
    private IResult AddSource([FromBody] Dictionary<string, object?>? body)
    {
        if (_sourceRegistry is null || _contentStore is null)
            return Json(new Dictionary<string, object?> { ["error"] = "source registry not wired" });
        if (body is null)
            return Json(new Dictionary<string, object?> { ["error"] = "body must be a JSON object" });

        // System.Text.Json deserializes into Dictionary<string, object?> where
        // every leaf is a JsonElement, not a string. Use ToString() (which
        // JsonElement implements to return the raw JSON value) the same way
        // AddProcessor does, otherwise `as string` silently returns null.
        var name = body.GetValueOrDefault("name")?.ToString() ?? "";
        var type = body.GetValueOrDefault("type")?.ToString() ?? "";
        if (string.IsNullOrEmpty(name))
            return Json(new Dictionary<string, object?> { ["error"] = "name required" });
        if (string.IsNullOrEmpty(type))
            return Json(new Dictionary<string, object?> { ["error"] = "type required" });
        if (!_sourceRegistry.Has(type))
            return Json(new Dictionary<string, object?> { ["error"] = $"unknown source type '{type}'" });

        var cfg = new Dictionary<string, string>();
        if (body.TryGetValue("config", out var cfgObj) && cfgObj is System.Text.Json.JsonElement je
            && je.ValueKind == System.Text.Json.JsonValueKind.Object)
        {
            foreach (var prop in je.EnumerateObject())
                cfg[prop.Name] = prop.Value.ToString();
        }

        IConnectorSource? source;
        try { source = _sourceRegistry.Create(type, name, cfg, _contentStore); }
        catch (Exception ex)
        {
            return Results.Content(
                CaravanJson.SerializeToString(new Dictionary<string, object?>
                {
                    ["error"] = $"factory failed: {ex.Message}",
                }), "application/json", statusCode: 400);
        }
        if (source is null)
            return Json(new Dictionary<string, object?>
            {
                ["status"] = "skipped",
                ["name"] = name,
                ["reason"] = "factory returned null — likely required config missing"
            });

        _fab.AddSource(source, cfg);
        return Json(new Dictionary<string, object?>
        {
            ["status"] = "added",
            ["name"] = name,
            ["type"] = type,
        });
    }

    private IResult UpdateSourceConfig(string name, [FromBody] Dictionary<string, object?>? body)
    {
        if (_sourceRegistry is null || _contentStore is null)
            return Json(new Dictionary<string, object?> { ["error"] = "source registry not wired" });
        if (string.IsNullOrEmpty(name))
            return Json(new Dictionary<string, object?> { ["error"] = "name required" });
        var existing = _fab.GetSources().FirstOrDefault(s => s.Name == name);
        if (existing.Name is null)
            return Json(new Dictionary<string, object?> { ["error"] = $"source '{name}' not found" });

        var cfg = new Dictionary<string, string>();
        if (body is not null)
        {
            foreach (var (k, v) in body)
            {
                if (k == "type") continue;
                cfg[k] = v?.ToString() ?? "";
            }
        }

        var rebuilt = _sourceRegistry.Create(existing.Type, name, cfg, _contentStore);
        if (rebuilt is null)
            return Json(new Dictionary<string, object?> { ["error"] = "factory returned null — likely required config missing" });

        return _fab.ReplaceSource(rebuilt, cfg)
            ? Json(new Dictionary<string, object?> { ["status"] = "updated", ["name"] = name })
            : Json(new Dictionary<string, object?> { ["error"] = "replace failed" });
    }

    private IResult ResetProcessorStats(string name)
    {
        if (string.IsNullOrEmpty(name))
            return Json(new Dictionary<string, object?> { ["error"] = "name required" });
        return _fab.ResetProcessorStats(name)
            ? Json(new Dictionary<string, object?> { ["status"] = "reset", ["name"] = name })
            : Json(new Dictionary<string, object?> { ["error"] = "processor not found" });
    }

    // --- Test flowfile injection ---
    //
    // Accepts {target, content|contentBase64, attributes}:
    //   target        — processor name to inject at, or "*" / omitted to use
    //                   graph entry points (same fan-out as a source would do).
    //   content       — UTF-8 text payload (most convenient for UI textareas).
    //   contentBase64 — binary payload as base64 (alternative to content).
    //   attributes    — string→string attribute map (optional).
    // Returns {status, flowfile, targets} on success.
    private IResult IngestFlowFile([FromBody] Dictionary<string, object?>? body)
    {
        if (body is null)
            return Json(new Dictionary<string, object?> { ["error"] = "request body must be a JSON object" });

        // CARAVANFLOW_MAX_INGEST_BYTES caps the accepted body size. Default
        // 16 MiB; shared with ListenHTTP's per-source default so operators
        // have a single dial. 413 is the right code here.
        var maxBytes = int.TryParse(
            Environment.GetEnvironmentVariable("CARAVANFLOW_MAX_INGEST_BYTES"),
            out var m) && m > 0 ? m : 16 * 1024 * 1024;

        byte[] data;
        if (body.TryGetValue("contentBase64", out var b64) && b64 is string b64Str && b64Str.Length > 0)
        {
            try { data = Convert.FromBase64String(b64Str); }
            catch (FormatException ex) { return Json(new Dictionary<string, object?> { ["error"] = $"contentBase64 invalid: {ex.Message}" }); }
        }
        else if (body.TryGetValue("content", out var txt) && txt is string s)
        {
            data = System.Text.Encoding.UTF8.GetBytes(s);
        }
        else
        {
            data = Array.Empty<byte>();
        }

        if (data.Length > maxBytes)
        {
            return Results.Content(
                CaravanJson.SerializeToString(new Dictionary<string, object?>
                {
                    ["error"] = $"body {data.Length} bytes exceeds CARAVANFLOW_MAX_INGEST_BYTES={maxBytes}",
                }), "application/json", statusCode: 413);
        }

        var attrs = new Dictionary<string, string>();
        if (body.TryGetValue("attributes", out var a) && a is Dictionary<string, object?> aDict)
        {
            foreach (var (k, v) in aDict)
                attrs[k] = v?.ToString() ?? "";
        }

        var target = body.GetValueOrDefault("target") as string ?? "";

        var ff = FlowFile.Create(data, attrs);

        if (string.IsNullOrEmpty(target) || target == "*")
        {
            // No more "entry-points" fan-out. The test-ingest endpoint now
            // matches the runtime model: every FF enters at a named node.
            FlowFile.Return(ff);
            return Results.Content(
                CaravanJson.SerializeToString(new Dictionary<string, object?>
                {
                    ["error"] = "target required — pick a processor name; there is no implicit fan-out",
                }),
                "application/json",
                statusCode: 400);
        }
        else
        {
            var ok = _fab.Execute(ff, target);
            return Json(new Dictionary<string, object?>
            {
                ["status"] = ok ? "ingested" : "rejected",
                ["flowfile"] = $"ff-{ff.NumericId}",
                ["target"] = target,
            });
        }
    }
}
