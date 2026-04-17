using System.Diagnostics;
using System.Runtime;
using System.Text;
using CaravanFlow.Core;
using CaravanFlow.Fabric;
using CaravanFlow.StdLib;

// --- Mode selection ---
var mode = args.Length > 0 ? args[0] : "serve";

if (mode == "--bench" || mode == "bench")
{
    RunBenchmarks();
    return;
}

if (mode == "validate" || mode == "--validate")
{
    Environment.Exit(RunValidate(args.Length > 1 ? args[1] : null));
    return;
}

if (mode == "--help" || mode == "-h" || mode == "help")
{
    Console.WriteLine("caravan-flow — data flow engine");
    Console.WriteLine();
    Console.WriteLine("Usage:");
    Console.WriteLine("  caravan-flow                  Start the server using ./config.yaml (default)");
    Console.WriteLine("  caravan-flow validate [path]  Check a config without starting; exit 0 on success, 1 on errors");
    Console.WriteLine("  caravan-flow bench            Run pipeline throughput benchmarks");
    Console.WriteLine("  caravan-flow help             Show this message");
    return;
}

// --- Production server mode ---
Console.WriteLine("caravan-flow-csharp starting");

// Load config.yaml
var configPath = Path.Combine(Directory.GetCurrentDirectory(), "config.yaml");
if (!File.Exists(configPath))
    configPath = Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "config.yaml");

// Load config with optional overlays: base ← config.local.yaml ← secrets.yaml.
// Overlay paths resolve from $CARAVANFLOW_CONFIG_LOCAL / $CARAVANFLOW_SECRETS_PATH
// env vars first, then sibling-file defaults next to the base config. Missing
// files are not errors — the layer contributes an empty map. The resolved
// snapshot is kept around so `GET /api/overlays` can surface layer provenance.
Overlay.Resolved? resolvedOverlay = null;
Dictionary<string, object?> config;
if (File.Exists(configPath))
{
    resolvedOverlay = Overlay.Load(configPath);
    config = resolvedOverlay.Effective;
    Console.WriteLine($"Config loaded from {configPath}");
    foreach (var layer in resolvedOverlay.Layers)
    {
        if (layer.Role == "base") continue; // already reported above
        if (layer.Present)
            Console.WriteLine($"  overlay [{layer.Role}] {layer.Path} ({layer.Content.Count} top-level keys)");
    }
}
else
{
    config = new();
    Console.WriteLine("No config.yaml found, using defaults");
}

// Create providers
var contentDir = GetConfigString(config, "content.dir", "/tmp/caravan-flow-csharp/content");
// Not ParseInt's empty-is-default path — the fallback "256" comes from
// GetConfigString, so the string is always present here. Bad input
// must throw rather than silently use the compiled-in default.
ContentHelpers.ClaimThreshold = ConfigHelpers.ParseIntRaw(
    GetConfigString(config, "content.offloadThresholdKb", "256"),
    "content.offloadThresholdKb") * 1024;
var store = new FileContentStore(contentDir);
var cleanup = new ContentStoreCleanup(store, contentDir);
ContentStoreCleanup.Instance = cleanup;
var contentProvider = new ContentProvider("content", store);
contentProvider.Enable();

var configProvider = new ConfigProvider(config);
configProvider.Enable();

var loggingProvider = new LoggingProvider();
loggingProvider.JsonOutput = GetConfigString(config, "logging.format", "text") == "json";
loggingProvider.Enable();

var provenanceProvider = new ProvenanceProvider();
provenanceProvider.Enable();

// Build global context
var globalCtx = new ProcessorContext();
globalCtx.AddProvider(contentProvider);
globalCtx.AddProvider(configProvider);
globalCtx.AddProvider(loggingProvider);
globalCtx.AddProvider(provenanceProvider);

// Embedded schema registry — always wired (airgapped, no remote backend).
// Pre-loads from the optional `schemas:` section in config, then exposes REST
// endpoints under /api/schema-registry/* on the management port and supports
// auto-capture from incoming OCF files via ConvertOCFToRecord's
// autoRegisterSubject config.
var embeddedRegistry = new CaravanFlow.StdLib.EmbeddedSchemaRegistry();
var schemaConfigDir = File.Exists(configPath) ? Path.GetDirectoryName(Path.GetFullPath(configPath)) : null;
var schemasSection = CaravanFlow.Fabric.Fabric.AsStringDict(config.GetValueOrDefault("schemas"));
var preloaded = embeddedRegistry.LoadFromConfig(schemasSection, schemaConfigDir);
var srProvider = new SchemaRegistryProvider(embeddedRegistry);
srProvider.Enable();
globalCtx.AddProvider(srProvider);
Console.WriteLine($"[schema-registry] embedded ({preloaded} subjects preloaded)");
if (!string.IsNullOrEmpty(GetConfigString(config, "schema_registry.url", "")))
    Console.Error.WriteLine("[schema-registry] WARNING: schema_registry.url is set but ignored — this build only supports the embedded backend");

// Optional VC provider — shells out to git for config-commit + push.
// Defaults to the parent directory of the base config file as the repo
// when `vc.repo` isn't specified; that matches the common "config
// lives in a git repo" layout and avoids needing explicit paths.
if (GetConfigString(config, "vc.enabled", "false") == "true")
{
    var vcRepo = GetConfigString(config, "vc.repo",
        File.Exists(configPath) ? Path.GetDirectoryName(Path.GetFullPath(configPath)) ?? "." : ".");
    var vcGit = GetConfigString(config, "vc.git", "git");
    var vcRemote = GetConfigString(config, "vc.remote", "origin");
    var vcBranch = GetConfigString(config, "vc.branch", "main");
    var vcProvider = new VersionControlProvider(vcRepo, vcGit, vcRemote, vcBranch);
    vcProvider.Enable();
    globalCtx.AddProvider(vcProvider);
    Console.WriteLine($"[vc] enabled repo={vcRepo} remote={vcRemote} branch={vcBranch}");
}

// Create registry
var reg = new Registry();
BuiltinProcessors.RegisterAll(reg);
Console.WriteLine($"Registry loaded: {reg.List().Count} processor types");

// Validate config before loading
var validationErrors = ConfigValidator.Validate(config, reg);
if (validationErrors.Count > 0)
{
    Console.Error.WriteLine("Config validation errors:");
    foreach (var err in validationErrors)
        Console.Error.WriteLine(err);
    Console.Error.WriteLine("Continuing with valid processors...");
}

// Create fabric and load flow
var fab = new CaravanFlow.Fabric.Fabric(reg, globalCtx);
fab.LoadFlow(config);
fab.StartAsync();
fab.Status();

// Create output directory
Directory.CreateDirectory("/tmp/caravan-flow-csharp/output");

// Config-driven sources — generic `sources:` shape:
//   sources:
//     <name>:
//       type: GetFile|GenerateFlowFile|...
//       config: { key: value, ... }
// Each named entry becomes an IConnectorSource via the registry.
// Multiple instances per type are allowed (e.g. two GetFile pollers
// on different dirs). A factory returning null signals "not
// configured" and is skipped without error.
//
// HTTP ingest is handled on the management port (POST /) below, not
// as a source.
var sourceRegistry = new SourceRegistry();
BuiltinSources.RegisterAll(sourceRegistry);

var sourcesSection = CaravanFlow.Fabric.Fabric.AsStringDict(config.GetValueOrDefault("sources"));
if (sourcesSection is not null)
{
    foreach (var (sourceName, sourceSpecObj) in sourcesSection)
    {
        var spec = CaravanFlow.Fabric.Fabric.AsStringDict(sourceSpecObj);
        if (spec is null)
        {
            Console.Error.WriteLine($"source '{sourceName}': expected map, got {sourceSpecObj?.GetType().Name}");
            continue;
        }
        var typeName = CaravanFlow.Fabric.Fabric.GetStr(spec, "type");
        if (string.IsNullOrEmpty(typeName))
        {
            Console.Error.WriteLine($"source '{sourceName}': missing 'type' key");
            continue;
        }
        if (!sourceRegistry.Has(typeName))
        {
            Console.Error.WriteLine($"source '{sourceName}': unknown type '{typeName}'");
            continue;
        }

        var cfgMap = CaravanFlow.Fabric.Fabric.AsStringDict(spec.GetValueOrDefault("config"));
        var cfgFlat = new Dictionary<string, string>();
        if (cfgMap is not null)
        {
            foreach (var (k, v) in cfgMap)
                cfgFlat[k] = v?.ToString() ?? "";
        }

        var source = sourceRegistry.Create(typeName, sourceName, cfgFlat, store);
        if (source is not null)
        {
            fab.AddSource(source);
            Console.WriteLine($"source {sourceName} ({typeName}) registered");
        }
        else
        {
            Console.WriteLine($"source {sourceName} ({typeName}) disabled by factory — skipping");
        }
    }
}

// Build ASP.NET Minimal API app
var builder = WebApplication.CreateBuilder(args);
builder.Logging.ClearProviders();
builder.Logging.AddConsole();

// Use source-generated CaravanJsonContext for AOT-safe JSON (no reflection fallback).
builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.TypeInfoResolverChain.Insert(0, CaravanFlow.Core.CaravanJsonContext.Default);
});

var port = GetConfigString(config, "server.port", "9091");
builder.WebHost.UseUrls($"http://0.0.0.0:{port}");

var app = builder.Build();

// Dashboard — serve at /
string? dashboardPath = null;
foreach (var candidate in new[]
{
    Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "dashboard.html"),      // JIT: bin/Debug/net10.0/../../..
    Path.Combine(AppContext.BaseDirectory, "..", "CaravanFlow", "dashboard.html"),      // AOT: build/../CaravanFlow/
    Path.Combine(Directory.GetCurrentDirectory(), "CaravanFlow", "dashboard.html"),     // CWD/CaravanFlow/
    Path.Combine(Directory.GetCurrentDirectory(), "dashboard.html"),                 // CWD/
})
{
    if (File.Exists(candidate)) { dashboardPath = candidate; break; }
}
if (dashboardPath is not null)
{
    var dashHtml = File.ReadAllText(dashboardPath);
    app.MapGet("/", () => Results.Content(dashHtml, "text/html"));
}

// POST / — HTTP ingest on the management port. Body is the raw
// FlowFile payload; X-Flow-* headers become FlowFile attributes.
// Mirrors caravan-flow-java's handler; wraps Fabric.IngestAndExecute
// which was already the internal ingest path used by source connectors.
app.MapPost("/", async (HttpRequest req, HttpResponse resp) =>
{
    using var ms = new MemoryStream();
    await req.Body.CopyToAsync(ms);
    var body = ms.ToArray();

    var attrs = new Dictionary<string, string>();
    foreach (var h in req.Headers)
    {
        var lower = h.Key.ToLowerInvariant();
        if (lower.StartsWith("x-flow-"))
        {
            attrs[lower.Substring("x-flow-".Length)] = h.Value.ToString();
        }
    }

    var ff = FlowFile.Create(body, attrs);
    try
    {
        fab.IngestAndExecute(ff);
        resp.StatusCode = 202;
        await resp.WriteAsync(ff.Id);
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"ingest failed for {ff.Id}: {ex.Message}");
        resp.StatusCode = 500;
        await resp.WriteAsync("pipeline error: " + ex.Message);
    }
});

// Management API — admin surface for processors/sources/providers
var api = new ApiHandler(fab);
api.SetConfigPath(configPath);
api.SetResolvedOverlay(resolvedOverlay);
api.MapRoutes(app);

// Schema registry REST endpoints (Confluent path shapes under /api/schema-registry)
new SchemaRegistryHandler(embeddedRegistry).MapRoutes(app);

// Prometheus metrics
var metrics = new MetricsHandler(fab);
app.Map("/metrics", (RequestDelegate)metrics.HandleMetrics);

// Periodic stats
_ = Task.Run(async () =>
{
    while (true)
    {
        await Task.Delay(30_000);
        fab.Status();
    }
});

// Content store cleanup
var sweepMs = ConfigHelpers.ParseIntRaw(
    GetConfigString(config, "content.sweepIntervalMs", "300000"),
    "content.sweepIntervalMs");
var appCts = new CancellationTokenSource();
cleanup.StartPeriodicSweep(sweepMs, appCts.Token);

// File-system-watch auto-reload has been removed — the UI / operator
// triggers reload explicitly via POST /api/reload on both tracks, so
// a background watcher is redundant. The reload endpoint in ApiHandler
// reads config.yaml on demand; the schema-registry re-apply happens
// there as well.

Console.WriteLine($"Listening on port {port}");

// Graceful shutdown
var lifetime = app.Lifetime;
lifetime.ApplicationStopping.Register(() =>
{
    Console.WriteLine("Shutdown signal received");
    fab.StopAsync();
    Thread.Sleep(2000);
    globalCtx.ShutdownAll();
    Console.WriteLine("Shutdown complete");
});

app.Run();

// --- Helpers ---

static int RunValidate(string? pathArg)
{
    var path = pathArg ?? Path.Combine(Directory.GetCurrentDirectory(), "config.yaml");
    if (!File.Exists(path))
    {
        Console.Error.WriteLine($"validate: config file not found: {path}");
        return 2;
    }

    Dictionary<string, object?> config;
    try
    {
        config = YamlParser.Parse(File.ReadAllText(path));
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"validate: YAML parse failed: {ex.Message}");
        return 1;
    }

    var reg = new Registry();
    BuiltinProcessors.RegisterAll(reg);
    var result = FlowValidator.Validate(config, reg);

    Console.WriteLine($"validate: {path}");
    if (result.Issues.Count == 0)
    {
        Console.WriteLine("  no issues — config is valid");
        return 0;
    }
    foreach (var issue in result.Issues)
        Console.WriteLine($"  {issue}");
    Console.WriteLine();
    Console.WriteLine($"summary: {result.ErrorCount} error(s), {result.WarningCount} warning(s)");
    return result.HasErrors ? 1 : 0;
}

static string GetConfigString(Dictionary<string, object?> config, string dotPath, string defaultVal)
{
    var parts = dotPath.Split('.');
    object? current = config;
    foreach (var part in parts)
    {
        if (CaravanFlow.Fabric.Fabric.TryGetDictValue(current, part, out current))
            continue;
        return defaultVal;
    }
    return current?.ToString() ?? defaultVal;
}

// --- Benchmarks (activated with --bench) ---

static void RunBenchmarks()
{
    Console.WriteLine("=== caravan-flow-csharp (.NET 10) benchmark ===");
    Console.WriteLine($"Runtime: {System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription}");
    Console.WriteLine($"GC: Server={GCSettings.IsServerGC}, LatencyMode={GCSettings.LatencyMode}");
    Console.WriteLine();

    Console.WriteLine("Warmup (pools + JIT)...");
    BenchPipelineThroughput(10_000, quiet: true);

    GC.Collect(2, GCCollectionMode.Aggressive, true, true);
    GC.WaitForPendingFinalizers();
    GC.Collect(2, GCCollectionMode.Aggressive, true, true);

    int g0Base = GC.CollectionCount(0), g1Base = GC.CollectionCount(1), g2Base = GC.CollectionCount(2);
    long allocBase = GC.GetTotalAllocatedBytes(true);
    Console.WriteLine();

    Console.WriteLine("Pipeline throughput (2-hop direct execution):");
    BenchPipelineThroughput(10_000);
    BenchPipelineThroughput(50_000);
    BenchPipelineThroughput(100_000);
    BenchPipelineThroughput(500_000);
    Console.WriteLine();

    long allocTotal = GC.GetTotalAllocatedBytes(true) - allocBase;
    int g0 = GC.CollectionCount(0) - g0Base, g1 = GC.CollectionCount(1) - g1Base, g2 = GC.CollectionCount(2) - g2Base;
    Console.WriteLine("GC during benchmarks (post-warmup):");
    Console.WriteLine($"  Gen0: {g0}, Gen1: {g1}, Gen2: {g2}");
    Console.WriteLine($"  Allocated: {allocTotal / 1024.0 / 1024.0:F2} MB");
    Console.WriteLine($"  Heap now:  {GC.GetTotalMemory(false) / 1024.0 / 1024.0:F2} MB");
}

static void BenchPipelineThroughput(int n, bool quiet = false)
{
    // Build a 2-hop pipeline: tag → sink (direct execution, no queues)
    var globalCtx = new ProcessorContext();
    var reg = new Registry();
    BuiltinProcessors.RegisterAll(reg);

    var fab = new CaravanFlow.Fabric.Fabric(reg, globalCtx);
    var config = new Dictionary<string, object?>
    {
        ["flow"] = new Dictionary<string, object?>
        {
            ["processors"] = new Dictionary<string, object?>
            {
                ["tag"] = new Dictionary<string, object?>
                {
                    ["type"] = "UpdateAttribute",
                    ["config"] = new Dictionary<string, object?> { ["env"] = "prod" },
                    ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "sink" } }
                },
                ["sink"] = new Dictionary<string, object?>
                {
                    ["type"] = "UpdateAttribute",
                    ["config"] = new Dictionary<string, object?> { ["done"] = "true" }
                }
            }
        }
    };
    fab.LoadFlow(config);

    var payload = "bench payload data here"u8.ToArray();
    int g0Before = GC.CollectionCount(0);

    var sw = Stopwatch.StartNew();

    for (int i = 0; i < n; i++)
    {
        var ff = FlowFile.Create(payload, new Dictionary<string, string>
        {
            ["type"] = "order",
            ["id"] = i.ToString()
        });
        fab.Execute(ff, "tag");
    }

    sw.Stop();
    long ms = sw.ElapsedMilliseconds;
    int g0During = GC.CollectionCount(0) - g0Before;

    if (!quiet)
    {
        if (ms > 0)
        {
            long rate = n * 1000L / ms;
            Console.WriteLine($"  {n:N0} ff, 2 hops: {ms}ms ({rate:N0} ff/s) [gc0: {g0During}]");
        }
        else
        {
            Console.WriteLine($"  {n:N0} ff, 2 hops: <1ms [gc0: {g0During}]");
        }
    }
}
