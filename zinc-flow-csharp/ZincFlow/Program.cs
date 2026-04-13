using System.Diagnostics;
using System.Runtime;
using System.Text;
using ZincFlow.Core;
using ZincFlow.Fabric;
using ZincFlow.StdLib;

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
    Console.WriteLine("zinc-flow — data flow engine");
    Console.WriteLine();
    Console.WriteLine("Usage:");
    Console.WriteLine("  zinc-flow                  Start the server using ./config.yaml (default)");
    Console.WriteLine("  zinc-flow validate [path]  Check a config without starting; exit 0 on success, 1 on errors");
    Console.WriteLine("  zinc-flow bench            Run pipeline throughput benchmarks");
    Console.WriteLine("  zinc-flow help             Show this message");
    return;
}

// --- Production server mode ---
Console.WriteLine("zinc-flow-csharp starting");

// Load config.yaml
var configPath = Path.Combine(Directory.GetCurrentDirectory(), "config.yaml");
if (!File.Exists(configPath))
    configPath = Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "config.yaml");

Dictionary<string, object?> config;
if (File.Exists(configPath))
{
    var yaml = File.ReadAllText(configPath);
    config = YamlParser.Parse(yaml);
    Console.WriteLine($"Config loaded from {configPath}");
}
else
{
    config = new();
    Console.WriteLine("No config.yaml found, using defaults");
}

// Create providers
var contentDir = GetConfigString(config, "content.dir", "/tmp/zinc-flow-csharp/content");
if (int.TryParse(GetConfigString(config, "content.offload_threshold_kb", "256"), out var threshKb))
    ContentHelpers.ClaimThreshold = threshKb * 1024;
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
// auto_register_subject config.
var embeddedRegistry = new ZincFlow.StdLib.EmbeddedSchemaRegistry();
var schemaConfigDir = File.Exists(configPath) ? Path.GetDirectoryName(Path.GetFullPath(configPath)) : null;
var schemasSection = ZincFlow.Fabric.Fabric.AsStringDict(config.GetValueOrDefault("schemas"));
var preloaded = embeddedRegistry.LoadFromConfig(schemasSection, schemaConfigDir);
var srProvider = new SchemaRegistryProvider(embeddedRegistry);
srProvider.Enable();
globalCtx.AddProvider(srProvider);
Console.WriteLine($"[schema-registry] embedded ({preloaded} subjects preloaded)");
if (!string.IsNullOrEmpty(GetConfigString(config, "schema_registry.url", "")))
    Console.Error.WriteLine("[schema-registry] WARNING: schema_registry.url is set but ignored — this build only supports the embedded backend");

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
var fab = new ZincFlow.Fabric.Fabric(reg, globalCtx);
fab.LoadFlow(config);
fab.StartAsync();
fab.Status();

// Create output directory
Directory.CreateDirectory("/tmp/zinc-flow-csharp/output");

// Config-driven sources
var fileInputDir = GetConfigString(config, "sources.file.input_dir", "");
if (!string.IsNullOrEmpty(fileInputDir))
{
    var pattern = GetConfigString(config, "sources.file.pattern", "*");
    var pollMs = int.TryParse(GetConfigString(config, "sources.file.poll_interval_ms", "1000"), out var p) ? p : 1000;
    fab.AddSource(new GetFile("file-ingest", fileInputDir, pattern, pollMs, store));
}

// ListenHTTP source — default ingest on port 9092, configurable
var ingestPort = GetConfigString(config, "sources.listen_http.port", "9092");
var ingestPath = GetConfigString(config, "sources.listen_http.path", "/");
fab.AddSource(new ListenHTTP("http-ingest", int.Parse(ingestPort), ingestPath, store));

// GenerateFlowFile source — optional, for testing/heartbeats
var genContent = GetConfigString(config, "sources.generate.content", "");
if (!string.IsNullOrEmpty(genContent))
{
    var genType = GetConfigString(config, "sources.generate.content_type", "");
    var genAttrs = GetConfigString(config, "sources.generate.attributes", "");
    var genBatch = int.TryParse(GetConfigString(config, "sources.generate.batch_size", "1"), out var gb) ? gb : 1;
    var genPoll = int.TryParse(GetConfigString(config, "sources.generate.poll_interval_ms", "1000"), out var gp) ? gp : 1000;
    fab.AddSource(new GenerateFlowFile("generator", genPoll, genContent, genType, genAttrs, genBatch));
}

// Build ASP.NET Minimal API app
var builder = WebApplication.CreateBuilder(args);
builder.Logging.ClearProviders();
builder.Logging.AddConsole();

// Use source-generated ZincJsonContext for AOT-safe JSON (no reflection fallback).
builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.TypeInfoResolverChain.Insert(0, ZincFlow.Core.ZincJsonContext.Default);
});

var port = GetConfigString(config, "server.port", "9091");
builder.WebHost.UseUrls($"http://0.0.0.0:{port}");

var app = builder.Build();

// Dashboard — serve at /
string? dashboardPath = null;
foreach (var candidate in new[]
{
    Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "dashboard.html"),      // JIT: bin/Debug/net10.0/../../..
    Path.Combine(AppContext.BaseDirectory, "..", "ZincFlow", "dashboard.html"),      // AOT: build/../ZincFlow/
    Path.Combine(Directory.GetCurrentDirectory(), "ZincFlow", "dashboard.html"),     // CWD/ZincFlow/
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

// Management API — ingestion goes through source connectors
var api = new ApiHandler(fab);
api.SetConfigPath(configPath);
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
var sweepMs = int.TryParse(GetConfigString(config, "content.sweep_interval_ms", "300000"), out var si) ? si : 300_000;
var appCts = new CancellationTokenSource();
cleanup.StartPeriodicSweep(sweepMs, appCts.Token);

// Hot reload: watch config.yaml for changes
if (File.Exists(configPath))
{
    var configDir = Path.GetDirectoryName(Path.GetFullPath(configPath))!;
    var configFile = Path.GetFileName(configPath);
    var watcher = new FileSystemWatcher(configDir, configFile);
    var reloadTimer = new System.Threading.Timer(_ =>
    {
        try
        {
            Console.WriteLine("[hot-reload] config.yaml changed, reloading...");
            var yaml = File.ReadAllText(configPath);
            var newConfig = YamlParser.Parse(yaml);
            var errors = ConfigValidator.Validate(newConfig, reg);
            if (errors.Count > 0)
            {
                Console.Error.WriteLine("[hot-reload] validation warnings:");
                foreach (var err in errors)
                    Console.Error.WriteLine($"  {err}");
            }
            fab.ReloadFlow(newConfig);

            // Re-apply schemas: section. Additive upsert — identical schemas
            // are no-ops, changed schemas become new versions, missing
            // subjects are left in place (DELETE via REST or restart to remove).
            var newSchemasSection = ZincFlow.Fabric.Fabric.AsStringDict(newConfig.GetValueOrDefault("schemas"));
            var added = embeddedRegistry.LoadFromConfig(newSchemasSection, schemaConfigDir);
            if (added > 0)
                Console.WriteLine($"[hot-reload] schemas re-applied ({added} subjects)");
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[hot-reload] reload failed: {ex.Message}");
        }
    });
    watcher.Changed += (_, _) => reloadTimer.Change(500, Timeout.Infinite);
    watcher.NotifyFilter = NotifyFilters.LastWrite;
    watcher.EnableRaisingEvents = true;
    Console.WriteLine($"[hot-reload] watching {configPath}");
}

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
        if (ZincFlow.Fabric.Fabric.TryGetDictValue(current, part, out current))
            continue;
        return defaultVal;
    }
    return current?.ToString() ?? defaultVal;
}

// --- Benchmarks (activated with --bench) ---

static void RunBenchmarks()
{
    Console.WriteLine("=== zinc-flow-csharp (.NET 10) benchmark ===");
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

    var fab = new ZincFlow.Fabric.Fabric(reg, globalCtx);
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
