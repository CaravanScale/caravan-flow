using System.Diagnostics;
using System.Runtime;
using System.Text;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;
using ZincFlow.Core;
using ZincFlow.Fabric;

// --- Mode selection ---
var mode = args.Length > 0 ? args[0] : "serve";

if (mode == "--bench" || mode == "bench")
{
    RunBenchmarks();
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
    var deserializer = new DeserializerBuilder()
        .WithNamingConvention(UnderscoredNamingConvention.Instance)
        .Build();
    config = deserializer.Deserialize<Dictionary<string, object?>>(yaml) ?? new();
    Console.WriteLine($"Config loaded from {configPath}");
}
else
{
    config = new();
    Console.WriteLine("No config.yaml found, using defaults");
}

// Create providers
var contentDir = GetConfigString(config, "content.dir", "/tmp/zinc-flow-csharp/content");
var store = new FileContentStore(contentDir);
var contentProvider = new ContentProvider("content", store);
contentProvider.Enable();

var configProvider = new ConfigProvider(config);
configProvider.Enable();

var loggingProvider = new LoggingProvider();
loggingProvider.Enable();

// Build global context
var globalCtx = new ProcessorContext();
globalCtx.AddProvider(contentProvider);
globalCtx.AddProvider(configProvider);
globalCtx.AddProvider(loggingProvider);

// Create registry
var reg = new Registry();
BuiltinProcessors.RegisterAll(reg);
Console.WriteLine($"Registry loaded: {reg.List().Count} processor types");

// Create fabric and load flow
var fab = new ZincFlow.Fabric.Fabric(reg, globalCtx);
fab.LoadFlow(config);
fab.StartAsync();
fab.Status();

// Create output directory
Directory.CreateDirectory("/tmp/zinc-flow-csharp/output");

// HTTP source
var httpSource = new HttpSource(fab, store);

// Build ASP.NET Minimal API app
var builder = WebApplication.CreateBuilder(args);
builder.Logging.ClearProviders();
builder.Logging.AddConsole();

// Enable reflection-based JSON serialization (needed for Dict/anonymous types with AOT)
builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.TypeInfoResolverChain.Insert(0,
        new System.Text.Json.Serialization.Metadata.DefaultJsonTypeInfoResolver());
});

var port = GetConfigString(config, "server.port", "9091");
builder.WebHost.UseUrls($"http://0.0.0.0:{port}");

var app = builder.Build();

// Map routes — use RequestDelegate for HttpContext handlers
app.Map("/ingest", (RequestDelegate)httpSource.HandleIngest);
app.Map("/health", (RequestDelegate)httpSource.HandleHealth);

var api = new ApiHandler(fab);
api.MapRoutes(app);

// Periodic stats
_ = Task.Run(async () =>
{
    while (true)
    {
        await Task.Delay(30_000);
        fab.Status();
    }
});

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

    Console.WriteLine("Warmup (JIT)...");
    BenchQueueThroughput(10_000, quiet: true);
    BenchSessionThroughput(5_000, quiet: true);

    GC.Collect(2, GCCollectionMode.Aggressive, true, true);
    GC.WaitForPendingFinalizers();
    GC.Collect(2, GCCollectionMode.Aggressive, true, true);
    Console.WriteLine();

    Console.WriteLine("Queue throughput:");
    BenchQueueThroughput(100_000);
    Console.WriteLine();

    Console.WriteLine("Session throughput (2-hop pipeline):");
    BenchSessionThroughput(10_000);
    BenchSessionThroughput(50_000);
    BenchSessionThroughput(100_000);
    Console.WriteLine();

    Console.WriteLine("GC collections during benchmark:");
    Console.WriteLine($"  Gen0: {GC.CollectionCount(0)}, Gen1: {GC.CollectionCount(1)}, Gen2: {GC.CollectionCount(2)}");
    Console.WriteLine($"  Total memory: {GC.GetTotalMemory(false) / 1024.0 / 1024.0:F2} MB");
}

static void BenchQueueThroughput(int n, bool quiet = false)
{
    var q = new FlowQueue("bench", n + 100, 0, 30_000);
    var payload = "x"u8.ToArray();

    var sw = Stopwatch.StartNew();
    for (int i = 0; i < n; i++)
    {
        var ff = FlowFile.Create(payload, new Dictionary<string, string>());
        q.Offer(ff);
    }
    sw.Stop();
    long offerMs = sw.ElapsedMilliseconds;

    sw.Restart();
    for (int i = 0; i < n; i++)
    {
        var entry = q.Claim()!;
        q.Ack(entry.Id);
    }
    sw.Stop();
    long claimMs = sw.ElapsedMilliseconds;

    if (!quiet)
    {
        long offerRate = offerMs > 0 ? n * 1000L / offerMs : 0;
        long claimRate = claimMs > 0 ? n * 1000L / claimMs : 0;
        Console.WriteLine($"  {n:N0} offer: {offerMs}ms ({offerRate:N0} ops/s)");
        Console.WriteLine($"  {n:N0} claim+ack: {claimMs}ms ({claimRate:N0} ops/s)");
    }
}

static void BenchSessionThroughput(int n, bool quiet = false)
{
    var tag = new AddAttribute("env", "prod");
    var sink = new AddAttribute("done", "true");

    var tagQ = new FlowQueue("tag", n + 100, 0, 30_000);
    var sinkQ = new FlowQueue("sink", n + 100, 0, 30_000);
    var queues = new Dictionary<string, FlowQueue> { ["tag"] = tagQ, ["sink"] = sinkQ };

    var tagEngine = new RulesEngine();
    tagEngine.AddOrReplaceRuleset("flow", new List<RoutingRule>
    {
        new RoutingRule("to-sink", "env", Operator.Exists, "", "sink")
    });
    var sinkEngine = new RulesEngine();

    var dlq = new DLQ();
    var tagSession = new ProcessSession(tagQ, tag, "tag", tagEngine, queues, dlq, 5);
    var sinkSession = new ProcessSession(sinkQ, sink, "sink", sinkEngine, queues, dlq, 5);

    var payload = "bench payload data here"u8.ToArray();
    for (int i = 0; i < n; i++)
    {
        var ff = FlowFile.Create(payload, new Dictionary<string, string>
        {
            ["type"] = "order",
            ["id"] = i.ToString()
        });
        tagQ.Offer(ff);
    }

    var sw = Stopwatch.StartNew();

    for (int i = 0; i < n; i++)
        tagSession.Execute();
    for (int i = 0; i < n; i++)
        sinkSession.Execute();

    sw.Stop();
    long ms = sw.ElapsedMilliseconds;

    if (!quiet)
    {
        if (ms > 0)
        {
            long rate = n * 1000L / ms;
            Console.WriteLine($"  {n:N0} flowfiles, 2 hops: {ms}ms ({rate:N0} ff/s)");
        }
        else
        {
            Console.WriteLine($"  {n:N0} flowfiles, 2 hops: <1ms");
        }
    }
}
