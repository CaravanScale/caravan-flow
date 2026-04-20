using System.Text;
using CaravanFlow.Core;
using CaravanFlow.Fabric;
using CaravanFlow.StdLib;
using static CaravanFlow.Tests.TestRunner;

namespace CaravanFlow.Tests;

public static class Helpers
{
    /// <summary>Extract a long stat value from processor stats (handles object boxing)</summary>
    public static long Stat(Dictionary<string, Dictionary<string, object>> stats, string proc, string key)
        => stats.TryGetValue(proc, out var d) && d.TryGetValue(key, out var v) ? Convert.ToInt64(v) : 0;

    public static ProcessorContext TestContext()
    {
        var store = new MemoryContentStore();
        var cp = new ContentProvider("content", store); cp.Enable();
        var ctx = new ProcessorContext(); ctx.AddProvider(cp);
        return ctx;
    }

    public static ScopedContext TestScopedCtx()
    {
        var store = new MemoryContentStore();
        var cp = new ContentProvider("content", store); cp.Enable();
        return new ScopedContext(new Dictionary<string, IProvider> { ["content"] = cp });
    }

    public static byte[] TestJsonArray() => """[{"name": "Alice", "amount": 42}]"""u8.ToArray();

    public static CaravanFlow.Fabric.Fabric BuildFabric(Dictionary<string, object?> config, ProcessorContext? ctx = null)
    {
        ctx ??= TestContext();
        var reg = new Registry();
        BuiltinProcessors.RegisterAll(reg);
        var fab = new CaravanFlow.Fabric.Fabric(reg, ctx);
        fab.LoadFlow(config);
        return fab;
    }

    public static CaravanFlow.Fabric.Fabric BuildFabricWithCustom(
        Dictionary<string, object?> config,
        Action<Registry> registerCustom,
        ProcessorContext? ctx = null)
    {
        ctx ??= TestContext();
        var reg = new Registry();
        BuiltinProcessors.RegisterAll(reg);
        registerCustom(reg);
        var fab = new CaravanFlow.Fabric.Fabric(reg, ctx);
        fab.LoadFlow(config);
        return fab;
    }

    public static (CaravanFlow.Fabric.Fabric, ProcessorContext, Registry) CreateFabricWithConfig(Dictionary<string, object?> config)
    {
        var reg = new Registry();
        BuiltinProcessors.RegisterAll(reg);
        var ctx = new ProcessorContext();
        ctx.AddProvider(new ContentProvider("content", new MemoryContentStore()));
        ctx.GetProvider("content")!.Enable();
        var log = new LoggingProvider();
        log.Enable();
        ctx.AddProvider(log);
        var prov = new ProvenanceProvider();
        prov.Enable();
        ctx.AddProvider(prov);
        var fab = new CaravanFlow.Fabric.Fabric(reg, ctx);
        fab.LoadFlow(config);
        return (fab, ctx, reg);
    }

    public static Dictionary<string, object?> MakeFlowConfig(Dictionary<string, object?> processors)
    {
        var flow = new Dictionary<string, object?> { ["processors"] = processors };
        return new Dictionary<string, object?> { ["flow"] = flow };
    }

    public static Dictionary<string, object?> MakeProc(string type, Dictionary<string, string> config,
        List<string>? requires = null, Dictionary<string, List<string>>? connections = null)
    {
        // Convert to Dictionary<string, object?> to match YAML deserialization shape
        var cfgObj = config.ToDictionary(kv => kv.Key, kv => (object?)kv.Value);
        var def = new Dictionary<string, object?> { ["type"] = type, ["config"] = cfgObj };
        if (requires is not null)
            def["requires"] = requires.Cast<object?>().ToList();
        if (connections is not null)
        {
            var connDict = new Dictionary<string, object?>();
            foreach (var (rel, dests) in connections)
                connDict[rel] = dests.Cast<object?>().ToList();
            def["connections"] = connDict;
        }
        return def;
    }

    // --- Test infrastructure helpers ---

    /// <summary>
    /// OS-assigned free TCP port. Avoids hardcoded ports that collide on
    /// parallel CI runs. Race-free in practice because the listener releases
    /// immediately and the test grabs the port within milliseconds.
    /// </summary>
    public static int FreePort()
    {
        var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    /// <summary>
    /// Polls <paramref name="predicate"/> every ~10ms until it returns true or
    /// <paramref name="timeoutMs"/> elapses. Returns true on success, false on
    /// timeout. Replaces fragile Thread.Sleep(N) with event-driven waits so
    /// tests don't break under CI load.
    /// </summary>
    public static bool WaitFor(Func<bool> predicate, int timeoutMs = 5000, int pollMs = 10)
    {
        var deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (predicate()) return true;
            Thread.Sleep(pollMs);
        }
        return predicate();
    }

    /// <summary>
    /// Convenience: wait for an HTTP endpoint to start accepting connections.
    /// Used to wait for the management API / SchemaRegistryHandler to bind.
    /// </summary>
    public static bool WaitForHttpReady(string url, int timeoutMs = 5000)
    {
        using var client = new HttpClient { Timeout = TimeSpan.FromMilliseconds(200) };
        return WaitFor(() =>
        {
            try { client.GetAsync(url).GetAwaiter().GetResult(); return true; }
            catch { return false; }
        }, timeoutMs);
    }

    // --- Custom processor types used by tests ---

    public class ThrowingProcessor : IProcessor
    {
        public ProcessorResult Process(FlowFile ff) => throw new ArgumentException("test exception");
    }

    public class CustomRouteProcessor : IProcessor
    {
        public ProcessorResult Process(FlowFile ff) => RoutedResult.Rent("custom_route", ff);
    }

    /// <summary>
    /// Test sink that captures FlowFiles for content verification. Thread-safe —
    /// concurrent-source tests rely on this not losing items under contention.
    /// Snapshots attributes (via TryGetValue) and content bytes before returning.
    /// </summary>
    public class CaptureSink : IProcessor
    {
        private readonly List<CapturedFlowFile> _captured = new();
        private readonly object _lock = new();
        public IReadOnlyList<CapturedFlowFile> Captured
        {
            get { lock (_lock) return _captured.ToArray(); }
        }
        private readonly string[] _attrKeys;

        /// <param name="attrKeys">Attribute keys to snapshot (since AttributeMap can't enumerate)</param>
        public CaptureSink(params string[] attrKeys) => _attrKeys = attrKeys;

        public ProcessorResult Process(FlowFile ff)
        {
            var attrs = new Dictionary<string, string>();
            foreach (var key in _attrKeys)
            {
                if (ff.Attributes.TryGetValue(key, out var val))
                    attrs[key] = val;
            }
            byte[]? data = null;
            if (ff.Content is Raw raw)
                data = raw.Data.ToArray();
            List<Record>? records = null;
            if (ff.Content is RecordContent rc)
                records = rc.Records;
            lock (_lock) _captured.Add(new CapturedFlowFile(attrs, data, records));
            return SingleResult.Rent(ff);
        }
    }

    public record CapturedFlowFile(
        Dictionary<string, string> Attrs,
        byte[]? Data,
        List<Record>? Records
    )
    {
        public string Text => Data is not null ? Encoding.UTF8.GetString(Data) : "";
    }

    public class TestPoller : PollingSource
    {
        public override string SourceType => "TestPoller";
        public int PollCount;
        public List<FlowFile> NextBatch = new();
        public List<FlowFile> IngestedFiles = new();
        public List<FlowFile> RejectedFiles = new();

        public TestPoller(string name, int intervalMs) : base(name, intervalMs) { }

        protected override List<FlowFile> Poll(CancellationToken ct)
        {
            Interlocked.Increment(ref PollCount);
            var batch = new List<FlowFile>(NextBatch);
            NextBatch.Clear();
            return batch;
        }

        protected override void OnIngested(FlowFile ff) => IngestedFiles.Add(ff);
        protected override void OnRejected(FlowFile ff) => RejectedFiles.Add(ff);
    }
}
