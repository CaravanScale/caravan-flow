using System.Text;
using ZincFlow.Core;
using ZincFlow.Fabric;
using ZincFlow.StdLib;
using static ZincFlow.Tests.TestRunner;

namespace ZincFlow.Tests;

public static class Helpers
{
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

    public static ZincFlow.Fabric.Fabric BuildFabric(Dictionary<string, object?> config, ProcessorContext? ctx = null)
    {
        ctx ??= TestContext();
        var reg = new Registry();
        BuiltinProcessors.RegisterAll(reg);
        var fab = new ZincFlow.Fabric.Fabric(reg, ctx);
        fab.LoadFlow(config);
        return fab;
    }

    public static ZincFlow.Fabric.Fabric BuildFabricWithCustom(
        Dictionary<string, object?> config,
        Action<Registry> registerCustom,
        ProcessorContext? ctx = null)
    {
        ctx ??= TestContext();
        var reg = new Registry();
        BuiltinProcessors.RegisterAll(reg);
        registerCustom(reg);
        var fab = new ZincFlow.Fabric.Fabric(reg, ctx);
        fab.LoadFlow(config);
        return fab;
    }

    public static (ZincFlow.Fabric.Fabric, ProcessorContext, Registry) CreateFabricWithConfig(Dictionary<string, object?> config)
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
        var fab = new ZincFlow.Fabric.Fabric(reg, ctx);
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
    /// Test sink that captures FlowFiles for content verification.
    /// Snapshots attributes (via TryGetValue) and content bytes before returning.
    /// </summary>
    public class CaptureSink : IProcessor
    {
        public readonly List<CapturedFlowFile> Captured = new();
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
            List<GenericRecord>? records = null;
            if (ff.Content is RecordContent rc)
                records = rc.Records;
            Captured.Add(new CapturedFlowFile(attrs, data, records));
            return SingleResult.Rent(ff);
        }
    }

    public record CapturedFlowFile(
        Dictionary<string, string> Attrs,
        byte[]? Data,
        List<GenericRecord>? Records
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
