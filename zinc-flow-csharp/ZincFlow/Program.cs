using System.Diagnostics;
using System.Runtime;
using System.Runtime.CompilerServices;
using System.Text;
using ZincFlow.Core;
using ZincFlow.Fabric;

Console.WriteLine("=== zinc-flow-csharp (.NET 10) benchmark ===");
Console.WriteLine($"Runtime: {System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription}");
Console.WriteLine($"GC: Server={GCSettings.IsServerGC}, LatencyMode={GCSettings.LatencyMode}");
Console.WriteLine();

// --- Warmup: JIT compile all hot paths ---
Console.WriteLine("Warmup (JIT)...");
BenchQueueThroughput(10_000, quiet: true);
BenchSessionThroughput(5_000, quiet: true);

// Force Gen2 collection to get clean baseline
GC.Collect(2, GCCollectionMode.Aggressive, true, true);
GC.WaitForPendingFinalizers();
GC.Collect(2, GCCollectionMode.Aggressive, true, true);
Console.WriteLine();

// --- Steady-state benchmarks ---
Console.WriteLine("Queue throughput:");
BenchQueueThroughput(100_000);
Console.WriteLine();

Console.WriteLine("Session throughput (2-hop pipeline):");
BenchSessionThroughput(10_000);
BenchSessionThroughput(50_000);
BenchSessionThroughput(100_000);
Console.WriteLine();

// GC stats
Console.WriteLine("GC collections during benchmark:");
Console.WriteLine($"  Gen0: {GC.CollectionCount(0)}, Gen1: {GC.CollectionCount(1)}, Gen2: {GC.CollectionCount(2)}");
Console.WriteLine($"  Total memory: {GC.GetTotalMemory(false) / 1024.0 / 1024.0:F2} MB");

static void BenchQueueThroughput(int n, bool quiet = false)
{
    var q = new FlowQueue("bench", n + 100, 0, 30_000);
    var payload = Encoding.UTF8.GetBytes("x");

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

    // Pre-load
    var payload = Encoding.UTF8.GetBytes("bench payload data here");
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
