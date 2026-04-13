using System.Diagnostics;
using ZincFlow.Core;
using ZincFlow.Fabric;
using ZincFlow.StdLib;
using static ZincFlow.Tests.TestRunner;
using static ZincFlow.Tests.Helpers;

namespace ZincFlow.Tests;

/// <summary>
/// Drives many FlowFiles through a multi-hop pipeline to verify:
///   - no FlowFile is silently dropped (in == out + failed)
///   - the backpressure semaphore behaves under sustained pressure
///   - throughput stays in the right ballpark (sanity, not perf-test)
///
/// Not a benchmark — those live in Program.cs --bench. These tests assert
/// correctness invariants under load that the existing happy-path tests
/// don't exercise.
/// </summary>
public static class SustainedLoadTests
{
    public static void RunAll()
    {
        TestSustainedThousandFlowFiles();
        TestBackpressureRejectsWhenSaturated();
        TestConcurrentSourcesNoLoss();
    }

    static void TestSustainedThousandFlowFiles()
    {
        Console.WriteLine("--- SustainedLoad: 1000 FlowFiles end-to-end, no loss ---");
        // 3-hop pipeline: tag → log → sink. Sink is a CaptureSink so we can
        // count exactly what got through.
        var ctx = TestContext();
        var reg = new Registry();
        BuiltinProcessors.RegisterAll(reg);
        var capture = new CaptureSink("seq");
        reg.Register(
            new ProcessorInfo("LoadTestSink", "captures FlowFiles for the load test", []),
            (_, _) => capture);
        var fab = new ZincFlow.Fabric.Fabric(reg, ctx);

        fab.LoadFlow(MakeFlowConfig(new()
        {
            ["tag"] = MakeProc("UpdateAttribute",
                new() { ["key"] = "stage", ["value"] = "tagged" },
                connections: new() { ["success"] = new() { "log" } }),
            ["log"] = MakeProc("UpdateAttribute",
                new() { ["key"] = "logged", ["value"] = "true" },
                connections: new() { ["success"] = new() { "sink" } }),
            ["sink"] = MakeProc("LoadTestSink", new())
        }));

        const int N = 1000;
        int rejected = 0;
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < N; i++)
        {
            var ff = FlowFile.Create("payload"u8.ToArray(), new() { ["seq"] = i.ToString() });
            if (!fab.Execute(ff, "tag")) rejected++;
        }
        sw.Stop();

        AssertIntEqual("no rejections (synchronous loop fits within semaphore budget)", rejected, 0);
        AssertIntEqual("captured exactly N FlowFiles", capture.Captured.Count, N);

        // Every seq value should be present exactly once — no losses, no dupes.
        var seenSeqs = new HashSet<string>(capture.Captured
            .Select(c => c.Attrs.GetValueOrDefault("seq", "")));
        AssertIntEqual("unique seq count", seenSeqs.Count, N);

        // Stats should reflect the work.
        var stats = fab.GetProcessorStats();
        AssertIntEqual("tag processed N", (int)Stat(stats, "tag", "processed"), N);
        AssertIntEqual("log processed N", (int)Stat(stats, "log", "processed"), N);
        AssertIntEqual("sink processed N", (int)Stat(stats, "sink", "processed"), N);

        var ratePerSec = (long)(N / Math.Max(sw.Elapsed.TotalSeconds, 0.001));
        Console.WriteLine($"  load: {N} ff in {sw.ElapsedMilliseconds}ms ({ratePerSec:N0} ff/s, single-thread)");
    }

    static void TestBackpressureRejectsWhenSaturated()
    {
        Console.WriteLine("--- SustainedLoad: backpressure rejects when execution gate is saturated ---");
        // Configure max_concurrent_executions=1, then have a slow processor
        // that holds the gate. Concurrent submissions should be rejected.
        var ctx = TestContext();
        var reg = new Registry();
        BuiltinProcessors.RegisterAll(reg);

        var holdLatch = new ManualResetEventSlim(false);
        var enteredLatch = new CountdownEvent(1);
        reg.Register(
            new ProcessorInfo("HoldProcessor", "blocks until released, for backpressure tests", []),
            (_, _) => new HoldProcessor(holdLatch, enteredLatch));

        var fab = new ZincFlow.Fabric.Fabric(reg, ctx);
        var config = MakeFlowConfig(new()
        {
            ["hold"] = MakeProc("HoldProcessor", new())
        });
        // Top-level defaults section. Fabric.LoadFlow reads
        // defaults.max_concurrent_executions as int.
        config["defaults"] = new Dictionary<string, object?>
        {
            ["max_concurrent_executions"] = 1
        };
        fab.LoadFlow(config);

        // Submit one FlowFile on a background thread — it grabs the gate and blocks.
        var bg = Task.Run(() =>
            fab.Execute(FlowFile.Create("a"u8.ToArray(), new()), "hold"));
        AssertTrue("first FlowFile entered the gate", enteredLatch.Wait(TimeSpan.FromSeconds(2)));

        // Second submission must be rejected (semaphore at zero).
        var accepted = fab.Execute(FlowFile.Create("b"u8.ToArray(), new()), "hold");
        AssertFalse("second FlowFile rejected (backpressure)", accepted);

        // Release the first FlowFile and confirm a fresh submission succeeds.
        holdLatch.Set();
        bg.Wait(TimeSpan.FromSeconds(2));

        // Wait for the gate to be released after bg's execution returns.
        var ok = WaitFor(() =>
            fab.Execute(FlowFile.Create("c"u8.ToArray(), new()), "hold"),
            timeoutMs: 2000);
        AssertTrue("fresh submission succeeds after release", ok);
    }

    static void TestConcurrentSourcesNoLoss()
    {
        Console.WriteLine("--- SustainedLoad: concurrent sources, no loss across threads ---");
        var ctx = TestContext();
        var reg = new Registry();
        BuiltinProcessors.RegisterAll(reg);
        var capture = new CaptureSink("source", "seq");
        reg.Register(
            new ProcessorInfo("ConcurrentSink", "captures for concurrency test", []),
            (_, _) => capture);
        var fab = new ZincFlow.Fabric.Fabric(reg, ctx);
        fab.LoadFlow(MakeFlowConfig(new()
        {
            ["sink"] = MakeProc("ConcurrentSink", new())
        }));

        const int Threads = 4;
        const int PerThread = 250;
        const int Total = Threads * PerThread;

        int rejected = 0;
        var threads = new Thread[Threads];
        for (int t = 0; t < Threads; t++)
        {
            var threadId = t;
            threads[t] = new Thread(() =>
            {
                for (int i = 0; i < PerThread; i++)
                {
                    var ff = FlowFile.Create("p"u8.ToArray(),
                        new() { ["source"] = threadId.ToString(), ["seq"] = i.ToString() });
                    if (!fab.Execute(ff, "sink")) Interlocked.Increment(ref rejected);
                }
            });
        }
        var sw = Stopwatch.StartNew();
        foreach (var th in threads) th.Start();
        foreach (var th in threads) th.Join();
        sw.Stop();

        // Concurrent submissions may hit the default semaphore (100) ceiling for
        // brief windows under contention. Track exactly how many landed and how
        // many were rejected — sum must equal Total (no silent losses).
        var captured = capture.Captured.Count;
        AssertIntEqual("captured + rejected == total submitted", captured + rejected, Total);

        var ratePerSec = (long)(Total / Math.Max(sw.Elapsed.TotalSeconds, 0.001));
        Console.WriteLine($"  concurrent: {Threads} threads × {PerThread} = {Total} ff in {sw.ElapsedMilliseconds}ms ({ratePerSec:N0} ff/s, captured={captured}, rejected={rejected})");
    }

    private sealed class HoldProcessor : IProcessor
    {
        private readonly ManualResetEventSlim _release;
        private readonly CountdownEvent _entered;
        public HoldProcessor(ManualResetEventSlim release, CountdownEvent entered)
        {
            _release = release;
            _entered = entered;
        }
        public ProcessorResult Process(FlowFile ff)
        {
            _entered.Signal();
            _release.Wait(TimeSpan.FromSeconds(5));
            return SingleResult.Rent(ff);
        }
    }
}
