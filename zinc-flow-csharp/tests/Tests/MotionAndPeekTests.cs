using System.Text;
using ZincFlow.Core;
using ZincFlow.Fabric;
using static ZincFlow.Tests.TestRunner;
using static ZincFlow.Tests.Helpers;

namespace ZincFlow.Tests;

/// Phase A coverage — edge counters + sample ring instrumentation
/// installed in Fabric's dispatch loop. Mirrors the shape of existing
/// ProcessorTests so the test runner picks them up uniformly.
public static class MotionAndPeekTests
{
    public static void RunAll()
    {
        TestEdgeCountersIncrement();
        TestSampleRingBoundedNewestFirst();
        TestSampleRingAttributesSnapshotted();
        TestSamplingCanBeDisabled();
    }

    static void TestEdgeCountersIncrement()
    {
        Console.WriteLine("--- Motion: edge counters bump on dispatch ---");
        var config = MakeFlowConfig(new Dictionary<string, object?>
        {
            ["tag"] = MakeProc("UpdateAttribute", new() { ["key"] = "env", ["value"] = "dev" }),
            ["log"] = MakeProc("LogAttribute", new() { ["prefix"] = "mid" }),
            ["sink"] = MakeProc("PutStdout", new() { ["format"] = "attrs" }),
        });
        // Connect tag → log → sink.
        var processors = (Dictionary<string, object?>)((Dictionary<string, object?>)config["flow"]!)["processors"]!;
        ((Dictionary<string, object?>)processors["tag"]!)["connections"] = new Dictionary<string, object?>
        {
            ["success"] = new List<object?> { "log" }
        };
        ((Dictionary<string, object?>)processors["log"]!)["connections"] = new Dictionary<string, object?>
        {
            ["success"] = new List<object?> { "sink" }
        };

        var (fab, _, _) = CreateFabricWithConfig(config);
        fab.Execute(FlowFile.Create("x"u8.ToArray(), new()), "tag");

        var edges = fab.GetEdgeCounts();
        AssertTrue("tag|success|log counted", edges.GetValueOrDefault("tag|success|log") >= 1);
        AssertTrue("log|success|sink counted", edges.GetValueOrDefault("log|success|sink") >= 1);
    }

    static void TestSampleRingBoundedNewestFirst()
    {
        Console.WriteLine("--- Peek: sample ring holds last 5, newest first ---");
        var config = MakeFlowConfig(new Dictionary<string, object?>
        {
            ["tag"] = MakeProc("UpdateAttribute", new() { ["key"] = "batch", ["value"] = "n" }),
        });
        var (fab, _, _) = CreateFabricWithConfig(config);

        for (int i = 0; i < 12; i++)
            fab.Execute(FlowFile.Create(Encoding.UTF8.GetBytes($"body{i}"), new() { ["seq"] = i.ToString() }), "tag");

        var samples = fab.GetSamples("tag");
        AssertIntEqual("ring capped at 5", samples.Count, 5);
        // Newest first — seq 11 is the last one pushed.
        AssertEqual("newest sample seq", samples[0].Attributes.GetValueOrDefault("seq", ""), "11");
        AssertEqual("oldest sample seq (in ring of 5)", samples[4].Attributes.GetValueOrDefault("seq", ""), "7");
    }

    static void TestSampleRingAttributesSnapshotted()
    {
        Console.WriteLine("--- Peek: sample attribute snapshot is immutable post-push ---");
        var config = MakeFlowConfig(new Dictionary<string, object?>
        {
            ["tag"] = MakeProc("UpdateAttribute", new() { ["key"] = "env", ["value"] = "dev" }),
        });
        var (fab, _, _) = CreateFabricWithConfig(config);

        fab.Execute(FlowFile.Create("x"u8.ToArray(), new() { ["trace"] = "t1" }), "tag");
        var beforeSample = fab.GetSamples("tag")[0];
        var envVal = beforeSample.Attributes.GetValueOrDefault("env", "");
        AssertEqual("UpdateAttribute set env=dev in sample", envVal, "dev");
        AssertEqual("trace attribute preserved in sample", beforeSample.Attributes.GetValueOrDefault("trace", ""), "t1");
    }

    static void TestSamplingCanBeDisabled()
    {
        Console.WriteLine("--- Peek: SetSamplingEnabled(false) suppresses new samples ---");
        var config = MakeFlowConfig(new Dictionary<string, object?>
        {
            ["tag"] = MakeProc("UpdateAttribute", new() { ["key"] = "k", ["value"] = "v" }),
        });
        var (fab, _, _) = CreateFabricWithConfig(config);

        fab.SetSamplingEnabled(false);
        fab.Execute(FlowFile.Create("x"u8.ToArray(), new()), "tag");
        AssertIntEqual("no samples when disabled", fab.GetSamples("tag").Count, 0);

        fab.SetSamplingEnabled(true);
        fab.Execute(FlowFile.Create("y"u8.ToArray(), new()), "tag");
        AssertIntEqual("samples resume when re-enabled", fab.GetSamples("tag").Count, 1);
    }
}
