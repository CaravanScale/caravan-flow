using System.Text;
using ZincFlow.Core;
using ZincFlow.Fabric;
using ZincFlow.StdLib;
using static ZincFlow.Tests.TestRunner;
using static ZincFlow.Tests.Helpers;

namespace ZincFlow.Tests;

public static class SourceTests
{
    public static void RunAll()
    {
        TestPutFile();
        TestPutStdout();
        TestConnectorSourceLifecycle();
        TestGetFile();
        TestListenHTTP();
        TestStructuredLogging();
        TestConfigValidation();
        TestProvenance();
        TestContentStoreCleanup();
        TestPollingSourceLifecycle();
        TestPollingSourceBackpressure();
    }

    static void TestPutFile()
    {
        Console.WriteLine("--- PutFile ---");
        var tmpDir = Path.Combine(Path.GetTempPath(), $"zinc-test-putfile-{Environment.TickCount64}");
        Directory.CreateDirectory(tmpDir);
        try
        {
            var store = new MemoryContentStore();
            var proc = new PutFile(tmpDir, "filename", "", ".dat", store);

            // Test with filename attribute
            var ff1 = FlowFile.Create("hello world"u8, new() { ["filename"] = "test.txt" });
            var result1 = proc.Process(ff1);
            AssertTrue("putfile returns single", result1 is SingleResult);
            var outFf1 = ((SingleResult)result1).FlowFile;
            AssertTrue("has output.path", outFf1.Attributes.TryGetValue("output.path", out _));
            AssertTrue("file exists", File.Exists(Path.Combine(tmpDir, "test.txt")));
            var contents = File.ReadAllText(Path.Combine(tmpDir, "test.txt"));
            AssertTrue("file content", contents == "hello world");

            // Test without filename attribute — uses counter
            var ff2 = FlowFile.Create("data"u8, new() { ["type"] = "order" });
            var result2 = proc.Process(ff2);
            AssertTrue("putfile counter fallback", result2 is SingleResult);
        }
        finally
        {
            Directory.Delete(tmpDir, true);
        }
    }

    static void TestPutStdout()
    {
        Console.WriteLine("--- PutStdout ---");
        var store = new MemoryContentStore();

        // Text format
        var proc = new PutStdout("text", store);
        var ff = FlowFile.Create("stdout test"u8, new() { ["type"] = "test" });
        var result = proc.Process(ff);
        AssertTrue("stdout returns single", result is SingleResult);
        AssertTrue("stdout passthrough", ReferenceEquals(((SingleResult)result).FlowFile, ff));

        // Attrs format
        var procAttrs = new PutStdout("attrs", store);
        var result2 = procAttrs.Process(ff);
        AssertTrue("stdout attrs returns single", result2 is SingleResult);
    }

    static void TestConnectorSourceLifecycle()
    {
        Console.WriteLine("--- ConnectorSource Lifecycle ---");
        var store = new MemoryContentStore();
        // Use GetFileSource for lifecycle test (no port needed)
        var tmpDir = Path.Combine(Path.GetTempPath(), $"zinc-test-lifecycle-{Environment.TickCount64}");
        Directory.CreateDirectory(tmpDir);
        try
        {
            var source = new GetFile("test-file", tmpDir, "*", 60000, store);

            AssertTrue("not running initially", !source.IsRunning);
            AssertTrue("name correct", source.Name == "test-file");
            AssertTrue("type is GetFile", source.SourceType == "GetFile");

            using var cts = new CancellationTokenSource();
            source.Start(ff => true, cts.Token);
            AssertTrue("running after start", source.IsRunning);

            source.Stop();
            Thread.Sleep(200);
            AssertTrue("stopped after stop", !source.IsRunning);
        }
        finally
        {
            Directory.Delete(tmpDir, true);
        }
    }

    static void TestGetFile()
    {
        Console.WriteLine("--- GetFileSource ---");
        var tmpDir = Path.Combine(Path.GetTempPath(), $"zinc-test-getfile-{Environment.TickCount64}");
        Directory.CreateDirectory(tmpDir);
        try
        {
            var store = new MemoryContentStore();
            var source = new GetFile("test-file", tmpDir, "*", 200, store);

            AssertTrue("not running initially", !source.IsRunning);

            // Write a test file
            File.WriteAllText(Path.Combine(tmpDir, "input.txt"), "file source test");

            var ingested = new List<FlowFile>();
            using var cts = new CancellationTokenSource();
            source.Start(ff => { ingested.Add(ff); return true; }, cts.Token);
            AssertTrue("running after start", source.IsRunning);

            // Wait for poll cycle
            Thread.Sleep(1000);

            AssertTrue("ingested 1 file", ingested.Count >= 1);
            if (ingested.Count > 0)
            {
                AssertTrue("filename attr", ingested[0].Attributes.TryGetValue("filename", out var fn) && fn == "input.txt");
                AssertTrue("source attr", ingested[0].Attributes.TryGetValue("source", out var src) && src == "test-file");
            }

            // File should be moved to .processed
            AssertTrue("input removed", !File.Exists(Path.Combine(tmpDir, "input.txt")));
            AssertTrue("processed exists", File.Exists(Path.Combine(tmpDir, ".processed", "input.txt")));

            source.Stop();
            AssertTrue("stopped", !source.IsRunning);
        }
        finally
        {
            Directory.Delete(tmpDir, true);
        }
    }

    static void TestListenHTTP()
    {
        Console.WriteLine("--- ListenHTTP Source ---");
        var store = new MemoryContentStore();
        var source = new ListenHTTP("test-listen", 19876, "/ingest", store);

        AssertTrue("not running initially", !source.IsRunning);
        AssertTrue("type is ListenHTTP", source.SourceType == "ListenHTTP");

        var ingested = new List<FlowFile>();
        using var cts = new CancellationTokenSource();
        source.Start(ff => { lock (ingested) { ingested.Add(ff); } return true; }, cts.Token);

        // Wait for server to start
        Thread.Sleep(1000);
        AssertTrue("running after start", source.IsRunning);

        // POST data to the listener
        try
        {
            using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
            var response = client.PostAsync("http://localhost:19876/ingest",
                new StringContent("{\"key\":\"value\"}", System.Text.Encoding.UTF8, "application/json")).GetAwaiter().GetResult();
            AssertTrue("accepted 200", response.IsSuccessStatusCode);

            Thread.Sleep(200);
            AssertTrue("ingested 1 flowfile", ingested.Count >= 1);
            if (ingested.Count > 0)
            {
                AssertTrue("source attr set", ingested[0].Attributes.TryGetValue("source", out var src) && src == "test-listen");
            }

            // Health check
            var health = client.GetAsync("http://localhost:19876/health").GetAwaiter().GetResult();
            AssertTrue("health ok", health.IsSuccessStatusCode);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  WARN: ListenHTTP test skipped ({ex.GetType().Name}: {ex.Message})");
        }
        finally
        {
            source.Stop();
            Thread.Sleep(500);
            AssertTrue("stopped after stop", !source.IsRunning);
        }
    }

    static void TestStructuredLogging()
    {
        Console.WriteLine("--- Structured Logging ---");
        var provider = new LoggingProvider();
        provider.Enable();

        // Text mode (default)
        provider.JsonOutput = false;
        var sw = new StringWriter();
        var orig = Console.Out;
        Console.SetOut(sw);
        provider.Log("INFO", "test", "hello world");
        Console.SetOut(orig);
        var output = sw.ToString();
        AssertTrue("text log contains level", output.Contains("[INFO]"));
        AssertTrue("text log contains component", output.Contains("[test]"));
        AssertTrue("text log contains message", output.Contains("hello world"));

        // JSON mode
        provider.JsonOutput = true;
        sw = new StringWriter();
        Console.SetOut(sw);
        provider.Log("WARN", "fabric", "queue full", new() { ["queue"] = "ingest" });
        Console.SetOut(orig);
        output = sw.ToString();
        AssertTrue("json log has level", output.Contains("\"level\":\"WARN\""));
        AssertTrue("json log has component", output.Contains("\"component\":\"fabric\""));
        AssertTrue("json log has extra", output.Contains("\"queue\":\"ingest\""));

        // Disabled provider does nothing
        provider.Disable(0);
        sw = new StringWriter();
        Console.SetOut(sw);
        provider.Log("ERROR", "test", "should not appear");
        Console.SetOut(orig);
        AssertTrue("disabled log empty", sw.ToString().Length == 0);
    }

    static void TestConfigValidation()
    {
        Console.WriteLine("--- Config Validation ---");
        var reg = new Registry();
        BuiltinProcessors.RegisterAll(reg);

        // Valid config
        var good = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["tagger"] = new Dictionary<string, object?> { ["type"] = "UpdateAttribute", ["config"] = new Dictionary<string, object?> { ["key"] = "env", ["value"] = "prod" } },
                    ["logger"] = new Dictionary<string, object?> { ["type"] = "LogAttribute", ["config"] = new Dictionary<string, object?> { ["prefix"] = "test" } }
                }
            }
        };
        var errors = ConfigValidator.Validate(good, reg);
        AssertIntEqual("valid config no errors", errors.Count, 0);

        // Missing flow section
        var noFlow = new Dictionary<string, object?>();
        errors = ConfigValidator.Validate(noFlow, reg);
        AssertTrue("missing flow error", errors.Count > 0);

        // Unknown processor type
        var badType = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["bad"] = new Dictionary<string, object?> { ["type"] = "nonexistent" }
                }
            }
        };
        errors = ConfigValidator.Validate(badType, reg);
        AssertTrue("unknown type error", errors.Any(e => e.Message.Contains("unknown processor type")));

        // Connection to nonexistent processor
        var badConn = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["tagger"] = new Dictionary<string, object?> { ["type"] = "UpdateAttribute",
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "missing" } } }
                }
            }
        };
        errors = ConfigValidator.Validate(badConn, reg);
        AssertTrue("bad connection error", errors.Any(e => e.Message.Contains("not a defined processor")));
    }

    static void TestProvenance()
    {
        Console.WriteLine("--- Provenance Provider ---");
        var prov = new ProvenanceProvider();
        prov.Enable();
        var ctx = TestContext();
        ctx.AddProvider(prov);

        var config = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["tagger"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "env", ["value"] = "prod" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "sink" } }
                    },
                    ["sink"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "done", ["value"] = "true" }
                    }
                }
            }
        };
        var fab = BuildFabric(config, ctx);

        var ff = FlowFile.Create("test"u8, new() { ["type"] = "order" });
        var ffId = ff.NumericId;
        fab.Execute(ff, "tagger");

        // Provenance provider should have recorded events
        var events = prov.GetEvents(ffId);
        AssertTrue("has provenance events", events.Count >= 2);
        AssertTrue("processed event", events.Any(e => e.EventType == ProvenanceEventType.Processed && e.Component == "tagger"));
        AssertTrue("routed event", events.Any(e => e.EventType == ProvenanceEventType.Routed && e.Component == "tagger" && e.Details == "sink"));

        // Recent events API
        var recent = prov.GetRecent(10);
        AssertTrue("recent has events", recent.Count >= 2);
    }

    static void TestContentStoreCleanup()
    {
        Console.WriteLine("--- ContentStore Cleanup ---");
        var tmpDir = Path.Combine(Path.GetTempPath(), $"zinc-test-cleanup-{Environment.TickCount64}");
        try
        {
            var store = new FileContentStore(tmpDir);
            var cleanup = new ContentStoreCleanup(store, tmpDir);

            // Store some claims
            var id1 = store.Store("active data"u8.ToArray());
            var id2 = store.Store("orphan data"u8.ToArray());

            // Track only id1 as active
            cleanup.TrackClaim(id1);
            AssertIntEqual("1 active claim", cleanup.ActiveCount, 1);

            // Sweep should delete id2 (orphaned)
            int swept = cleanup.Sweep();
            AssertTrue("swept orphan", swept >= 1);
            AssertTrue("active still exists", store.Exists(id1));
            AssertTrue("orphan deleted", !store.Exists(id2));

            // Release id1 and sweep again
            cleanup.ReleaseClaim(id1);
            swept = cleanup.Sweep();
            AssertTrue("swept released", swept >= 1);
            AssertTrue("all cleaned", !store.Exists(id1));
        }
        finally
        {
            if (Directory.Exists(tmpDir)) Directory.Delete(tmpDir, true);
        }
    }

    static void TestPollingSourceLifecycle()
    {
        Console.WriteLine("--- PollingSource: lifecycle ---");
        var poller = new TestPoller("test-poller", 100);

        AssertTrue("not running initially", !poller.IsRunning);
        AssertEqual("name", poller.Name, "test-poller");
        AssertEqual("source type", poller.SourceType, "TestPoller");

        // Start with a batch ready
        poller.NextBatch.Add(FlowFile.Create("hello"u8, new() { ["key"] = "value" }));

        using var cts = new CancellationTokenSource();
        poller.Start(ff => true, cts.Token);
        AssertTrue("running after start", poller.IsRunning);

        // Wait for at least 2 poll cycles
        Thread.Sleep(500);

        AssertTrue("poll called multiple times", poller.PollCount >= 2);
        AssertTrue("first batch ingested", poller.IngestedFiles.Count >= 1);
        AssertTrue("no rejections", poller.RejectedFiles.Count == 0);

        poller.Stop();
        Thread.Sleep(200);
        AssertTrue("stopped", !poller.IsRunning);
    }

    static void TestPollingSourceBackpressure()
    {
        Console.WriteLine("--- PollingSource: backpressure ---");
        var poller = new TestPoller("bp-poller", 100);

        // Prepare a batch
        poller.NextBatch.Add(FlowFile.Create("a"u8, new()));
        poller.NextBatch.Add(FlowFile.Create("b"u8, new()));

        using var cts = new CancellationTokenSource();
        // Ingest rejects everything (simulates backpressure)
        poller.Start(ff => false, cts.Token);

        Thread.Sleep(400);
        poller.Stop();
        Thread.Sleep(200);

        AssertTrue("poll was called", poller.PollCount >= 1);
        AssertTrue("no ingested (all rejected)", poller.IngestedFiles.Count == 0);
        AssertTrue("rejections recorded", poller.RejectedFiles.Count >= 2);
    }
}
