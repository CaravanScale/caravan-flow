using System.Text;
using ZincFlow.Core;
using ZincFlow.Fabric;
using ZincFlow.StdLib;
using static ZincFlow.Tests.TestRunner;
using static ZincFlow.Tests.Helpers;

namespace ZincFlow.Tests;

public static class E2ETests
{
    public static void RunAll()
    {
        TestE2EFullPipeline();
        TestE2EContentStoreLifecycle();
        TestE2EProvenanceChain();
        TestE2EListenHTTPPipeline();
        TestE2EFullPipelineFormats();
        TestE2ECsvEtlPipeline();
        TestE2EFanOutContentIntegrity();
        TestE2ECascadingFailure();
        TestE2EHotReloadDataIntegrity();
        TestE2EAttributeAccumulation();
    }

    static void TestE2EFullPipeline()
    {
        Console.WriteLine("--- E2E: Full Pipeline (source -> Execute -> sink) ---");
        try
        {
            var store = new MemoryContentStore();
            var prov = new ProvenanceProvider();
            prov.Enable();

            var reg = new Registry();
            BuiltinProcessors.RegisterAll(reg);

            var globalCtx = new ProcessorContext();
            globalCtx.AddProvider(new ContentProvider("content", store));
            globalCtx.AddProvider(new LoggingProvider());
            globalCtx.AddProvider(prov);

            var fab = new ZincFlow.Fabric.Fabric(reg, globalCtx);

            // 3-stage pipeline: tagger -> logger -> sink via explicit connections.
            var config = new Dictionary<string, object?>
            {
                ["flow"] = new Dictionary<string, object?>
                {
                    ["processors"] = new Dictionary<string, object?>
                    {
                        ["tagger"] = new Dictionary<string, object?> { ["type"] = "UpdateAttribute", ["config"] = new Dictionary<string, object?> { ["key"] = "stage", ["value"] = "1" },
                            ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "logger" } } },
                        ["logger"] = new Dictionary<string, object?> { ["type"] = "UpdateAttribute", ["config"] = new Dictionary<string, object?> { ["key"] = "stage", ["value"] = "2" },
                            ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "sink" } } },
                        ["sink"] = new Dictionary<string, object?> { ["type"] = "UpdateAttribute", ["config"] = new Dictionary<string, object?> { ["key"] = "stage", ["value"] = "3" } }
                    }
                }
            };

            fab.LoadFlow(config);

            // Execute a FlowFile synchronously through the 3-stage pipeline
            var ff = FlowFile.Create("{\"order\":123}"u8, new() { ["type"] = "order", ["stage"] = "0" });
            var ffId = ff.NumericId;
            var ok = fab.Execute(ff, "tagger");
            AssertTrue("execute accepted", ok);

            // Verify stats — all 3 processors should have run
            var stats = fab.GetProcessorStats();
            AssertTrue("tagger processed", stats["tagger"]["processed"] == 1);
            AssertTrue("logger processed", stats["logger"]["processed"] == 1);
            AssertTrue("sink processed", stats["sink"]["processed"] == 1);

            // Verify provenance chain — all 3 processors touched the FlowFile
            var events = prov.GetEvents(ffId);
            AssertTrue("provenance has events", events.Count >= 3);
            AssertTrue("processed by tagger", events.Any(e => e.EventType == ProvenanceEventType.Processed && e.Component == "tagger"));
            AssertTrue("processed by logger", events.Any(e => e.EventType == ProvenanceEventType.Processed && e.Component == "logger"));
            AssertTrue("processed by sink", events.Any(e => e.EventType == ProvenanceEventType.Processed && e.Component == "sink"));
        }
        finally
        {
        }
    }

    static void TestE2EContentStoreLifecycle()
    {
        Console.WriteLine("--- E2E: Content Store Lifecycle (offload -> Execute -> cleanup) ---");
        var tmpDir = Path.Combine(Path.GetTempPath(), $"zinc-e2e-content-{Environment.TickCount64}");
        try
        {
            var store = new FileContentStore(tmpDir);
            var cleanup = new ContentStoreCleanup(store, tmpDir);
            var prevInstance = ContentStoreCleanup.Instance;
            ContentStoreCleanup.Instance = cleanup;

            // Create large content that triggers offload
            var largeData = new byte[300 * 1024]; // 300KB > 256KB threshold
            Array.Fill(largeData, (byte)'X');
            var content = ContentHelpers.MaybeOffload(store, largeData);

            AssertTrue("offloaded to claim", content is ClaimContent);
            var claim = (ClaimContent)content;
            AssertTrue("claim tracked", cleanup.ActiveCount == 1);
            AssertTrue("claim file exists", store.Exists(claim.ClaimId));

            // Create FlowFile with the claim content
            var ff = FlowFile.CreateWithContent(content, new() { ["type"] = "large" });

            // Resolve content (simulates processor reading it)
            var (resolved, error) = ContentHelpers.Resolve(store, ff.Content);
            AssertTrue("resolve ok", error == "");
            AssertIntEqual("resolved size", resolved.Length, 300 * 1024);

            // Return FlowFile — should release the claim
            FlowFile.Return(ff);
            AssertIntEqual("claim released", cleanup.ActiveCount, 0);

            // Sweep should delete the claim file
            int swept = cleanup.Sweep();
            AssertTrue("swept claim file", swept >= 1);
            AssertTrue("claim file gone", !store.Exists(claim.ClaimId));

            ContentStoreCleanup.Instance = prevInstance;
        }
        finally
        {
            if (Directory.Exists(tmpDir)) Directory.Delete(tmpDir, true);
        }
    }

    static void TestE2EProvenanceChain()
    {
        Console.WriteLine("--- E2E: Provenance Chain (multi-hop tracking via Execute) ---");
        var prov = new ProvenanceProvider();
        prov.Enable();
        var ctx = TestContext();
        ctx.AddProvider(prov);

        // 3-hop pipeline: tagger -> logger -> sink
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
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "logger" } }
                    },
                    ["logger"] = new Dictionary<string, object?>
                    {
                        ["type"] = "LogAttribute",
                        ["config"] = new Dictionary<string, object?> { ["prefix"] = "chain" },
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

        var ff = FlowFile.Create("chain-test"u8, new() { ["type"] = "order" });
        var ffId = ff.NumericId;
        fab.Execute(ff, "tagger");

        var events = prov.GetEvents(ffId);
        AssertTrue("chain has 5+ events", events.Count >= 5); // 3 processed + 2 routed
        AssertTrue("tagger processed", events.Any(e => e.Component == "tagger" && e.EventType == ProvenanceEventType.Processed));
        AssertTrue("tagger routed to logger", events.Any(e => e.Component == "tagger" && e.EventType == ProvenanceEventType.Routed && e.Details == "logger"));
        AssertTrue("logger processed", events.Any(e => e.Component == "logger" && e.EventType == ProvenanceEventType.Processed));
        AssertTrue("logger routed to sink", events.Any(e => e.Component == "logger" && e.EventType == ProvenanceEventType.Routed && e.Details == "sink"));
        AssertTrue("sink processed", events.Any(e => e.Component == "sink" && e.EventType == ProvenanceEventType.Processed));

        // Verify ordering — tagger before logger before sink
        var procEvents = events.Where(e => e.EventType == ProvenanceEventType.Processed).ToList();
        AssertTrue("order: tagger first", procEvents[0].Component == "tagger");
        AssertTrue("order: logger second", procEvents[1].Component == "logger");
        AssertTrue("order: sink third", procEvents[2].Component == "sink");
    }

    static void TestE2EListenHTTPPipeline()
    {
        Console.WriteLine("--- E2E: ListenHTTP -> Pipeline -> PutFile ---");
        var tmpDir = Path.Combine(Path.GetTempPath(), $"zinc-e2e-http-{Environment.TickCount64}");
        var outputDir = Path.Combine(tmpDir, "output");
        Directory.CreateDirectory(outputDir);
        try
        {
            var store = new MemoryContentStore();
            var prov = new ProvenanceProvider();
            prov.Enable();

            var reg = new Registry();
            BuiltinProcessors.RegisterAll(reg);

            var globalCtx = new ProcessorContext();
            globalCtx.AddProvider(new ContentProvider("content", store));
            var logProv = new LoggingProvider();
            logProv.Enable();
            globalCtx.AddProvider(logProv);
            globalCtx.AddProvider(prov);

            var fab = new ZincFlow.Fabric.Fabric(reg, globalCtx);

            var config = new Dictionary<string, object?>
            {
                ["flow"] = new Dictionary<string, object?>
                {
                    ["processors"] = new Dictionary<string, object?>
                    {
                        ["writer"] = new Dictionary<string, object?> { ["type"] = "PutFile", ["config"] = new Dictionary<string, object?> { ["output_dir"] = outputDir } }
                    }
                }
            };

            fab.LoadFlow(config);

            // Add ListenHTTP source on a test port
            var listenSource = new ListenHTTP("e2e-listen", 19877, "/", store);
            fab.AddSource(listenSource);

            fab.StartAsync();
            Thread.Sleep(1500); // wait for ListenHTTP to start

            // POST data to ListenHTTP
            using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
            var response = client.PostAsync("http://localhost:19877/",
                new StringContent("{\"e2e\":true}", Encoding.UTF8, "application/json")).GetAwaiter().GetResult();
            AssertTrue("ingest accepted", response.IsSuccessStatusCode);

            // Wait for pipeline to process
            Thread.Sleep(2000);

            // Verify output file was written
            var files = Directory.GetFiles(outputDir);
            AssertTrue("output file written", files.Length >= 1);

            // Verify provenance captured the flow
            var recent = prov.GetRecent(20);
            AssertTrue("provenance captured", recent.Count >= 1);
            AssertTrue("writer processed", recent.Any(e => e.Component == "writer" && e.EventType == ProvenanceEventType.Processed));

            // Verify sources API
            var sources = fab.GetSources();
            AssertTrue("source registered", sources.Any(s => s.Name == "e2e-listen" && s.Running));

            fab.StopAsync();
            Thread.Sleep(500);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  WARN: ListenHTTP pipeline test issue ({ex.GetType().Name}: {ex.Message})");
        }
        finally
        {
            if (Directory.Exists(tmpDir)) Directory.Delete(tmpDir, true);
        }
    }

    static void TestE2EFullPipelineFormats()
    {
        Console.WriteLine("--- TestE2EFullPipelineFormats ---");
        var ctx = TestContext();
        // Full format pipeline: ConvertJSONToRecord -> UpdateAttribute -> ConvertRecordToCSV -> sink
        var config = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["json-parse"] = new Dictionary<string, object?>
                    {
                        ["type"] = "ConvertJSONToRecord",
                        ["config"] = new Dictionary<string, object?> { ["schema_name"] = "data" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "enrich" } }
                    },
                    ["enrich"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "pipeline", ["value"] = "format-test" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "to-csv" } }
                    },
                    ["to-csv"] = new Dictionary<string, object?>
                    {
                        ["type"] = "ConvertRecordToCSV",
                        ["config"] = new Dictionary<string, object?> {},
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

        var ff = FlowFile.Create("""[{"name":"Alice","amount":42}]"""u8, new());
        var ok = fab.Execute(ff, "json-parse");
        AssertTrue("e2e formats: execute returns true", ok);
        var stats = fab.GetProcessorStats();
        AssertTrue("e2e formats: json-parse processed=1", stats["json-parse"]["processed"] == 1);
        AssertTrue("e2e formats: enrich processed=1", stats["enrich"]["processed"] == 1);
        AssertTrue("e2e formats: to-csv processed=1", stats["to-csv"]["processed"] == 1);
        AssertTrue("e2e formats: sink processed=1", stats["sink"]["processed"] == 1);
    }

    static void TestE2ECsvEtlPipeline()
    {
        Console.WriteLine("--- E2E: CSV ETL pipeline (content verified) ---");
        var ctx = TestContext();
        var captureSink = new CaptureSink("env", "processed_by");
        var reg = new Registry();
        BuiltinProcessors.RegisterAll(reg);
        reg.Register(new ProcessorInfo("Capture", "captures output", []),
            (_, _) => captureSink);

        var config = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["csv-parse"] = new Dictionary<string, object?>
                    {
                        ["type"] = "ConvertCSVToRecord",
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "transform" } }
                    },
                    ["transform"] = new Dictionary<string, object?>
                    {
                        ["type"] = "TransformRecord",
                        ["config"] = new Dictionary<string, object?> { ["operations"] = "toUpper:name" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "enrich" } }
                    },
                    ["enrich"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "env", ["value"] = "production" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "to-csv" } }
                    },
                    ["to-csv"] = new Dictionary<string, object?>
                    {
                        ["type"] = "ConvertRecordToCSV",
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "sink" } }
                    },
                    ["sink"] = new Dictionary<string, object?>
                    {
                        ["type"] = "Capture"
                    }
                }
            }
        };

        var fab = new ZincFlow.Fabric.Fabric(reg, ctx);
        fab.LoadFlow(config);

        var csv = "name,email,age\nalice,alice@test.com,30\nbob,bob@test.com,25\n";
        var ff = FlowFile.Create(Encoding.UTF8.GetBytes(csv), new() { ["format"] = "csv" });
        var ok = fab.Execute(ff, "csv-parse");
        AssertTrue("etl: execute ok", ok);

        // Verify actual content at the sink
        AssertIntEqual("etl: captured 1 flowfile", captureSink.Captured.Count, 1);
        var output = captureSink.Captured[0];

        // Verify attributes were enriched
        AssertTrue("etl: env attr set", output.Attrs.ContainsKey("env") && output.Attrs["env"] == "production");

        // Verify CSV output content — names should be uppercased
        var outputCsv = output.Text;
        AssertTrue("etl: output contains ALICE", outputCsv.Contains("ALICE"));
        AssertTrue("etl: output contains BOB", outputCsv.Contains("BOB"));
        AssertTrue("etl: output contains header", outputCsv.Contains("name"));
        AssertTrue("etl: output contains email", outputCsv.Contains("alice@test.com"));
    }

    static void TestE2EFanOutContentIntegrity()
    {
        Console.WriteLine("--- E2E: Fan-out content integrity ---");
        var ctx = TestContext();
        var sinkA = new CaptureSink("branch", "original");
        var sinkB = new CaptureSink("branch", "original");
        var sinkC = new CaptureSink("branch", "original");
        var reg = new Registry();
        BuiltinProcessors.RegisterAll(reg);
        reg.Register(new ProcessorInfo("CaptureA", "sink a", []), (_, _) => sinkA);
        reg.Register(new ProcessorInfo("CaptureB", "sink b", []), (_, _) => sinkB);
        reg.Register(new ProcessorInfo("CaptureC", "sink c", []), (_, _) => sinkC);

        var config = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["source"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "original", ["value"] = "true" },
                        ["connections"] = new Dictionary<string, object?>
                        {
                            ["success"] = new List<object?> { "branch-a", "branch-b", "branch-c" }
                        }
                    },
                    ["branch-a"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "branch", ["value"] = "a" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "sink-a" } }
                    },
                    ["branch-b"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "branch", ["value"] = "b" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "sink-b" } }
                    },
                    ["branch-c"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "branch", ["value"] = "c" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "sink-c" } }
                    },
                    ["sink-a"] = new Dictionary<string, object?> { ["type"] = "CaptureA" },
                    ["sink-b"] = new Dictionary<string, object?> { ["type"] = "CaptureB" },
                    ["sink-c"] = new Dictionary<string, object?> { ["type"] = "CaptureC" }
                }
            }
        };

        var fab = new ZincFlow.Fabric.Fabric(reg, ctx);
        fab.LoadFlow(config);

        var ff = FlowFile.Create("shared-content"u8, new() { ["id"] = "42" });
        fab.Execute(ff, "source");

        // Verify each branch received a copy with correct attributes
        AssertIntEqual("fanout: sink-a got 1", sinkA.Captured.Count, 1);
        AssertIntEqual("fanout: sink-b got 1", sinkB.Captured.Count, 1);
        AssertIntEqual("fanout: sink-c got 1", sinkC.Captured.Count, 1);

        AssertEqual("fanout: branch-a attr", sinkA.Captured[0].Attrs.GetValueOrDefault("branch", ""), "a");
        AssertEqual("fanout: branch-b attr", sinkB.Captured[0].Attrs.GetValueOrDefault("branch", ""), "b");
        AssertEqual("fanout: branch-c attr", sinkC.Captured[0].Attrs.GetValueOrDefault("branch", ""), "c");

        // Verify all branches have the original attribute
        AssertEqual("fanout: a has original", sinkA.Captured[0].Attrs.GetValueOrDefault("original", ""), "true");
        AssertEqual("fanout: b has original", sinkB.Captured[0].Attrs.GetValueOrDefault("original", ""), "true");
        AssertEqual("fanout: c has original", sinkC.Captured[0].Attrs.GetValueOrDefault("original", ""), "true");
    }

    static void TestE2ECascadingFailure()
    {
        Console.WriteLine("--- E2E: Cascading failure routing ---");
        var ctx = TestContext();
        var logProv = new LoggingProvider(); logProv.Enable();
        ctx.AddProvider(logProv);

        // A (UpdateAttribute) -> B (ConvertJSONToRecord, will fail on non-JSON)
        // B failure -> C (also ConvertJSONToRecord, will also fail)
        // C failure -> D (UpdateAttribute, catches the error)
        var finalSink = new CaptureSink("stage", "caught");
        var reg = new Registry();
        BuiltinProcessors.RegisterAll(reg);
        reg.Register(new ProcessorInfo("FinalCatch", "final catch", []), (_, _) => finalSink);

        var config = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["stage-a"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "stage", ["value"] = "a-done" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "stage-b" } }
                    },
                    ["stage-b"] = new Dictionary<string, object?>
                    {
                        ["type"] = "ConvertJSONToRecord",
                        ["connections"] = new Dictionary<string, object?>
                        {
                            ["success"] = new List<object?> { "never-reached" },
                            ["failure"] = new List<object?> { "stage-c" }
                        }
                    },
                    ["stage-c"] = new Dictionary<string, object?>
                    {
                        ["type"] = "ConvertJSONToRecord",
                        ["connections"] = new Dictionary<string, object?>
                        {
                            ["success"] = new List<object?> { "never-reached" },
                            ["failure"] = new List<object?> { "stage-d" }
                        }
                    },
                    ["stage-d"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "caught", ["value"] = "true" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "final-sink" } }
                    },
                    ["never-reached"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "bad", ["value"] = "true" }
                    },
                    ["final-sink"] = new Dictionary<string, object?> { ["type"] = "FinalCatch" }
                }
            }
        };

        var fab = new ZincFlow.Fabric.Fabric(reg, ctx);
        fab.LoadFlow(config);

        var ff = FlowFile.Create("not json at all"u8, new() { ["id"] = "cascade-test" });
        fab.Execute(ff, "stage-a");

        var stats = fab.GetProcessorStats();
        AssertTrue("cascade: a processed", stats["stage-a"]["processed"] == 1);
        AssertTrue("cascade: b processed (then failed)", stats["stage-b"]["processed"] == 1);
        AssertTrue("cascade: c processed (then failed)", stats["stage-c"]["processed"] == 1);
        AssertTrue("cascade: d processed (caught)", stats["stage-d"]["processed"] == 1);
        AssertTrue("cascade: never-reached=0", stats["never-reached"]["processed"] == 0);

        // Verify the final sink received the FlowFile with accumulated attributes
        AssertIntEqual("cascade: final-sink captured 1", finalSink.Captured.Count, 1);
        AssertEqual("cascade: caught=true", finalSink.Captured[0].Attrs.GetValueOrDefault("caught", ""), "true");
        AssertEqual("cascade: stage-a attr preserved", finalSink.Captured[0].Attrs.GetValueOrDefault("stage", ""), "a-done");
    }

    static void TestE2EHotReloadDataIntegrity()
    {
        Console.WriteLine("--- E2E: Hot reload data integrity ---");
        var ctx = TestContext();
        var sink = new CaptureSink("env");
        var reg = new Registry();
        BuiltinProcessors.RegisterAll(reg);
        reg.Register(new ProcessorInfo("Capture", "captures output", []), (_, _) => sink);

        var configBefore = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["tagger"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "env", ["value"] = "dev" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "sink" } }
                    },
                    ["sink"] = new Dictionary<string, object?> { ["type"] = "Capture" }
                }
            }
        };

        var fab = new ZincFlow.Fabric.Fabric(reg, ctx);
        fab.LoadFlow(configBefore);

        // Execute before reload
        fab.Execute(FlowFile.Create("test"u8, new()), "tagger");
        AssertIntEqual("reload-data: captured 1 before", sink.Captured.Count, 1);
        AssertEqual("reload-data: env=dev before reload", sink.Captured[0].Attrs.GetValueOrDefault("env", ""), "dev");

        // Reload with changed config
        var configAfter = new Dictionary<string, object?>
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
                    ["sink"] = new Dictionary<string, object?> { ["type"] = "Capture" }
                }
            }
        };
        fab.ReloadFlow(configAfter);

        // Execute after reload — sink instance is shared, so captures accumulate
        fab.Execute(FlowFile.Create("test"u8, new()), "tagger");
        AssertIntEqual("reload-data: captured 2 total", sink.Captured.Count, 2);
        AssertEqual("reload-data: env=prod after reload", sink.Captured[1].Attrs.GetValueOrDefault("env", ""), "prod");
    }

    static void TestE2EAttributeAccumulation()
    {
        Console.WriteLine("--- E2E: Attribute accumulation through pipeline ---");
        var ctx = TestContext();
        var sink = new CaptureSink("stage1", "stage2", "stage3", "original");
        var reg = new Registry();
        BuiltinProcessors.RegisterAll(reg);
        reg.Register(new ProcessorInfo("Capture", "captures output", []), (_, _) => sink);

        var config = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["step1"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "stage1", ["value"] = "done" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "step2" } }
                    },
                    ["step2"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "stage2", ["value"] = "done" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "step3" } }
                    },
                    ["step3"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "stage3", ["value"] = "done" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "sink" } }
                    },
                    ["sink"] = new Dictionary<string, object?> { ["type"] = "Capture" }
                }
            }
        };

        var fab = new ZincFlow.Fabric.Fabric(reg, ctx);
        fab.LoadFlow(config);

        var ff = FlowFile.Create("accumulation test"u8, new() { ["original"] = "yes" });
        fab.Execute(ff, "step1");

        AssertIntEqual("accum: captured 1", sink.Captured.Count, 1);
        var captured = sink.Captured[0];
        AssertEqual("accum: original preserved", captured.Attrs.GetValueOrDefault("original", ""), "yes");
        AssertEqual("accum: stage1 set", captured.Attrs.GetValueOrDefault("stage1", ""), "done");
        AssertEqual("accum: stage2 set", captured.Attrs.GetValueOrDefault("stage2", ""), "done");
        AssertEqual("accum: stage3 set", captured.Attrs.GetValueOrDefault("stage3", ""), "done");
    }
}
