using System.Text;
using CaravanFlow.Core;
using CaravanFlow.Fabric;
using CaravanFlow.StdLib;
using static CaravanFlow.Tests.TestRunner;
using static CaravanFlow.Tests.Helpers;

namespace CaravanFlow.Tests;

public static class FabricTests
{
    public static void RunAll()
    {
        // Fabric integration
        TestFabricMultiHop();
        TestFabricDlqFromFailure();

        // Provider/context
        TestScopedContextAccess();
        TestProviderLifecycle();

        // Scenarios
        TestScenarioProcessorChain();
        TestScenarioScopedProviderIsolation();
        TestScenarioIRSFanOut();

        TestMaxHopCycleDetection();

        // DAG validator + connection tests
        TestDagValidatorValid();
        TestDagValidatorCycle();
        TestDagValidatorInvalidTarget();
        TestDagValidatorEntryPoints();
        TestConnectionFanOut();
        TestSinkNoConnections();

        // Hot reload
        TestHotReloadAddProcessor();
        TestHotReloadRemoveProcessor();
        TestHotReloadUpdateProcessor();
        TestHotReloadConnections();
        TestHotReloadNoChange();
        TestHotReloadEndToEnd();

        // Execute-based tests
        TestExecuteSingleProcessor();
        TestExecuteLinearPipeline();
        TestExecuteFanOut();
        TestExecuteFailureRouting();
        TestExecuteFailureNoHandler();
        TestExecuteDropped();
        TestExecuteMultipleResult();
        TestExecuteMaxHops();
        TestExecuteBackpressure();

        // Failure scenarios and edge cases
        TestProcessorException();
        TestFanOutMixedSuccess();
        TestPartialPipelineFailure();
        TestRoutedResultNoConnection();
        TestDualSinkPipeline();
        TestMultipleResultEdgeCases();

        // RouteOnAttribute integration
        TestRouteOnAttributeInPipeline();

        // AddProcessor incremental (API-style)
        TestAddProcessorIncremental();

        // Reliability hardening (Tier 0 + Tier 1 plan)
        TestSourceConnectionsConcurrent();
        TestSplitRecordManyIterations();
    }

    static void TestFabricMultiHop()
    {
        Console.WriteLine("--- Fabric: Multi-Hop via Execute ---");
        var config = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["tag-env"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "env", ["value"] = "prod" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "logger" } }
                    },
                    ["logger"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "logged", ["value"] = "true" }
                    }
                }
            }
        };
        var fab = BuildFabric(config);
        var ff = FlowFile.Create("multi-hop"u8, new() { ["type"] = "test" });
        var ok = fab.Execute(ff, "tag-env");
        AssertTrue("execute returns true", ok);

        var stats = fab.GetProcessorStats();
        AssertTrue("tag-env processed", Stat(stats, "tag-env", "processed") == 1);
        AssertTrue("logger processed", Stat(stats, "logger", "processed") == 1);
    }

    static void TestFabricDlqFromFailure()
    {
        Console.WriteLine("--- Fabric: Failure routing via connections ---");
        var ctx = TestContext();
        var logProv = new LoggingProvider(); logProv.Enable();
        ctx.AddProvider(logProv);

        var config = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["parser"] = new Dictionary<string, object?>
                    {
                        ["type"] = "ConvertJSONToRecord",
                        ["config"] = new Dictionary<string, object?> { ["schemaName"] = "test" },
                        ["connections"] = new Dictionary<string, object?>
                        {
                            ["success"] = new List<object?> { "sink" },
                            ["failure"] = new List<object?> { "error-handler" }
                        }
                    },
                    ["sink"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "done", ["value"] = "true" }
                    },
                    ["error-handler"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "error", ["value"] = "true" }
                    }
                }
            }
        };
        var fab = BuildFabric(config, ctx);

        // Good input goes to sink
        var goodFf = FlowFile.Create("""[{"key":"val"}]"""u8, new());
        var ok1 = fab.Execute(goodFf, "parser");
        AssertTrue("good execute ok", ok1);
        var pstats = fab.GetProcessorStats();
        AssertTrue("parser processed good", Stat(pstats, "parser", "processed") == 1);
        AssertTrue("sink processed good", Stat(pstats, "sink", "processed") == 1);
        AssertTrue("error-handler not invoked for good", Stat(pstats, "error-handler", "processed") == 0);

        // Bad input goes to error-handler
        var badFf = FlowFile.Create("not-json"u8, new());
        var ok2 = fab.Execute(badFf, "parser");
        AssertTrue("bad execute ok", ok2);
        pstats = fab.GetProcessorStats();
        AssertTrue("parser processed bad", Stat(pstats, "parser", "processed") == 2);
        AssertTrue("error-handler invoked for bad", Stat(pstats, "error-handler", "processed") == 1);
        AssertTrue("sink still 1", Stat(pstats, "sink", "processed") == 1);
    }

    static void TestScopedContextAccess()
    {
        Console.WriteLine("--- ScopedContext: Access ---");
        var store = new MemoryContentStore();
        var cp = new ContentProvider("content", store); cp.Enable();
        var ctx = new ScopedContext(new Dictionary<string, IProvider> { ["content"] = cp });

        AssertIntEqual("provider count", ctx.ListProviders().Count, 1);
        var p = ctx.GetProvider("content");
        AssertEqual("provider name", p.Name, "content");
    }

    static void TestProviderLifecycle()
    {
        Console.WriteLine("--- Provider: Lifecycle ---");
        var cp = new ContentProvider("content", new MemoryContentStore());

        AssertFalse("initially disabled", ((IProvider)cp).IsEnabled);
        cp.Enable();
        AssertTrue("after enable", ((IProvider)cp).IsEnabled);
        cp.Disable(0);
        AssertFalse("after disable", ((IProvider)cp).IsEnabled);
        cp.Enable();
        AssertTrue("re-enabled", ((IProvider)cp).IsEnabled);
        cp.Shutdown();
        AssertFalse("after shutdown", ((IProvider)cp).IsEnabled);
    }

    static void TestScenarioProcessorChain()
    {
        Console.WriteLine("--- Scenario: Processor Chain via Execute ---");
        var config = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["tag-env"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "env", ["value"] = "dev" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "logger" } }
                    },
                    ["logger"] = new Dictionary<string, object?>
                    {
                        ["type"] = "LogAttribute",
                        ["config"] = new Dictionary<string, object?> { ["prefix"] = "chain-test" }
                    }
                }
            }
        };
        var fab = BuildFabric(config);

        var ff = FlowFile.Create("hello"u8, new() { ["type"] = "order" });
        var ok = fab.Execute(ff, "tag-env");
        AssertTrue("chain execute ok", ok);

        var stats = fab.GetProcessorStats();
        AssertTrue("tag-env processed 1", Stat(stats, "tag-env", "processed") == 1);
        AssertTrue("logger processed 1", Stat(stats, "logger", "processed") == 1);
    }

    static void TestScenarioScopedProviderIsolation()
    {
        Console.WriteLine("--- Scenario: Scoped Provider Isolation ---");
        var cp = new ContentProvider("content", new MemoryContentStore()); cp.Enable();
        var lp = new LoggingProvider(); lp.Enable();

        var ctx = new ScopedContext(new Dictionary<string, IProvider> { ["content"] = cp });
        var p = ctx.GetProvider("content");
        AssertEqual("content provider type", p.ProviderType, "content");
        AssertIntEqual("only 1 provider in scope", ctx.ListProviders().Count, 1);
    }

    static void TestScenarioIRSFanOut()
    {
        Console.WriteLine("--- Scenario: Connection Fan-Out via Execute ---");
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
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "dest-a", "dest-b", "dest-c" } }
                    },
                    ["dest-a"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "branch", ["value"] = "a" }
                    },
                    ["dest-b"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "branch", ["value"] = "b" }
                    },
                    ["dest-c"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "branch", ["value"] = "c" }
                    }
                }
            }
        };
        var fab = BuildFabric(config);

        var ff = FlowFile.Create("fanout"u8, new() { ["type"] = "order" });
        var ok = fab.Execute(ff, "tagger");
        AssertTrue("fanout execute ok", ok);

        var stats = fab.GetProcessorStats();
        AssertTrue("tagger processed 1", Stat(stats, "tagger", "processed") == 1);
        AssertTrue("dest-a processed 1", Stat(stats, "dest-a", "processed") == 1);
        AssertTrue("dest-b processed 1", Stat(stats, "dest-b", "processed") == 1);
        AssertTrue("dest-c processed 1", Stat(stats, "dest-c", "processed") == 1);
    }

    static void TestMaxHopCycleDetection()
    {
        Console.WriteLine("--- Max Hop Cycle Detection via Execute ---");
        var ctx = TestContext();
        var logProv = new LoggingProvider(); logProv.Enable();
        ctx.AddProvider(logProv);

        // Create a cycle: A -> B -> A with maxHops = 5
        var config = new Dictionary<string, object?>
        {
            ["defaults"] = new Dictionary<string, object?>
            {
                ["maxHops"] = 5
            },
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["proc-a"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "hop", ["value"] = "a" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "proc-b" } }
                    },
                    ["proc-b"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "hop", ["value"] = "b" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "proc-a" } }
                    }
                }
            }
        };
        var fab = BuildFabric(config, ctx);

        var ff = FlowFile.Create("cycle-test"u8, new() { ["type"] = "test" });
        var ok = fab.Execute(ff, "proc-a");
        AssertTrue("execute completes (no infinite loop)", ok);

        // Should have processed some hops then stopped at max
        var stats = fab.GetProcessorStats();
        var totalProcessed = Stat(stats, "proc-a", "processed") + Stat(stats, "proc-b", "processed");
        AssertTrue("processed limited by max hops", totalProcessed <= 5);
        AssertTrue("errors recorded for hop limit", Stat(stats, "proc-a", "errors") + Stat(stats, "proc-b", "errors") >= 1);
    }

    static void TestDagValidatorValid()
    {
        Console.WriteLine("--- DagValidator: Valid DAG ---");
        var connections = new Dictionary<string, Dictionary<string, List<string>>>
        {
            ["tagger"] = new() { ["success"] = ["logger"] },
            ["logger"] = new() { ["success"] = ["sink"] },
            ["sink"] = new()
        };
        var result = DagValidator.Validate(connections);
        AssertIntEqual("no errors", result.Errors.Count, 0);
        AssertIntEqual("no warnings", result.Warnings.Count, 0);
        AssertIntEqual("1 entry point", result.EntryPoints.Count, 1);
        AssertEqual("entry point is tagger", result.EntryPoints[0], "tagger");
    }

    static void TestDagValidatorCycle()
    {
        Console.WriteLine("--- DagValidator: Cycle Detection ---");
        var connections = new Dictionary<string, Dictionary<string, List<string>>>
        {
            ["a"] = new() { ["success"] = ["b"] },
            ["b"] = new() { ["success"] = ["a"] }
        };
        var result = DagValidator.Validate(connections);
        AssertTrue("cycle warning", result.Warnings.Any(w => w.Contains("cycle detected")));
    }

    static void TestDagValidatorInvalidTarget()
    {
        Console.WriteLine("--- DagValidator: Invalid Target ---");
        var connections = new Dictionary<string, Dictionary<string, List<string>>>
        {
            ["a"] = new() { ["success"] = ["nonexistent"] }
        };
        var result = DagValidator.Validate(connections);
        AssertTrue("invalid target error", result.Errors.Any(e => e.Contains("unknown target")));
    }

    static void TestDagValidatorEntryPoints()
    {
        Console.WriteLine("--- DagValidator: Entry Points ---");
        // Two entry points: src1 -> shared, src2 -> shared
        var connections = new Dictionary<string, Dictionary<string, List<string>>>
        {
            ["src1"] = new() { ["success"] = ["shared"] },
            ["src2"] = new() { ["success"] = ["shared"] },
            ["shared"] = new()
        };
        var result = DagValidator.Validate(connections);
        AssertIntEqual("2 entry points", result.EntryPoints.Count, 2);
        AssertTrue("src1 is entry", result.EntryPoints.Contains("src1"));
        AssertTrue("src2 is entry", result.EntryPoints.Contains("src2"));
    }

    static void TestConnectionFanOut()
    {
        Console.WriteLine("--- Connection: Fan-Out via Execute ---");
        var config = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["src"] = new Dictionary<string, object?>
                    {
                        ["type"] = "LogAttribute",
                        ["config"] = new Dictionary<string, object?> { ["prefix"] = "fanout" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "a", "b", "c" } }
                    },
                    ["a"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "branch", ["value"] = "a" }
                    },
                    ["b"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "branch", ["value"] = "b" }
                    },
                    ["c"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "branch", ["value"] = "c" }
                    }
                }
            }
        };
        var fab = BuildFabric(config);

        var ff = FlowFile.Create("fanout"u8, new());
        fab.Execute(ff, "src");

        var stats = fab.GetProcessorStats();
        AssertTrue("a has 1", Stat(stats, "a", "processed") == 1);
        AssertTrue("b has 1", Stat(stats, "b", "processed") == 1);
        AssertTrue("c has 1", Stat(stats, "c", "processed") == 1);
        AssertTrue("src processed", Stat(stats, "src", "processed") == 1);
    }

    static void TestSinkNoConnections()
    {
        Console.WriteLine("--- Sink: No Connections (terminal) via Execute ---");
        var config = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["sink"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "done", ["value"] = "true" }
                    }
                }
            }
        };
        var fab = BuildFabric(config);

        var ff = FlowFile.Create("terminal"u8, new());
        var ok = fab.Execute(ff, "sink");
        AssertTrue("sink execute ok", ok);

        var stats = fab.GetProcessorStats();
        AssertTrue("sink processed 1", Stat(stats, "sink", "processed") == 1);
        AssertTrue("sink no errors", Stat(stats, "sink", "errors") == 0);
    }

    static void TestHotReloadAddProcessor()
    {
        Console.WriteLine("--- Hot Reload: Add Processor ---");
        var config1 = MakeFlowConfig(new Dictionary<string, object?>
        {
            ["tag-env"] = MakeProc("UpdateAttribute", new() { ["key"] = "env", ["value"] = "dev" })
        });
        var (fab, ctx, reg) = CreateFabricWithConfig(config1);

        AssertIntEqual("initial proc count", fab.GetProcessorNames().Count, 1);

        // Reload with an added processor
        var config2 = MakeFlowConfig(new Dictionary<string, object?>
        {
            ["tag-env"] = MakeProc("UpdateAttribute", new() { ["key"] = "env", ["value"] = "dev" }),
            ["tag-src"] = MakeProc("UpdateAttribute", new() { ["key"] = "source", ["value"] = "api" })
        });
        var (added, removed, updated, connectionsChanged) = fab.ReloadFlow(config2);
        AssertIntEqual("added", added, 1);
        AssertIntEqual("removed", removed, 0);
        AssertIntEqual("updated", updated, 0);
        AssertIntEqual("proc count after add", fab.GetProcessorNames().Count, 2);
        AssertTrue("new proc exists", fab.GetProcessorNames().Contains("tag-src"));
    }

    static void TestHotReloadRemoveProcessor()
    {
        Console.WriteLine("--- Hot Reload: Remove Processor ---");
        var config1 = MakeFlowConfig(new Dictionary<string, object?>
        {
            ["tag-env"] = MakeProc("UpdateAttribute", new() { ["key"] = "env", ["value"] = "dev" }),
            ["tag-src"] = MakeProc("UpdateAttribute", new() { ["key"] = "source", ["value"] = "api" })
        });
        var (fab, ctx, reg) = CreateFabricWithConfig(config1);

        AssertIntEqual("initial proc count", fab.GetProcessorNames().Count, 2);

        // Reload without tag-src
        var config2 = MakeFlowConfig(new Dictionary<string, object?>
        {
            ["tag-env"] = MakeProc("UpdateAttribute", new() { ["key"] = "env", ["value"] = "dev" })
        });
        var (added, removed, updated, _) = fab.ReloadFlow(config2);
        AssertIntEqual("added", added, 0);
        AssertIntEqual("removed", removed, 1);
        AssertIntEqual("updated", updated, 0);
        AssertIntEqual("proc count after remove", fab.GetProcessorNames().Count, 1);
        AssertFalse("removed proc gone", fab.GetProcessorNames().Contains("tag-src"));
    }

    static void TestHotReloadUpdateProcessor()
    {
        Console.WriteLine("--- Hot Reload: Update Processor ---");
        var config1 = MakeFlowConfig(new Dictionary<string, object?>
        {
            ["tag-env"] = MakeProc("UpdateAttribute", new() { ["key"] = "env", ["value"] = "dev" })
        });
        var (fab, ctx, reg) = CreateFabricWithConfig(config1);

        // Change the value config
        var config2 = MakeFlowConfig(new Dictionary<string, object?>
        {
            ["tag-env"] = MakeProc("UpdateAttribute", new() { ["key"] = "env", ["value"] = "prod" })
        });
        var (added, removed, updated, _) = fab.ReloadFlow(config2);
        AssertIntEqual("added", added, 0);
        AssertIntEqual("removed", removed, 0);
        AssertIntEqual("updated", updated, 1);
        AssertIntEqual("proc count unchanged", fab.GetProcessorNames().Count, 1);

        // Verify the processor uses new config by processing a FlowFile
        var ff = FlowFile.Create("test"u8, new() { ["type"] = "order" });
        fab.Execute(ff, "tag-env");
        var stats = fab.GetProcessorStats();
        AssertTrue("tag-env processed", Stat(stats, "tag-env", "processed") == 1);
    }

    static void TestHotReloadConnections()
    {
        Console.WriteLine("--- Hot Reload: Connections ---");
        var config1 = MakeFlowConfig(new Dictionary<string, object?>
        {
            ["tag-env"] = MakeProc("UpdateAttribute", new() { ["key"] = "env", ["value"] = "dev" }),
            ["logger"] = MakeProc("LogAttribute", new() { ["prefix"] = "test" })
        });
        var (fab, ctx, reg) = CreateFabricWithConfig(config1);

        var connBefore = fab.GetConnections();
        AssertIntEqual("tag-env no connections initially", connBefore.GetValueOrDefault("tag-env")?.Count ?? 0, 0);

        // Reload with connections: tag-env -> logger
        var config2 = MakeFlowConfig(new Dictionary<string, object?>
        {
            ["tag-env"] = MakeProc("UpdateAttribute", new() { ["key"] = "env", ["value"] = "dev" },
                connections: new() { ["success"] = ["logger"] }),
            ["logger"] = MakeProc("LogAttribute", new() { ["prefix"] = "test" })
        });
        var (added, removed, updated, connectionsChanged) = fab.ReloadFlow(config2);
        AssertTrue("connections changed", connectionsChanged > 0);
        var connAfter = fab.GetConnections();
        AssertTrue("tag-env has success connection", connAfter.ContainsKey("tag-env") && connAfter["tag-env"].ContainsKey("success"));
        AssertIntEqual("tag-env success targets", connAfter["tag-env"]["success"].Count, 1);
    }

    static void TestHotReloadNoChange()
    {
        Console.WriteLine("--- Hot Reload: No Change ---");
        var config = MakeFlowConfig(new Dictionary<string, object?>
        {
            ["tag-env"] = MakeProc("UpdateAttribute", new() { ["key"] = "env", ["value"] = "dev" },
                connections: new() { ["success"] = ["logger"] }),
            ["logger"] = MakeProc("LogAttribute", new() { ["prefix"] = "test" })
        });
        var (fab, ctx, reg) = CreateFabricWithConfig(config);

        var (added, removed, updated, connectionsChanged) = fab.ReloadFlow(config);
        AssertIntEqual("no adds", added, 0);
        AssertIntEqual("no removes", removed, 0);
        AssertIntEqual("no updates", updated, 0);
        AssertIntEqual("no connection changes", connectionsChanged, 0);
    }

    static void TestHotReloadEndToEnd()
    {
        Console.WriteLine("--- Hot Reload: E2E Pipeline Swap ---");

        // Start with tagger (entry point, no connections = terminal)
        var config1 = MakeFlowConfig(new Dictionary<string, object?>
        {
            ["tagger"] = MakeProc("UpdateAttribute", new() { ["key"] = "env", ["value"] = "dev" })
        });
        var (fab, ctx, reg) = CreateFabricWithConfig(config1);

        // Execute a FlowFile through the initial config
        var ff1 = FlowFile.Create("hello"u8, new() { ["type"] = "order" });
        fab.Execute(ff1, "tagger");

        var stats1 = fab.GetStats();
        AssertTrue("processed before reload", (long)stats1["processed"] > 0);

        // Hot reload: change tagger value from dev -> prod, add a second processor with connection
        var config2 = MakeFlowConfig(new Dictionary<string, object?>
        {
            ["tagger"] = MakeProc("UpdateAttribute", new() { ["key"] = "env", ["value"] = "prod" },
                connections: new() { ["success"] = ["logger"] }),
            ["logger"] = MakeProc("LogAttribute", new() { ["prefix"] = "reload-test" })
        });

        var (added, removed, updated, connectionsChanged) = fab.ReloadFlow(config2);
        AssertIntEqual("e2e added", added, 1);
        AssertIntEqual("e2e updated", updated, 1);
        // tagger config changed -> counted as update (connections bundled with processor update)
        AssertTrue("e2e changes applied", added + updated + connectionsChanged >= 2);
        AssertIntEqual("e2e proc count", fab.GetProcessorNames().Count, 2);

        // Execute another FlowFile — should flow through updated pipeline
        var ff2 = FlowFile.Create("world"u8, new() { ["type"] = "order" });
        fab.Execute(ff2, "tagger");

        var stats2 = fab.GetStats();
        AssertTrue("processed after reload", (long)stats2["processed"] > (long)stats1["processed"]);
    }

    static void TestExecuteSingleProcessor()
    {
        Console.WriteLine("--- Execute: Single Processor (sink) ---");
        var config = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["tag"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "env", ["value"] = "prod" }
                    }
                }
            }
        };
        var fab = BuildFabric(config);
        var ff = FlowFile.Create("test"u8, new() { ["type"] = "order" });
        var ok = fab.Execute(ff, "tag");
        AssertTrue("execute returns true", ok);
        var stats = fab.GetProcessorStats();
        AssertTrue("tag processed", Stat(stats, "tag", "processed") == 1);
        AssertTrue("tag no errors", Stat(stats, "tag", "errors") == 0);
    }

    static void TestExecuteLinearPipeline()
    {
        Console.WriteLine("--- Execute: Linear Pipeline (A -> B -> C) ---");
        var config = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["tag"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "env", ["value"] = "prod" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "enrich" } }
                    },
                    ["enrich"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "version", ["value"] = "1.0" },
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
        var fab = BuildFabric(config);
        var ff = FlowFile.Create("test"u8, new() { ["type"] = "order" });
        var ok = fab.Execute(ff, "tag");
        AssertTrue("execute returns true", ok);
        var stats = fab.GetProcessorStats();
        AssertTrue("tag processed", Stat(stats, "tag", "processed") == 1);
        AssertTrue("enrich processed", Stat(stats, "enrich", "processed") == 1);
        AssertTrue("sink processed", Stat(stats, "sink", "processed") == 1);
    }

    static void TestExecuteFanOut()
    {
        Console.WriteLine("--- Execute: Fan-Out (A -> [B, C]) ---");
        var config = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["source"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "tagged", ["value"] = "true" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "branch-a", "branch-b" } }
                    },
                    ["branch-a"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "branch", ["value"] = "a" }
                    },
                    ["branch-b"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "branch", ["value"] = "b" }
                    }
                }
            }
        };
        var fab = BuildFabric(config);
        var ff = FlowFile.Create("fanout"u8, new() { ["type"] = "order" });
        var ok = fab.Execute(ff, "source");
        AssertTrue("execute returns true", ok);
        var stats = fab.GetProcessorStats();
        AssertTrue("source processed 1", Stat(stats, "source", "processed") == 1);
        AssertTrue("branch-a processed 1", Stat(stats, "branch-a", "processed") == 1);
        AssertTrue("branch-b processed 1", Stat(stats, "branch-b", "processed") == 1);
    }

    static void TestExecuteFailureRouting()
    {
        Console.WriteLine("--- Execute: Failure -> failure connection -> handler ---");
        var ctx = TestContext();
        var config = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["parser"] = new Dictionary<string, object?>
                    {
                        ["type"] = "ConvertJSONToRecord",
                        ["config"] = new Dictionary<string, object?> { ["schemaName"] = "data" },
                        ["connections"] = new Dictionary<string, object?>
                        {
                            ["success"] = new List<object?> { "sink" },
                            ["failure"] = new List<object?> { "error-sink" }
                        }
                    },
                    ["sink"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "ok", ["value"] = "true" }
                    },
                    ["error-sink"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "failed", ["value"] = "true" }
                    }
                }
            }
        };
        var fab = BuildFabric(config, ctx);

        // Send bad JSON — should route to error-sink via failure connection
        var ff = FlowFile.Create("invalid json"u8, new());
        var ok = fab.Execute(ff, "parser");
        AssertTrue("execute ok", ok);
        var stats = fab.GetProcessorStats();
        AssertTrue("parser processed", Stat(stats, "parser", "processed") == 1);
        AssertTrue("error-sink received failure", Stat(stats, "error-sink", "processed") == 1);
        AssertTrue("sink not invoked", Stat(stats, "sink", "processed") == 0);
    }

    static void TestExecuteFailureNoHandler()
    {
        Console.WriteLine("--- Execute: Failure with no handler -> log+drop ---");
        var ctx = TestContext();
        var logProv = new LoggingProvider(); logProv.Enable();
        ctx.AddProvider(logProv);

        var config = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["parser"] = new Dictionary<string, object?>
                    {
                        ["type"] = "ConvertJSONToRecord",
                        ["config"] = new Dictionary<string, object?> { ["schemaName"] = "data" },
                        ["connections"] = new Dictionary<string, object?>
                        {
                            ["success"] = new List<object?> { "sink" }
                        }
                    },
                    ["sink"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "ok", ["value"] = "true" }
                    }
                }
            }
        };
        var fab = BuildFabric(config, ctx);

        // Send bad JSON — no failure connection defined, so log+drop
        var ff = FlowFile.Create("bad"u8, new());
        var ok = fab.Execute(ff, "parser");
        AssertTrue("execute ok", ok);
        var stats = fab.GetProcessorStats();
        AssertTrue("parser processed", Stat(stats, "parser", "processed") == 1);
        AssertTrue("parser error counted", Stat(stats, "parser", "errors") == 1);
        AssertTrue("sink not invoked", Stat(stats, "sink", "processed") == 0);
    }

    static void TestExecuteDropped()
    {
        Console.WriteLine("--- Execute: DroppedResult stops traversal ---");
        // We need a processor that returns DroppedResult.
        // ConvertJSONToRecord with "[]" returns FailureResult (no records).
        // Instead we use a config pipeline where a disabled processor is skipped.
        var config = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["source"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "tagged", ["value"] = "true" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "disabled-proc" } }
                    },
                    ["disabled-proc"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "should-not-reach", ["value"] = "true" },
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
        var fab = BuildFabric(config);
        // Disable the middle processor — disabled processors skip and return FlowFile
        fab.DisableProcessor("disabled-proc");

        var ff = FlowFile.Create("test"u8, new());
        var ok = fab.Execute(ff, "source");
        AssertTrue("execute ok", ok);

        var stats = fab.GetProcessorStats();
        AssertTrue("source processed", Stat(stats, "source", "processed") == 1);
        // disabled-proc is skipped, FlowFile is returned to pool
        AssertTrue("disabled-proc not processed", Stat(stats, "disabled-proc", "processed") == 0);
        AssertTrue("sink not reached", Stat(stats, "sink", "processed") == 0);
    }

    static void TestExecuteMultipleResult()
    {
        Console.WriteLine("--- Execute: MultipleResult (SplitText) ---");
        var ctx = TestContext();
        var config = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["splitter"] = new Dictionary<string, object?>
                    {
                        ["type"] = "SplitText",
                        ["config"] = new Dictionary<string, object?> { ["delimiter"] = @"\n" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "sink" } }
                    },
                    ["sink"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "split", ["value"] = "true" }
                    }
                }
            }
        };
        var fab = BuildFabric(config, ctx);

        var ff = FlowFile.Create("line1\nline2\nline3"u8, new());
        var ok = fab.Execute(ff, "splitter");
        AssertTrue("execute ok", ok);

        var stats = fab.GetProcessorStats();
        AssertTrue("splitter processed 1", Stat(stats, "splitter", "processed") == 1);
        AssertTrue("sink processed 3 (one per split)", Stat(stats, "sink", "processed") == 3);
    }

    static void TestExecuteMaxHops()
    {
        Console.WriteLine("--- Execute: Max Hops enforcement ---");
        var ctx = TestContext();
        var logProv = new LoggingProvider(); logProv.Enable();
        ctx.AddProvider(logProv);

        // Cycle with low maxHops
        var config = new Dictionary<string, object?>
        {
            ["defaults"] = new Dictionary<string, object?>
            {
                ["maxHops"] = 3
            },
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["a"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "hop", ["value"] = "a" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "b" } }
                    },
                    ["b"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "hop", ["value"] = "b" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "a" } }
                    }
                }
            }
        };
        var fab = BuildFabric(config, ctx);

        var ff = FlowFile.Create("cycle"u8, new());
        var ok = fab.Execute(ff, "a");
        AssertTrue("execute completes (no infinite loop)", ok);

        var stats = fab.GetProcessorStats();
        var totalProcessed = Stat(stats, "a", "processed") + Stat(stats, "b", "processed");
        AssertTrue("processed limited by max hops", totalProcessed <= 3);
        var totalErrors = Stat(stats, "a", "errors") + Stat(stats, "b", "errors");
        AssertTrue("hop limit errors recorded", totalErrors >= 1);
    }

    static void TestExecuteBackpressure()
    {
        Console.WriteLine("--- Execute: Backpressure (semaphore gating) ---");
        var ctx = TestContext();
        var config = new Dictionary<string, object?>
        {
            ["defaults"] = new Dictionary<string, object?>
            {
                ["maxConcurrentExecutions"] = 1
            },
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["tag"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "env", ["value"] = "prod" }
                    }
                }
            }
        };
        var fab = BuildFabric(config, ctx);

        // With maxConcurrentExecutions=1, a single synchronous Execute should succeed
        var ff1 = FlowFile.Create("test1"u8, new());
        var ok1 = fab.Execute(ff1, "tag");
        AssertTrue("first execute ok", ok1);

        // Second synchronous execute should also work (first completed)
        var ff2 = FlowFile.Create("test2"u8, new());
        var ok2 = fab.Execute(ff2, "tag");
        AssertTrue("second execute ok (first finished)", ok2);

        var stats = fab.GetProcessorStats();
        AssertTrue("tag processed 2", Stat(stats, "tag", "processed") == 2);
    }

    static void TestProcessorException()
    {
        Console.WriteLine("--- TestProcessorException ---");
        // Case 1: Throwing processor WITH failure connection — routes to error handler
        var config1 = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["thrower"] = new Dictionary<string, object?>
                    {
                        ["type"] = "Throwing",
                        ["config"] = new Dictionary<string, object?> {},
                        ["connections"] = new Dictionary<string, object?>
                        {
                            ["success"] = new List<object?> { "sink" },
                            ["failure"] = new List<object?> { "error-handler" }
                        }
                    },
                    ["sink"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "ok", ["value"] = "true" }
                    },
                    ["error-handler"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "error", ["value"] = "true" }
                    }
                }
            }
        };
        var fab1 = BuildFabricWithCustom(config1, reg =>
        {
            reg.Register(new ProcessorInfo("Throwing", "throws exception", []),
                (ctx, cfg) => new ThrowingProcessor());
        });
        var ff1 = FlowFile.Create("test"u8, new());
        var ok1 = fab1.Execute(ff1, "thrower");
        AssertTrue("exception: execute returns true", ok1);
        var stats1 = fab1.GetProcessorStats();
        AssertTrue("exception: thrower errors=1", Stat(stats1, "thrower", "errors") == 1);
        AssertTrue("exception: error-handler received flowfile", Stat(stats1, "error-handler", "processed") == 1);
        AssertTrue("exception: sink not invoked", Stat(stats1, "sink", "processed") == 0);

        // Case 2: Throwing processor WITHOUT failure connection — dropped gracefully
        var config2 = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["thrower"] = new Dictionary<string, object?>
                    {
                        ["type"] = "Throwing",
                        ["config"] = new Dictionary<string, object?> {},
                        ["connections"] = new Dictionary<string, object?>
                        {
                            ["success"] = new List<object?> { "sink" }
                        }
                    },
                    ["sink"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "ok", ["value"] = "true" }
                    }
                }
            }
        };
        var fab2 = BuildFabricWithCustom(config2, reg =>
        {
            reg.Register(new ProcessorInfo("Throwing", "throws exception", []),
                (ctx, cfg) => new ThrowingProcessor());
        });
        var ff2 = FlowFile.Create("test"u8, new());
        var ok2 = fab2.Execute(ff2, "thrower");
        AssertTrue("exception no handler: execute returns true", ok2);
        var stats2 = fab2.GetProcessorStats();
        AssertTrue("exception no handler: thrower errors=1", Stat(stats2, "thrower", "errors") == 1);
        AssertTrue("exception no handler: sink not invoked", Stat(stats2, "sink", "processed") == 0);
    }

    static void TestFanOutMixedSuccess()
    {
        Console.WriteLine("--- TestFanOutMixedSuccess ---");
        var ctx = TestContext();
        // source fans out to good-branch (UpdateAttribute) and bad-branch (ConvertJSONToRecord with non-JSON)
        // bad-branch has failure connection to error-handler
        var config = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["source"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "tagged", ["value"] = "true" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "good-branch", "bad-branch" } }
                    },
                    ["good-branch"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "good", ["value"] = "yes" }
                    },
                    ["bad-branch"] = new Dictionary<string, object?>
                    {
                        ["type"] = "ConvertJSONToRecord",
                        ["config"] = new Dictionary<string, object?> { ["schemaName"] = "data" },
                        ["connections"] = new Dictionary<string, object?>
                        {
                            ["success"] = new List<object?> { "never-reached" },
                            ["failure"] = new List<object?> { "error-handler" }
                        }
                    },
                    ["never-reached"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "nope", ["value"] = "true" }
                    },
                    ["error-handler"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "error", ["value"] = "true" }
                    }
                }
            }
        };
        var fab = BuildFabric(config, ctx);

        // Send non-JSON data: good-branch processes fine, bad-branch fails and routes to error-handler
        var ff = FlowFile.Create("not valid json"u8, new());
        var ok = fab.Execute(ff, "source");
        AssertTrue("fan-out mixed: execute returns true", ok);
        var stats = fab.GetProcessorStats();
        AssertTrue("fan-out mixed: source processed=1", Stat(stats, "source", "processed") == 1);
        AssertTrue("fan-out mixed: good-branch processed=1", Stat(stats, "good-branch", "processed") == 1);
        AssertTrue("fan-out mixed: bad-branch processed=1", Stat(stats, "bad-branch", "processed") == 1);
        AssertTrue("fan-out mixed: error-handler processed=1", Stat(stats, "error-handler", "processed") == 1);
        AssertTrue("fan-out mixed: never-reached processed=0", Stat(stats, "never-reached", "processed") == 0);
    }

    static void TestPartialPipelineFailure()
    {
        Console.WriteLine("--- TestPartialPipelineFailure ---");
        var ctx = TestContext();
        // 5-processor chain: A -> B -> C -> D -> E
        // C = ConvertJSONToRecord with non-JSON data -> returns FailureResult
        // C has failure connection to error-handler F
        var config = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["A"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "step", ["value"] = "A" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "B" } }
                    },
                    ["B"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "step", ["value"] = "B" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "C" } }
                    },
                    ["C"] = new Dictionary<string, object?>
                    {
                        ["type"] = "ConvertJSONToRecord",
                        ["config"] = new Dictionary<string, object?> { ["schemaName"] = "data" },
                        ["connections"] = new Dictionary<string, object?>
                        {
                            ["success"] = new List<object?> { "D" },
                            ["failure"] = new List<object?> { "F" }
                        }
                    },
                    ["D"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "step", ["value"] = "D" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "E" } }
                    },
                    ["E"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "step", ["value"] = "E" }
                    },
                    ["F"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "error_handled", ["value"] = "true" }
                    }
                }
            }
        };
        var fab = BuildFabric(config, ctx);

        var ff = FlowFile.Create("not json at all"u8, new());
        var ok = fab.Execute(ff, "A");
        AssertTrue("partial failure: execute returns true", ok);
        var stats = fab.GetProcessorStats();
        AssertTrue("partial failure: A processed=1", Stat(stats, "A", "processed") == 1);
        AssertTrue("partial failure: B processed=1", Stat(stats, "B", "processed") == 1);
        AssertTrue("partial failure: C processed=1", Stat(stats, "C", "processed") == 1);
        AssertTrue("partial failure: D processed=0", Stat(stats, "D", "processed") == 0);
        AssertTrue("partial failure: E processed=0", Stat(stats, "E", "processed") == 0);
        AssertTrue("partial failure: F processed=1", Stat(stats, "F", "processed") == 1);
    }

    static void TestRoutedResultNoConnection()
    {
        Console.WriteLine("--- TestRoutedResultNoConnection ---");
        // Processor returns RoutedResult with "custom_route" but only "success" connection exists
        var config = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["router"] = new Dictionary<string, object?>
                    {
                        ["type"] = "CustomRoute",
                        ["config"] = new Dictionary<string, object?> {},
                        ["connections"] = new Dictionary<string, object?>
                        {
                            ["success"] = new List<object?> { "sink" }
                        }
                    },
                    ["sink"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "done", ["value"] = "true" }
                    }
                }
            }
        };
        var fab = BuildFabricWithCustom(config, reg =>
        {
            reg.Register(new ProcessorInfo("CustomRoute", "returns custom route", []),
                (ctx, cfg) => new CustomRouteProcessor());
        });
        var ff = FlowFile.Create("routed"u8, new());
        var ok = fab.Execute(ff, "router");
        AssertTrue("routed no conn: execute completes", ok);
        var stats = fab.GetProcessorStats();
        AssertTrue("routed no conn: router processed=1", Stat(stats, "router", "processed") == 1);
        AssertTrue("routed no conn: router errors=0", Stat(stats, "router", "errors") == 0);
        AssertTrue("routed no conn: sink not invoked", Stat(stats, "sink", "processed") == 0);
    }

    static void TestDualSinkPipeline()
    {
        Console.WriteLine("--- TestDualSinkPipeline ---");
        var ctx = TestContext();
        var config = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["parser"] = new Dictionary<string, object?>
                    {
                        ["type"] = "ConvertJSONToRecord",
                        ["config"] = new Dictionary<string, object?> { ["schemaName"] = "data" },
                        ["connections"] = new Dictionary<string, object?>
                        {
                            ["success"] = new List<object?> { "json-sink" },
                            ["failure"] = new List<object?> { "error-sink" }
                        }
                    },
                    ["json-sink"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "done", ["value"] = "true" }
                    },
                    ["error-sink"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "error", ["value"] = "true" }
                    }
                }
            }
        };
        var fab = BuildFabric(config, ctx);

        // Send valid JSON -> json-sink gets it
        var ff1 = FlowFile.Create("""[{"name":"Alice","amount":42}]"""u8, new());
        var ok1 = fab.Execute(ff1, "parser");
        AssertTrue("dual sink: valid JSON executes", ok1);
        var stats1 = fab.GetProcessorStats();
        AssertTrue("dual sink: parser processed=1 (valid)", Stat(stats1, "parser", "processed") == 1);
        AssertTrue("dual sink: json-sink processed=1", Stat(stats1, "json-sink", "processed") == 1);
        AssertTrue("dual sink: error-sink processed=0", Stat(stats1, "error-sink", "processed") == 0);

        // Send invalid data -> error-sink gets it
        var ff2 = FlowFile.Create("this is not json"u8, new());
        var ok2 = fab.Execute(ff2, "parser");
        AssertTrue("dual sink: invalid data executes", ok2);
        var stats2 = fab.GetProcessorStats();
        AssertTrue("dual sink: parser processed=2 (both)", Stat(stats2, "parser", "processed") == 2);
        AssertTrue("dual sink: json-sink still=1", Stat(stats2, "json-sink", "processed") == 1);
        AssertTrue("dual sink: error-sink processed=1", Stat(stats2, "error-sink", "processed") == 1);
    }

    static void TestMultipleResultEdgeCases()
    {
        Console.WriteLine("--- TestMultipleResultEdgeCases ---");
        var ctx = TestContext();

        // Case 1: Single line (no split delimiter found) -> SplitText returns SingleResult (pass-through)
        var config = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["splitter"] = new Dictionary<string, object?>
                    {
                        ["type"] = "SplitText",
                        ["config"] = new Dictionary<string, object?> { ["delimiter"] = @"\n" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "sink" } }
                    },
                    ["sink"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "split", ["value"] = "true" }
                    }
                }
            }
        };
        var fab1 = BuildFabric(config, ctx);

        // Single line — no newline, so parts.Length == 1 -> SingleResult pass-through -> downstream gets 1
        var ff1 = FlowFile.Create("single line no newline"u8, new());
        var ok1 = fab1.Execute(ff1, "splitter");
        AssertTrue("split single line: execute ok", ok1);
        var stats1 = fab1.GetProcessorStats();
        AssertTrue("split single line: splitter processed=1", Stat(stats1, "splitter", "processed") == 1);
        AssertTrue("split single line: sink processed=1 (pass-through)", Stat(stats1, "sink", "processed") == 1);

        // Case 2: Empty content -> SplitText splits "" by \n -> [""] -> length 1 -> SingleResult pass-through
        var ctx2 = TestContext();
        var fab2 = BuildFabric(config, ctx2);
        var ff2 = FlowFile.Create(""u8, new());
        var ok2 = fab2.Execute(ff2, "splitter");
        AssertTrue("split empty: execute ok", ok2);
        var stats2 = fab2.GetProcessorStats();
        AssertTrue("split empty: splitter processed=1", Stat(stats2, "splitter", "processed") == 1);
        AssertTrue("split empty: sink processed=1 (pass-through)", Stat(stats2, "sink", "processed") == 1);
    }

    static void TestRouteOnAttributeInPipeline()
    {
        Console.WriteLine("--- Fabric: RouteOnAttribute in Pipeline ---");
        var ctx = TestContext();
        var premiumSink = new CaptureSink("tier", "routed");
        var bulkSink = new CaptureSink("tier", "routed");
        var defaultSink = new CaptureSink("tier", "routed");

        var reg = new Registry();
        BuiltinProcessors.RegisterAll(reg);
        reg.Register(new ProcessorInfo("PremiumSink", "premium sink", []), (_, _) => premiumSink);
        reg.Register(new ProcessorInfo("BulkSink", "bulk sink", []), (_, _) => bulkSink);
        reg.Register(new ProcessorInfo("DefaultSink", "default sink", []), (_, _) => defaultSink);

        var config = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["tagger"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "routed", ["value"] = "true" },
                        ["connections"] = new Dictionary<string, object?> { ["success"] = new List<object?> { "router" } }
                    },
                    ["router"] = new Dictionary<string, object?>
                    {
                        ["type"] = "RouteOnAttribute",
                        ["config"] = new Dictionary<string, object?> { ["routes"] = "premium:tier EQ premium;bulk:tier EQ bulk" },
                        ["connections"] = new Dictionary<string, object?>
                        {
                            ["premium"] = new List<object?> { "premium-sink" },
                            ["bulk"] = new List<object?> { "bulk-sink" },
                            ["unmatched"] = new List<object?> { "default-sink" }
                        }
                    },
                    ["premium-sink"] = new Dictionary<string, object?> { ["type"] = "PremiumSink" },
                    ["bulk-sink"] = new Dictionary<string, object?> { ["type"] = "BulkSink" },
                    ["default-sink"] = new Dictionary<string, object?> { ["type"] = "DefaultSink" }
                }
            }
        };

        var fab = new CaravanFlow.Fabric.Fabric(reg, ctx);
        fab.LoadFlow(config);

        // Premium tier
        var ff1 = FlowFile.Create("order1"u8, new() { ["tier"] = "premium" });
        fab.Execute(ff1, "tagger");

        // Bulk tier
        var ff2 = FlowFile.Create("order2"u8, new() { ["tier"] = "bulk" });
        fab.Execute(ff2, "tagger");

        // Unknown tier → default
        var ff3 = FlowFile.Create("order3"u8, new() { ["tier"] = "free" });
        fab.Execute(ff3, "tagger");

        // Verify routing
        AssertIntEqual("premium sink got 1", premiumSink.Captured.Count, 1);
        AssertEqual("premium has tier", premiumSink.Captured[0].Attrs.GetValueOrDefault("tier", ""), "premium");
        AssertEqual("premium has routed", premiumSink.Captured[0].Attrs.GetValueOrDefault("routed", ""), "true");

        AssertIntEqual("bulk sink got 1", bulkSink.Captured.Count, 1);
        AssertEqual("bulk has tier", bulkSink.Captured[0].Attrs.GetValueOrDefault("tier", ""), "bulk");

        AssertIntEqual("default sink got 1", defaultSink.Captured.Count, 1);
        AssertEqual("default has tier", defaultSink.Captured[0].Attrs.GetValueOrDefault("tier", ""), "free");
    }

    /// <summary>
    /// Test AddProcessor incrementally (API-style): add processors one at a time,
    /// connections reference processors that don't exist yet. Verify pipeline works
    /// after all processors are added.
    /// </summary>
    static void TestAddProcessorIncremental()
    {
        Console.WriteLine("--- AddProcessor: incremental (API-style) ---");
        var ctx = TestContext();
        var logProv = new LoggingProvider(); logProv.Enable();
        ctx.AddProvider(logProv);

        var reg = new Registry();
        BuiltinProcessors.RegisterAll(reg);
        var fab = new CaravanFlow.Fabric.Fabric(reg, ctx);

        // Add processors one at a time (connections reference future processors)
        var ok1 = fab.AddProcessor("parser", "ConvertJSONToRecord", new(),
            null, new() { ["success"] = new List<string> { "sink" }, ["failure"] = new List<string> { "err" } });
        AssertTrue("add parser", ok1);

        var ok2 = fab.AddProcessor("sink", "LogAttribute", new() { ["prefix"] = "OK" });
        AssertTrue("add sink", ok2);

        var ok3 = fab.AddProcessor("err", "LogAttribute", new() { ["prefix"] = "ERR" });
        AssertTrue("add err", ok3);

        // Verify graph state
        AssertIntEqual("3 processors", fab.GetProcessorNames().Count, 3);

        // Execute with valid JSON — should flow: parser → sink
        var ff1 = FlowFile.Create(Encoding.UTF8.GetBytes("[{\"name\":\"Alice\"}]"), new() { ["type"] = "test" });
        var exec1 = fab.Execute(ff1, "parser");
        AssertTrue("valid json: execute ok", exec1);

        var stats1 = fab.GetProcessorStats();
        AssertTrue("valid json: parser processed", Stat(stats1, "parser", "processed") >= 1);
        AssertTrue("valid json: parser no errors", Stat(stats1, "parser", "errors") == 0);
        AssertTrue("valid json: sink processed", Stat(stats1, "sink", "processed") >= 1);
        AssertTrue("valid json: err not invoked", Stat(stats1, "err", "processed") == 0);

        // Execute with invalid JSON — should flow: parser → err
        var ff2 = FlowFile.Create("not json"u8, new() { ["type"] = "bad" });
        var exec2 = fab.Execute(ff2, "parser");
        AssertTrue("bad json: execute ok", exec2);

        var stats2 = fab.GetProcessorStats();
        AssertTrue("bad json: err processed", Stat(stats2, "err", "processed") >= 1);
    }

    // --- Reliability hardening tests (Tier 0 / Tier 1 of the rock-solid plan) ---

    /// Concurrent writers to AddSourceConnection racing with readers in
    /// IngestFromSource. Before the ConcurrentDictionary + per-bucket lock
    /// conversion this would throw KeyNotFoundException or corrupt the
    /// inner list. Runs enough iterations to catch torn reads under load.
    static void TestSourceConnectionsConcurrent()
    {
        Console.WriteLine("--- Reliability: source-connections concurrent add/read ---");
        var config = MakeFlowConfig(new Dictionary<string, object?>
        {
            ["sink"] = MakeProc("UpdateAttribute", new() { ["key"] = "seen", ["value"] = "1" }),
        });
        var (fab, _, _) = CreateFabricWithConfig(config);

        // Register a fake source slot so the connection add/remove path
        // has a real name to attach to. We don't need to start it.
        var store = new MemoryContentStore();
        fab.AddSource(new CaravanFlow.StdLib.GenerateFlowFile("src", 1000, "body", "", "", 1));
        fab.StopSource("src"); // we just want the name registered, not ingesting

        var exceptions = 0;
        var done = false;
        var writer = Task.Run(() =>
        {
            try
            {
                for (int i = 0; i < 500 && !done; i++)
                {
                    fab.AddSourceConnection("src", "success", "sink");
                    fab.RemoveSourceConnection("src", "success", "sink");
                }
            }
            catch { Interlocked.Increment(ref exceptions); }
        });

        var reader = Task.Run(() =>
        {
            try
            {
                for (int i = 0; i < 500 && !done; i++)
                {
                    var snap = fab.GetSourceConnections();
                    // Read snapshot — iterating must not throw even as writer mutates.
                    foreach (var (_, byRel) in snap)
                        foreach (var (_, targets) in byRel)
                            foreach (var t in targets) _ = t.Length;
                }
            }
            catch { Interlocked.Increment(ref exceptions); }
        });

        Task.WaitAll(writer, reader);
        done = true;
        AssertIntEqual("no race exceptions", exceptions, 0);
    }

    /// SplitRecord used to leak two refs on each child's singleton
    /// RecordContent via the chained WithContent → WithAttribute →
    /// WithAttribute pattern. Run many iterations and assert each child
    /// emerges with the expected attributes + exactly one record — any
    /// attribute-map corruption or content double-release would surface.
    static void TestSplitRecordManyIterations()
    {
        Console.WriteLine("--- Reliability: SplitRecord stress (no shell/content leak symptoms) ---");
        var schema = new Schema("x", [new Field("v", FieldType.Long)]);
        var proc = new CaravanFlow.StdLib.SplitRecord();
        for (int iter = 0; iter < 100; iter++)
        {
            var recs = new List<Record>();
            for (int r = 0; r < 20; r++)
            {
                var rec = new Record(schema); rec.SetField("v", (long)r); recs.Add(rec);
            }
            var ff = FlowFile.CreateWithContent(new RecordContent(schema, recs), new() { ["trace"] = "t" });
            var result = proc.Process(ff);
            if (result is not MultipleResult mr) { AssertTrue("multi result", false); return; }
            if (mr.FlowFiles.Count != 20) { AssertIntEqual("children count", mr.FlowFiles.Count, 20); return; }
            for (int i = 0; i < mr.FlowFiles.Count; i++)
            {
                var child = mr.FlowFiles[i];
                if (child.Content is not RecordContent crc || crc.Records.Count != 1)
                { AssertTrue($"iter {iter} child {i} 1-record", false); return; }
                if (!child.Attributes.TryGetValue("split.total", out var total) || total != "20")
                { AssertTrue($"iter {iter} child {i} split.total", false); return; }
                if (!child.Attributes.TryGetValue("trace", out var tr) || tr != "t")
                { AssertTrue($"iter {iter} child {i} trace preserved", false); return; }
            }
            foreach (var child in mr.FlowFiles) FlowFile.Return(child);
            MultipleResult.Return(mr);
        }
        AssertTrue("100 iterations of 20-record splits completed", true);
    }
}
