using System.Text;
using ZincFlow.Core;
using ZincFlow.Fabric;
using ZincFlow.StdLib;
using static ZincFlow.Tests.TestRunner;
using static ZincFlow.Tests.Helpers;

namespace ZincFlow.Tests;

public static class PipelineTests
{
    public static void RunAll()
    {
        TestPipelineJsonToAvroToCsv();
        TestPipelineCsvTransformEnrich();
        TestPipelineTextExtractAndRoute();
        TestPipelineSplitFanOutSink();
        TestPipelineFailureToErrorHandler();
        TestPipelineFailureNoHandler();
        TestPipelineLargeContentOffload();
        TestPipelineSmallContentInline();
        TestPipelineExpressionChain();
        TestPipelineHotReloadLiveTraffic();
        TestPipelineEmptyContent();
        TestPipelineBadJsonFailure();
        TestPipelineBadCsvRecovery();
        TestPipelineMultiFormatConversion();
    }

    static void TestPipelineJsonToAvroToCsv()
    {
        Console.WriteLine("--- Pipeline: JSON -> Record -> Avro -> Record -> CSV ---");
        var store = new MemoryContentStore();
        var json = "[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":25}]";

        // JSON -> Record
        var jsonToRec = new ConvertJSONToRecord("people", store);
        var ff1 = FlowFile.Create(Encoding.UTF8.GetBytes(json), new());
        var r1 = (SingleResult)jsonToRec.Process(ff1);
        AssertTrue("json->record", r1.FlowFile.Content is RecordContent);
        var rc1 = (RecordContent)r1.FlowFile.Content;
        AssertIntEqual("2 records from json", rc1.Records.Count, 2);

        // Record -> Avro
        var recToAvro = new ConvertRecordToAvro();
        var r2 = (SingleResult)recToAvro.Process(r1.FlowFile);
        AssertTrue("record->avro raw", r2.FlowFile.Content is Raw);

        // Avro -> Record (using schema from attribute)
        r2.FlowFile.Attributes.TryGetValue("avro.schema", out var schema);
        var avroToRec = new ConvertAvroToRecord("people", schema ?? "", store);
        var r3 = (SingleResult)avroToRec.Process(r2.FlowFile);
        AssertTrue("avro->record", r3.FlowFile.Content is RecordContent);
        var rc2 = (RecordContent)r3.FlowFile.Content;
        AssertIntEqual("2 records from avro", rc2.Records.Count, 2);

        // Record -> CSV
        var recToCsv = new ConvertRecordToCSV(',', true);
        var r4 = (SingleResult)recToCsv.Process(r3.FlowFile);
        var (csvBytes, _) = ContentHelpers.Resolve(store, r4.FlowFile.Content);
        var csv = Encoding.UTF8.GetString(csvBytes);
        AssertTrue("csv has Alice", csv.Contains("Alice"));
        AssertTrue("csv has Bob", csv.Contains("Bob"));
        AssertTrue("csv has header", csv.Contains("name"));
    }

    static void TestPipelineCsvTransformEnrich()
    {
        Console.WriteLine("--- Pipeline: CSV -> Record -> Transform -> Enrich -> CSV ---");
        var store = new MemoryContentStore();
        var csv = "email,name\nalice@test.com,alice\nbob@test.com,bob\n";

        // CSV -> Record
        var csvToRec = new ConvertCSVToRecord("users", ',', true, store);
        var ff = FlowFile.Create(Encoding.UTF8.GetBytes(csv), new());
        var r1 = (SingleResult)csvToRec.Process(ff);
        var rc = (RecordContent)r1.FlowFile.Content;
        AssertIntEqual("2 user records", rc.Records.Count, 2);

        // Transform: uppercase names, add source field
        var transform = new TransformRecord("toUpper:name;add:source:import;default:role:user");
        var r2 = (SingleResult)transform.Process(r1.FlowFile);
        var rc2 = (RecordContent)r2.FlowFile.Content;
        AssertEqual("uppercased name", rc2.Records[0]["name"]?.ToString() ?? "", "ALICE");
        AssertEqual("added source", rc2.Records[0]["source"]?.ToString() ?? "", "import");
        AssertEqual("default role", rc2.Records[1]["role"]?.ToString() ?? "", "user");

        // Enrich with expression on FlowFile attributes
        var enrich = new EvaluateExpression(new() { ["processed_by"] = "zinc-flow", ["record_count"] = "2" });
        var r3 = (SingleResult)enrich.Process(r2.FlowFile);
        r3.FlowFile.Attributes.TryGetValue("processed_by", out var procBy);
        AssertEqual("enriched attr", procBy ?? "", "zinc-flow");

        // Record -> CSV
        var recToCsv = new ConvertRecordToCSV(',', true);
        var r4 = (SingleResult)recToCsv.Process(r3.FlowFile);
        var (outBytes, _) = ContentHelpers.Resolve(store, r4.FlowFile.Content);
        var outCsv = Encoding.UTF8.GetString(outBytes);
        AssertTrue("output has ALICE", outCsv.Contains("ALICE"));
        AssertTrue("output has source", outCsv.Contains("import"));
    }

    static void TestPipelineTextExtractAndRoute()
    {
        Console.WriteLine("--- Pipeline: ExtractText -> EvaluateExpression ---");
        var store = new MemoryContentStore();

        // Log line: extract fields
        var logLine = "2026-04-08 ERROR [auth] login failed for user=alice ip=10.0.1.5";
        var extract = new ExtractText(
            @"(?<date>\d{4}-\d{2}-\d{2})\s+(?<level>\w+)\s+\[(?<component>\w+)\]\s+(?<message>.+)",
            "", store);
        var ff = FlowFile.Create(Encoding.UTF8.GetBytes(logLine), new());
        var r1 = (SingleResult)extract.Process(ff);
        r1.FlowFile.Attributes.TryGetValue("level", out var level);
        r1.FlowFile.Attributes.TryGetValue("component", out var component);
        r1.FlowFile.Attributes.TryGetValue("date", out var date);
        AssertEqual("extracted level", level ?? "", "ERROR");
        AssertEqual("extracted component", component ?? "", "auth");
        AssertEqual("extracted date", date ?? "", "2026-04-08");

        // Expression: derive new attributes
        var expr = new EvaluateExpression(new()
        {
            ["alert_key"] = "${component:toUpper()}:${level}",
            ["is_error"] = "${level:contains('ERROR')}"
        });
        var r2 = (SingleResult)expr.Process(r1.FlowFile);
        r2.FlowFile.Attributes.TryGetValue("alert_key", out var alertKey);
        r2.FlowFile.Attributes.TryGetValue("is_error", out var isError);
        AssertEqual("alert key", alertKey ?? "", "AUTH:ERROR");
        AssertEqual("is error", isError ?? "", "true");
    }

    static void TestPipelineSplitFanOutSink()
    {
        Console.WriteLine("--- Pipeline: SplitText -> fan-out to multiple sinks ---");
        var store = new MemoryContentStore();
        var content = "line1\nline2\nline3";
        var split = new SplitText(@"\n", 0, store);
        var ff = FlowFile.Create(Encoding.UTF8.GetBytes(content), new() { ["source"] = "batch" });
        var result = split.Process(ff);
        AssertTrue("split produces multiple", result is MultipleResult);
        var multi = (MultipleResult)result;
        AssertIntEqual("3 splits", multi.FlowFiles.Count, 3);

        // Each split should have split.index attribute
        multi.FlowFiles[0].Attributes.TryGetValue("split.index", out var idx0);
        multi.FlowFiles[2].Attributes.TryGetValue("split.index", out var idx2);
        AssertEqual("first split index", idx0 ?? "", "0");
        AssertEqual("third split index", idx2 ?? "", "2");

        // Simulate fan-out: each split goes through UpdateAttribute
        var tag = new UpdateAttribute("processed", "true");
        foreach (var splitFf in multi.FlowFiles)
        {
            var r = (SingleResult)tag.Process(splitFf);
            r.FlowFile.Attributes.TryGetValue("processed", out var proc);
            AssertTrue("split processed", proc == "true");
        }
    }

    static void TestPipelineFailureToErrorHandler()
    {
        Console.WriteLine("--- Pipeline: Failure -> error handler connection via Fabric ---");
        var ctx = TestContext();

        // Pipeline: parser with failure connection to error-handler
        var config = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["parser"] = new Dictionary<string, object?>
                    {
                        ["type"] = "ConvertJSONToRecord",
                        ["config"] = new Dictionary<string, object?> { ["schema_name"] = "data" },
                        ["connections"] = new Dictionary<string, object?>
                        {
                            ["success"] = new List<object?> { "next" },
                            ["failure"] = new List<object?> { "error-handler" }
                        }
                    },
                    ["next"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "parsed", ["value"] = "true" }
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

        // Bad JSON -> should route to error-handler
        var ff = FlowFile.Create("not valid json at all"u8, new() { ["type"] = "order" });
        var ok = fab.Execute(ff, "parser");
        AssertTrue("execute ok", ok);

        var stats = fab.GetProcessorStats();
        AssertTrue("error handler got 1", stats["error-handler"]["processed"] == 1);
        AssertTrue("next not invoked", stats["next"]["processed"] == 0);
    }

    static void TestPipelineFailureNoHandler()
    {
        Console.WriteLine("--- Pipeline: Failure with no handler (log+drop) ---");
        var ctx = TestContext();
        var logProv = new LoggingProvider(); logProv.Enable();
        ctx.AddProvider(logProv);

        // Pipeline: parser with NO failure connection
        var config = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["parser"] = new Dictionary<string, object?>
                    {
                        ["type"] = "ConvertJSONToRecord",
                        ["config"] = new Dictionary<string, object?> { ["schema_name"] = "data" },
                        ["connections"] = new Dictionary<string, object?>
                        {
                            ["success"] = new List<object?> { "next" }
                        }
                    },
                    ["next"] = new Dictionary<string, object?>
                    {
                        ["type"] = "UpdateAttribute",
                        ["config"] = new Dictionary<string, object?> { ["key"] = "parsed", ["value"] = "true" }
                    }
                }
            }
        };
        var fab = BuildFabric(config, ctx);

        // Bad JSON -> no failure connection -> log+drop
        var ff = FlowFile.Create("broken"u8, new());
        var ok = fab.Execute(ff, "parser");
        AssertTrue("execute ok", ok);

        var stats = fab.GetProcessorStats();
        AssertTrue("parser processed", stats["parser"]["processed"] == 1);
        AssertTrue("parser has error", stats["parser"]["errors"] == 1);
        AssertTrue("next not invoked", stats["next"]["processed"] == 0);
    }

    static void TestPipelineLargeContentOffload()
    {
        Console.WriteLine("--- Pipeline: Large content -> offload to content store ---");
        var store = new FileContentStore("/tmp/zinc-flow-csharp-test-large");
        var cleanup = new ContentStoreCleanup(store, "/tmp/zinc-flow-csharp-test-large");
        ContentStoreCleanup.Instance = cleanup;

        try
        {
            // Create content larger than offload threshold (default 256KB)
            var oldThreshold = ContentHelpers.ClaimThreshold;
            ContentHelpers.ClaimThreshold = 100; // 100 bytes for test

            var largeData = new byte[500];
            Array.Fill(largeData, (byte)'X');
            var content = ContentHelpers.MaybeOffload(store, largeData);
            AssertTrue("offloaded to claim", content is ClaimContent);

            var ff = FlowFile.CreateWithContent(content, new() { ["size"] = "500" });

            // Process through ReplaceText (needs to resolve content)
            var replace = new ReplaceText("X{5}", "YYYYY", "first", store);
            var result = (SingleResult)replace.Process(ff);
            var (outData, err) = ContentHelpers.Resolve(store, result.FlowFile.Content);
            AssertTrue("no error", err == "");
            AssertTrue("content processed", outData.Length > 0);

            ContentHelpers.ClaimThreshold = oldThreshold;

            // Cleanup
            cleanup.Sweep();
        }
        finally
        {
            if (Directory.Exists("/tmp/zinc-flow-csharp-test-large"))
                Directory.Delete("/tmp/zinc-flow-csharp-test-large", true);
        }
    }

    static void TestPipelineSmallContentInline()
    {
        Console.WriteLine("--- Pipeline: Small content stays inline (Raw) ---");
        var store = new MemoryContentStore();
        var smallData = "{\"name\":\"test\"}";
        var ff = FlowFile.Create(Encoding.UTF8.GetBytes(smallData), new());

        AssertTrue("small content is Raw", ff.Content is Raw);
        AssertTrue("small content size", ff.Content.Size < 1000);

        // Process through JSON -> Record -> JSON roundtrip
        var toRec = new ConvertJSONToRecord("test", store);
        var r1 = (SingleResult)toRec.Process(ff);
        var toJson = new ConvertRecordToJSON();
        var r2 = (SingleResult)toJson.Process(r1.FlowFile);
        AssertTrue("output is Raw", r2.FlowFile.Content is Raw);

        var (outBytes, _) = ContentHelpers.Resolve(store, r2.FlowFile.Content);
        var outJson = Encoding.UTF8.GetString(outBytes);
        AssertTrue("roundtrip has name", outJson.Contains("test"));
    }

    static void TestPipelineExpressionChain()
    {
        Console.WriteLine("--- Pipeline: Chained expressions ---");
        var ff = FlowFile.Create("payload"u8, new()
        {
            ["first_name"] = "alice",
            ["last_name"] = "smith",
            ["env"] = "production"
        });

        // First expression: compute derived attributes
        var expr1 = new EvaluateExpression(new()
        {
            ["full_name"] = "${first_name:toUpper()} ${last_name:toUpper()}",
            ["env_short"] = "${env:substring(0,4)}"
        });
        var r1 = (SingleResult)expr1.Process(ff);
        r1.FlowFile.Attributes.TryGetValue("full_name", out var fullName);
        r1.FlowFile.Attributes.TryGetValue("env_short", out var envShort);
        AssertEqual("full name", fullName ?? "", "ALICE SMITH");
        AssertEqual("env short", envShort ?? "", "prod");

        // Second expression: use derived attributes
        var expr2 = new EvaluateExpression(new()
        {
            ["greeting"] = "Hello ${full_name}!",
            ["tag"] = "${env_short:append('-v1')}"
        });
        var r2 = (SingleResult)expr2.Process(r1.FlowFile);
        r2.FlowFile.Attributes.TryGetValue("greeting", out var greeting);
        r2.FlowFile.Attributes.TryGetValue("tag", out var tag);
        AssertEqual("greeting", greeting ?? "", "Hello ALICE SMITH!");
        AssertEqual("tag", tag ?? "", "prod-v1");
    }

    static void TestPipelineHotReloadLiveTraffic()
    {
        Console.WriteLine("--- Pipeline: Hot reload during live processing ---");
        // Config: tagger -> logger (2 processors)
        var config1 = MakeFlowConfig(new Dictionary<string, object?>
        {
            ["tagger"] = MakeProc("UpdateAttribute", new() { ["key"] = "env", ["value"] = "dev" },
                connections: new() { ["success"] = new List<string> { "logger" } }),
            ["logger"] = MakeProc("LogAttribute", new() { ["prefix"] = "test" })
        });
        var (fab, _, _) = CreateFabricWithConfig(config1);

        // Execute before reload
        var ff1 = FlowFile.Create("before"u8, new() { ["type"] = "order" });
        fab.Execute(ff1, "tagger");
        var statsBefore = fab.GetStats();
        AssertTrue("processed before reload", (long)statsBefore["processed"] > 0);

        // Hot reload: add a third processor, change connections
        var config2 = MakeFlowConfig(new Dictionary<string, object?>
        {
            ["tagger"] = MakeProc("UpdateAttribute", new() { ["key"] = "env", ["value"] = "prod" },
                connections: new() { ["success"] = new List<string> { "enricher" } }),
            ["enricher"] = MakeProc("UpdateAttribute", new() { ["key"] = "version", ["value"] = "2.0" },
                connections: new() { ["success"] = new List<string> { "logger" } }),
            ["logger"] = MakeProc("LogAttribute", new() { ["prefix"] = "reload-test" })
        });
        var (added, _, updated, connChanged) = fab.ReloadFlow(config2);
        AssertTrue("reload added or updated", added + updated + connChanged > 0);
        AssertIntEqual("3 processors after reload", fab.GetProcessorNames().Count, 3);

        // Execute after reload — should flow through 3 processors
        var ff2 = FlowFile.Create("after"u8, new() { ["type"] = "order" });
        fab.Execute(ff2, "tagger");
        var statsAfter = fab.GetStats();
        AssertTrue("processed after reload", (long)statsAfter["processed"] > (long)statsBefore["processed"]);
    }

    static void TestPipelineEmptyContent()
    {
        Console.WriteLine("--- Pipeline: Empty content handling ---");
        var store = new MemoryContentStore();

        // Empty content through processors — should not crash
        var ff = FlowFile.Create(Array.Empty<byte>(), new() { ["type"] = "empty" });

        // ReplaceText with empty content
        var replace = new ReplaceText("anything", "replaced", "all", store);
        var r1 = replace.Process(ff);
        AssertTrue("replace empty ok", r1 is SingleResult);

        // ConvertJSONToRecord with empty content -> failure
        var toRec = new ConvertJSONToRecord("test", store);
        var r2 = toRec.Process(ff);
        AssertTrue("json parse empty fails", r2 is FailureResult);

        // ExtractText with empty content — no match, pass through
        var extract = new ExtractText(@"(\w+)", "", store);
        var r3 = extract.Process(ff);
        AssertTrue("extract empty passthrough", r3 is SingleResult);
    }

    static void TestPipelineBadJsonFailure()
    {
        Console.WriteLine("--- Pipeline: Bad JSON -> failure path ---");
        var store = new MemoryContentStore();

        // Various bad JSON inputs
        var badInputs = new[] { "not json", "{incomplete", "[1,2,", "", "null" };
        var proc = new ConvertJSONToRecord("test", store);
        int failCount = 0;
        foreach (var bad in badInputs)
        {
            var ff = FlowFile.Create(Encoding.UTF8.GetBytes(bad), new());
            var result = proc.Process(ff);
            if (result is FailureResult) failCount++;
        }
        AssertTrue("most bad inputs fail", failCount >= 4);
    }

    static void TestPipelineBadCsvRecovery()
    {
        Console.WriteLine("--- Pipeline: Malformed CSV recovery ---");
        var store = new MemoryContentStore();

        // CSV with inconsistent columns — reader should handle gracefully
        var csv = "a,b,c\n1,2\n3,4,5,6\n7,8,9\n";
        var proc = new ConvertCSVToRecord("test", ',', true, store);
        var ff = FlowFile.Create(Encoding.UTF8.GetBytes(csv), new());
        var result = proc.Process(ff);
        AssertTrue("csv recovery succeeds", result is SingleResult);
        var rc = (RecordContent)((SingleResult)result).FlowFile.Content;
        AssertIntEqual("3 rows parsed", rc.Records.Count, 3);
        // Short row: missing columns should not crash
        AssertTrue("short row has a", rc.Records[0].ContainsKey("a"));
    }

    static void TestPipelineMultiFormatConversion()
    {
        Console.WriteLine("--- Pipeline: JSON -> Avro -> JSON -> CSV -> JSON roundtrip ---");
        var store = new MemoryContentStore();

        // Start with JSON
        var json = "[{\"product\":\"Widget\",\"price\":9.99},{\"product\":\"Gadget\",\"price\":19.99}]";
        var ff = FlowFile.Create(Encoding.UTF8.GetBytes(json), new());

        // JSON -> Record
        var r1 = (SingleResult)new ConvertJSONToRecord("products", store).Process(ff);

        // Record -> Avro
        var r2 = (SingleResult)new ConvertRecordToAvro().Process(r1.FlowFile);
        r2.FlowFile.Attributes.TryGetValue("avro.schema", out var avroSchema);

        // Avro -> Record
        var r3 = (SingleResult)new ConvertAvroToRecord("products", avroSchema ?? "", store).Process(r2.FlowFile);

        // Record -> JSON (verify data survived)
        var r4 = (SingleResult)new ConvertRecordToJSON().Process(r3.FlowFile);
        var (jsonBytes, _) = ContentHelpers.Resolve(store, r4.FlowFile.Content);
        var jsonStr = Encoding.UTF8.GetString(jsonBytes);
        AssertTrue("json has Widget", jsonStr.Contains("Widget"));
        AssertTrue("json has Gadget", jsonStr.Contains("Gadget"));

        // JSON -> Record -> CSV
        var r5 = (SingleResult)new ConvertJSONToRecord("products", store).Process(r4.FlowFile);
        var r6 = (SingleResult)new ConvertRecordToCSV(',', true).Process(r5.FlowFile);
        var (csvBytes, _) = ContentHelpers.Resolve(store, r6.FlowFile.Content);
        var csvStr = Encoding.UTF8.GetString(csvBytes);
        AssertTrue("csv has product header", csvStr.Contains("product"));
        AssertTrue("csv has Widget", csvStr.Contains("Widget"));

        // CSV -> Record -> JSON (full circle)
        var r7 = (SingleResult)new ConvertCSVToRecord("products", ',', true, store).Process(r6.FlowFile);
        var r8 = (SingleResult)new ConvertRecordToJSON().Process(r7.FlowFile);
        var (finalBytes, _) = ContentHelpers.Resolve(store, r8.FlowFile.Content);
        var finalJson = Encoding.UTF8.GetString(finalBytes);
        AssertTrue("final has Widget", finalJson.Contains("Widget"));
        AssertTrue("final has Gadget", finalJson.Contains("Gadget"));
    }
}
