using System.Text;
using ZincFlow.Core;
using ZincFlow.Fabric;
using ZincFlow.StdLib;

namespace ZincFlow.Tests;

/// <summary>
/// Test suite for zinc-flow-csharp — direct pipeline executor (Fabric.Execute).
/// Run via: ./zinc test
/// </summary>
public static class TestSuite
{
    private static int _pass, _fail;

    public static int Run()
    {
        _pass = 0; _fail = 0;
        Console.WriteLine("=== zinc-flow-csharp test suite ===");
        Console.WriteLine();

        // Core types
        TestFlowFileBasics();
        TestContentTypes();
        TestContentStore();
        TestResolveErrors();

        // V3 serde
        TestV3Roundtrip();
        TestV3MultipleRoundtrip();
        TestV3EmptyAttributes();
        TestV3EmptyContent();

        // Processors
        TestUpdateAttribute();
        TestLogAttribute();
        TestConvertJSONToRecord();
        TestConvertRecordToJSON();
        TestJSONRoundtrip();
        TestConvertJSONToRecordEmpty();

        // Routing engine
        TestRoutingEQ();
        TestRoutingNEQ();
        TestRoutingContains();
        TestRoutingStartsEndsWith();
        TestRoutingExists();
        TestRoutingComposite();
        TestRoutingNoMatch();

        // Fabric integration (Execute-based)
        TestFabricMultiHop();
        TestFabricDlqFromFailure();

        // Provider/context
        TestScopedContextAccess();
        TestProviderLifecycle();

        // Phase 2a processors
        TestPutFile();
        TestPutStdout();
        TestConnectorSourceLifecycle();
        TestGetFile();
        TestListenHTTP();

        // Phase 2b/2c observability + hardening
        TestStructuredLogging();
        TestConfigValidation();
        TestProvenance();
        TestContentStoreCleanup();

        // Scenarios (Execute-based)
        TestScenarioProcessorChain();
        TestScenarioScopedProviderIsolation();
        TestScenarioIRSFanOut();

        TestMaxHopCycleDetection();

        // E2E integration tests (Execute-based)
        TestE2EFullPipeline();
        TestE2EContentStoreLifecycle();
        TestE2EProvenanceChain();
        TestE2EListenHTTPPipeline();

        // StdLib expansion: codecs
        TestAvroZigzagVarint();
        TestAvroRoundtrip();
        TestAvroMultipleRecords();
        TestCsvRoundtrip();
        TestCsvQuotedFields();
        TestCsvNoHeader();

        // StdLib expansion: text processors
        TestReplaceText();
        TestExtractText();
        TestSplitText();

        // StdLib expansion: record conversion processors
        TestConvertAvroToRecord();
        TestConvertRecordToAvro();
        TestConvertCSVToRecord();
        TestConvertRecordToCSV();
        TestAvroJsonRoundtrip();

        // StdLib expansion: expression processors
        TestEvaluateExpression();
        TestEvaluateExpressionFunctions();
        TestTransformRecord();

        // DAG validator + connection tests
        TestDagValidatorValid();
        TestDagValidatorCycle();
        TestDagValidatorInvalidTarget();
        TestDagValidatorEntryPoints();
        TestConnectionFanOut();
        TestSinkNoConnections();

        // Comprehensive processor pipeline scenarios
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

        // Hot reload
        TestHotReloadAddProcessor();
        TestHotReloadRemoveProcessor();
        TestHotReloadUpdateProcessor();
        TestHotReloadConnections();
        TestHotReloadNoChange();
        TestHotReloadEndToEnd();

        // New Execute-based tests
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
        TestE2EFullPipelineFormats();
        TestMultipleResultEdgeCases();

        // PollingSource
        TestPollingSourceLifecycle();
        TestPollingSourceBackpressure();

        // E2E content-verifying scenarios
        TestE2ECsvEtlPipeline();
        TestE2EFanOutContentIntegrity();
        TestE2ECascadingFailure();
        TestE2EHotReloadDataIntegrity();
        TestE2EAttributeAccumulation();

        Console.WriteLine();
        Console.WriteLine($"=== {_pass} passed, {_fail} failed ({_pass + _fail} total) ===");
        return _fail;
    }

    // --- Assertion helpers ---

    static void AssertTrue(string label, bool value)
    {
        if (value) { _pass++; Console.WriteLine($"  PASS: {label}"); }
        else { _fail++; Console.WriteLine($"  FAIL: {label} — expected true"); }
    }

    static void AssertFalse(string label, bool value) => AssertTrue(label, !value);

    static void AssertEqual(string label, string actual, string expected)
    {
        if (actual == expected) { _pass++; Console.WriteLine($"  PASS: {label}"); }
        else { _fail++; Console.WriteLine($"  FAIL: {label} — expected '{expected}', got '{actual}'"); }
    }

    static void AssertIntEqual(string label, int actual, int expected)
    {
        if (actual == expected) { _pass++; Console.WriteLine($"  PASS: {label}"); }
        else { _fail++; Console.WriteLine($"  FAIL: {label} — expected {expected}, got {actual}"); }
    }

    // --- Test helpers ---

    static ProcessorContext TestContext()
    {
        var store = new MemoryContentStore();
        var cp = new ContentProvider("content", store); cp.Enable();
        var ctx = new ProcessorContext(); ctx.AddProvider(cp);
        return ctx;
    }

    static ScopedContext TestScopedCtx()
    {
        var store = new MemoryContentStore();
        var cp = new ContentProvider("content", store); cp.Enable();
        return new ScopedContext(new Dictionary<string, IProvider> { ["content"] = cp });
    }

    static byte[] TestJsonArray() => """[{"name": "Alice", "amount": 42}]"""u8.ToArray();

    static ZincFlow.Fabric.Fabric BuildFabric(Dictionary<string, object?> config, ProcessorContext? ctx = null)
    {
        ctx ??= TestContext();
        var reg = new Registry();
        BuiltinProcessors.RegisterAll(reg);
        var fab = new ZincFlow.Fabric.Fabric(reg, ctx);
        fab.LoadFlow(config);
        return fab;
    }

    // --- Core: FlowFile ---

    static void TestFlowFileBasics()
    {
        Console.WriteLine("--- FlowFile Basics ---");
        var ff = FlowFile.Create("hello"u8, new() { ["type"] = "order", ["source"] = "test" });
        AssertTrue("id starts with ff-", ff.Id.StartsWith("ff-"));
        AssertTrue("type attr", ff.Attributes.TryGetValue("type", out var t) && t == "order");
        AssertTrue("source attr", ff.Attributes.TryGetValue("source", out var s) && s == "test");

        var ff2 = FlowFile.WithAttribute(ff, "env", "dev");
        AssertTrue("added env", ff2.Attributes.TryGetValue("env", out var e) && e == "dev");
        AssertTrue("same id after WithAttribute", ff2.NumericId == ff.NumericId);
        AssertTrue("original attrs preserved", ff2.Attributes.TryGetValue("type", out var t2) && t2 == "order");

        var ff3 = FlowFile.WithContent(ff, new Raw("new data"u8));
        AssertTrue("same id after WithContent", ff3.NumericId == ff.NumericId);
    }

    // --- Core: Content types ---

    static void TestContentTypes()
    {
        Console.WriteLine("--- Content Types ---");
        var raw = new Raw("data"u8);
        AssertFalse("raw is not record", raw is RecordContent);
        AssertIntEqual("raw size", raw.Size, 4);

        var claim = new ClaimContent("claim-1", 1024);
        AssertFalse("claim is not record", claim is RecordContent);
        AssertIntEqual("claim size", claim.Size, 1024);
    }

    // --- Core: ContentStore ---

    static void TestContentStore()
    {
        Console.WriteLine("--- Content Store ---");
        var store = new MemoryContentStore();
        var claimId = store.Store("hello world"u8.ToArray());
        AssertTrue("claim id not empty", claimId != "");
        AssertTrue("claim exists", store.Exists(claimId));

        var retrieved = store.Retrieve(claimId);
        AssertEqual("retrieve content", Encoding.UTF8.GetString(retrieved), "hello world");

        store.Delete(claimId);
        AssertFalse("deleted claim gone", store.Exists(claimId));

        // maybeOffload — small content stays Raw
        var small = ContentHelpers.MaybeOffload(store, "small"u8.ToArray());
        AssertTrue("small stays raw", small is Raw);

        // resolve Raw
        var (data, err) = ContentHelpers.Resolve(store, new Raw("test"u8));
        AssertEqual("resolve raw", Encoding.UTF8.GetString(data), "test");
        AssertEqual("resolve raw no error", err, "");
    }

    static void TestResolveErrors()
    {
        Console.WriteLine("--- Resolve Errors ---");
        var store = new MemoryContentStore();

        var (data1, err1) = ContentHelpers.Resolve(store, new ClaimContent("nonexistent", 100));
        AssertIntEqual("missing claim returns empty", data1.Length, 0);

        var (_, err2) = ContentHelpers.Resolve(store, new RecordContent(new(), new()));
        AssertTrue("records resolve has error", err2 != "");

        var cid = store.Store("claimed data"u8.ToArray());
        var (data3, err3) = ContentHelpers.Resolve(store, new ClaimContent(cid, 12));
        AssertEqual("resolve claim", Encoding.UTF8.GetString(data3), "claimed data");
        AssertEqual("resolve claim no error", err3, "");
    }

    // --- V3 Serde ---

    static void TestV3Roundtrip()
    {
        Console.WriteLine("--- V3 Roundtrip ---");
        var ff = FlowFile.Create("Hello, NiFi!"u8, new() { ["filename"] = "test.txt", ["type"] = "doc" });
        var (content, _) = ContentHelpers.Resolve(new MemoryContentStore(), ff.Content);
        var packed = FlowFileV3.Pack(ff, content);
        AssertTrue("packed not empty", packed.Length > 0);
        AssertEqual("magic header", Encoding.UTF8.GetString(packed, 0, 7), "NiFiFF3");

        var (unpacked, _, error) = FlowFileV3.Unpack(packed, 0);
        AssertEqual("no unpack error", error, "");
        AssertTrue("filename survives", unpacked!.Attributes.TryGetValue("filename", out var fn) && fn == "test.txt");
        AssertTrue("type survives", unpacked.Attributes.TryGetValue("type", out var tp) && tp == "doc");

        var (bytes, _) = ContentHelpers.Resolve(new MemoryContentStore(), unpacked.Content);
        AssertEqual("content survives", Encoding.UTF8.GetString(bytes), "Hello, NiFi!");
    }

    static void TestV3MultipleRoundtrip()
    {
        Console.WriteLine("--- V3 Multiple Roundtrip ---");
        var ff1 = FlowFile.Create("first"u8, new() { ["index"] = "1" });
        var ff2 = FlowFile.Create("second"u8, new() { ["index"] = "2" });
        var store = new MemoryContentStore();
        var (c1, _) = ContentHelpers.Resolve(store, ff1.Content);
        var (c2, _) = ContentHelpers.Resolve(store, ff2.Content);
        var packed = FlowFileV3.PackMultiple([ff1, ff2], [c1, c2]);
        var all = FlowFileV3.UnpackAll(packed);
        AssertIntEqual("unpacked count", all.Count, 2);
        AssertTrue("first index", all[0].Attributes.TryGetValue("index", out var i1) && i1 == "1");
        AssertTrue("second index", all[1].Attributes.TryGetValue("index", out var i2) && i2 == "2");
    }

    static void TestV3EmptyAttributes()
    {
        Console.WriteLine("--- V3 Empty Attributes ---");
        var ff = FlowFile.Create("payload"u8, new());
        var (content, _) = ContentHelpers.Resolve(new MemoryContentStore(), ff.Content);
        var packed = FlowFileV3.Pack(ff, content);
        var (unpacked, _, error) = FlowFileV3.Unpack(packed, 0);
        AssertEqual("no error", error, "");
        AssertIntEqual("zero attrs", unpacked!.Attributes.Count, 0);
    }

    static void TestV3EmptyContent()
    {
        Console.WriteLine("--- V3 Empty Content ---");
        var ff = FlowFile.Create(ReadOnlySpan<byte>.Empty, new() { ["tag"] = "empty" });
        var packed = FlowFileV3.Pack(ff, []);
        var (unpacked, _, error) = FlowFileV3.Unpack(packed, 0);
        AssertEqual("no error", error, "");
        AssertTrue("tag preserved", unpacked!.Attributes.TryGetValue("tag", out var tag) && tag == "empty");
    }

    // --- Processors ---

    static void TestUpdateAttribute()
    {
        Console.WriteLine("--- UpdateAttribute ---");
        var proc = new UpdateAttribute("env", "prod");
        var ff = FlowFile.Create("data"u8, new() { ["type"] = "order" });
        var result = proc.Process(ff);
        AssertTrue("returns single", result is SingleResult);
        var outFf = ((SingleResult)result).FlowFile;
        AssertTrue("added env", outFf.Attributes.TryGetValue("env", out var e) && e == "prod");
        AssertTrue("kept type", outFf.Attributes.TryGetValue("type", out var t) && t == "order");
    }

    static void TestLogAttribute()
    {
        Console.WriteLine("--- LogAttribute ---");
        var proc = new LogAttribute("test");
        var ff = FlowFile.Create("data"u8, new() { ["type"] = "order" });
        var result = proc.Process(ff);
        AssertTrue("returns single", result is SingleResult);
        var outFf = ((SingleResult)result).FlowFile;
        AssertTrue("same id", outFf.NumericId == ff.NumericId);
    }

    static void TestConvertJSONToRecord()
    {
        Console.WriteLine("--- ConvertJSONToRecord ---");
        var proc = new ConvertJSONToRecord("order", new MemoryContentStore());
        var ff = FlowFile.Create(TestJsonArray(), new());
        var result = proc.Process(ff);
        AssertTrue("returns single", result is SingleResult);
        var outFf = ((SingleResult)result).FlowFile;
        AssertTrue("content is records", outFf.Content is RecordContent);
    }

    static void TestConvertRecordToJSON()
    {
        Console.WriteLine("--- ConvertRecordToJSON ---");
        var rc = new RecordContent(
            new() { ["name"] = "test" },
            [new() { ["name"] = (object?)"Bob" }]);
        var ff = FlowFile.CreateWithContent(rc, new());
        var proc = new ConvertRecordToJSON();
        var result = proc.Process(ff);
        AssertTrue("returns single", result is SingleResult);
        var outFf = ((SingleResult)result).FlowFile;
        AssertTrue("output is raw", outFf.Content is Raw);
        var (bytes, _) = ContentHelpers.Resolve(new MemoryContentStore(), outFf.Content);
        AssertTrue("json contains Bob", Encoding.UTF8.GetString(bytes).Contains("Bob"));
    }

    static void TestJSONRoundtrip()
    {
        Console.WriteLine("--- Processor: JSON Roundtrip ---");
        var rc = new RecordContent(
            new() { ["name"] = "geo" },
            [new() { ["city"] = (object?)"Portland" }]);
        var ff = FlowFile.CreateWithContent(rc, new());

        // Records -> JSON
        var toJson = new ConvertRecordToJSON();
        var step1 = toJson.Process(ff);
        AssertTrue("step1 returns single", step1 is SingleResult);
        var jsonFf = ((SingleResult)step1).FlowFile;
        AssertTrue("step1 is raw", jsonFf.Content is Raw);
        var (bytes, _) = ContentHelpers.Resolve(new MemoryContentStore(), jsonFf.Content);
        AssertTrue("json contains Portland", Encoding.UTF8.GetString(bytes).Contains("Portland"));

        // JSON -> Records
        var toRec = new ConvertJSONToRecord("geo", new MemoryContentStore());
        var step2 = toRec.Process(jsonFf);
        AssertTrue("step2 returns single", step2 is SingleResult);
        AssertTrue("step2 is records", ((SingleResult)step2).FlowFile.Content is RecordContent);
    }

    static void TestConvertJSONToRecordEmpty()
    {
        Console.WriteLine("--- ConvertJSONToRecord Empty ---");
        var proc = new ConvertJSONToRecord("test", new MemoryContentStore());
        var ff = FlowFile.Create("[]"u8, new());
        var result = proc.Process(ff);
        AssertTrue("empty json fails", result is FailureResult);
        AssertTrue("error message", ((FailureResult)result).Reason.Contains("no records"));
    }

    // --- Routing Engine ---

    static void TestRoutingEQ()
    {
        Console.WriteLine("--- Routing: EQ ---");
        var engine = new RulesEngine();
        engine.AddOrReplaceRuleset("test", [new RoutingRule("match-orders", "type", Operator.Eq, "order", "proc-1")]);

        var dests = new List<string>();
        engine.GetDestinations(AttributeMap.FromDict(new() { ["type"] = "order" }), dests);
        AssertIntEqual("eq match count", dests.Count, 1);
        AssertEqual("eq match dest", dests[0], "proc-1");

        engine.GetDestinations(AttributeMap.FromDict(new() { ["type"] = "event" }), dests);
        AssertIntEqual("eq no match", dests.Count, 0);
    }

    static void TestRoutingNEQ()
    {
        Console.WriteLine("--- Routing: NEQ ---");
        var engine = new RulesEngine();
        engine.AddOrReplaceRuleset("test", [new RoutingRule("not-orders", "type", Operator.Neq, "order", "proc-2")]);

        var dests = new List<string>();
        engine.GetDestinations(AttributeMap.FromDict(new() { ["type"] = "event" }), dests);
        AssertIntEqual("neq match count", dests.Count, 1);

        engine.GetDestinations(AttributeMap.FromDict(new() { ["type"] = "order" }), dests);
        AssertIntEqual("neq no match", dests.Count, 0);
    }

    static void TestRoutingContains()
    {
        Console.WriteLine("--- Routing: CONTAINS ---");
        var engine = new RulesEngine();
        engine.AddOrReplaceRuleset("test", [new RoutingRule("has-error", "message", Operator.Contains, "error", "dlq")]);

        var dests = new List<string>();
        engine.GetDestinations(AttributeMap.FromDict(new() { ["message"] = "got an error here" }), dests);
        AssertIntEqual("contains match", dests.Count, 1);

        engine.GetDestinations(AttributeMap.FromDict(new() { ["message"] = "all good" }), dests);
        AssertIntEqual("contains no match", dests.Count, 0);
    }

    static void TestRoutingStartsEndsWith()
    {
        Console.WriteLine("--- Routing: STARTSWITH/ENDSWITH ---");
        var engine = new RulesEngine();
        engine.AddOrReplaceRuleset("test", [
            new RoutingRule("starts", "path", Operator.StartsWith, "/api", "api-proc"),
            new RoutingRule("ends", "path", Operator.EndsWith, ".json", "json-proc")
        ]);

        var dests = new List<string>();
        engine.GetDestinations(AttributeMap.FromDict(new() { ["path"] = "/api/data.json" }), dests);
        AssertIntEqual("both match", dests.Count, 2);

        engine.GetDestinations(AttributeMap.FromDict(new() { ["path"] = "/api/data.xml" }), dests);
        AssertIntEqual("starts only", dests.Count, 1);
        AssertEqual("starts dest", dests[0], "api-proc");
    }

    static void TestRoutingExists()
    {
        Console.WriteLine("--- Routing: EXISTS ---");
        var engine = new RulesEngine();
        engine.AddOrReplaceRuleset("test", [new RoutingRule("has-priority", "priority", Operator.Exists, "", "priority-proc")]);

        var dests = new List<string>();
        engine.GetDestinations(AttributeMap.FromDict(new() { ["priority"] = "high" }), dests);
        AssertIntEqual("exists match", dests.Count, 1);

        engine.GetDestinations(AttributeMap.FromDict(new() { ["type"] = "order" }), dests);
        AssertIntEqual("exists no match", dests.Count, 0);
    }

    static void TestRoutingComposite()
    {
        Console.WriteLine("--- Routing: Composite AND/OR ---");

        // AND: type=order AND source=web
        var engine = new RulesEngine();
        engine.AddOrReplaceRuleset("test", [new RoutingRule("web-orders",
            new CompositeRule(new BaseRule("type", Operator.Eq, "order"), Joiner.And, new BaseRule("source", Operator.Eq, "web")),
            "web-order-proc")]);

        var dests = new List<string>();
        engine.GetDestinations(AttributeMap.FromDict(new() { ["type"] = "order", ["source"] = "web" }), dests);
        AssertIntEqual("and both match", dests.Count, 1);

        engine.GetDestinations(AttributeMap.FromDict(new() { ["type"] = "order", ["source"] = "api" }), dests);
        AssertIntEqual("and partial no match", dests.Count, 0);

        // OR: source=web OR source=mobile
        var orEngine = new RulesEngine();
        orEngine.AddOrReplaceRuleset("test", [new RoutingRule("any-source",
            new CompositeRule(new BaseRule("source", Operator.Eq, "web"), Joiner.Or, new BaseRule("source", Operator.Eq, "mobile")),
            "any-proc")]);

        orEngine.GetDestinations(AttributeMap.FromDict(new() { ["source"] = "web" }), dests);
        AssertIntEqual("or web match", dests.Count, 1);

        orEngine.GetDestinations(AttributeMap.FromDict(new() { ["source"] = "mobile" }), dests);
        AssertIntEqual("or mobile match", dests.Count, 1);

        orEngine.GetDestinations(AttributeMap.FromDict(new() { ["source"] = "api" }), dests);
        AssertIntEqual("or api no match", dests.Count, 0);
    }

    static void TestRoutingNoMatch()
    {
        Console.WriteLine("--- Routing: No Match ---");
        var engine = new RulesEngine();
        engine.AddOrReplaceRuleset("test", [new RoutingRule("specific", "type", Operator.Eq, "order", "proc-1")]);
        engine.AddOrReplaceRuleset("disabled-set", [new RoutingRule("disabled", "type", Operator.Eq, "order", "proc-2", false)]);

        var dests = new List<string>();
        engine.GetDestinations(AttributeMap.FromDict(new() { ["source"] = "test" }), dests);
        AssertIntEqual("missing attr no match", dests.Count, 0);

        engine.GetDestinations(AttributeMap.FromDict(new() { ["type"] = "order" }), dests);
        AssertIntEqual("disabled rule skipped", dests.Count, 1);
        AssertEqual("only enabled matched", dests[0], "proc-1");
    }

    // --- Fabric integration (Execute-based) ---

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
        AssertTrue("tag-env processed", stats["tag-env"]["processed"] == 1);
        AssertTrue("logger processed", stats["logger"]["processed"] == 1);
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
                        ["config"] = new Dictionary<string, object?> { ["schema_name"] = "test" },
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
        AssertTrue("parser processed good", pstats["parser"]["processed"] == 1);
        AssertTrue("sink processed good", pstats["sink"]["processed"] == 1);
        AssertTrue("error-handler not invoked for good", pstats["error-handler"]["processed"] == 0);

        // Bad input goes to error-handler
        var badFf = FlowFile.Create("not-json"u8, new());
        var ok2 = fab.Execute(badFf, "parser");
        AssertTrue("bad execute ok", ok2);
        pstats = fab.GetProcessorStats();
        AssertTrue("parser processed bad", pstats["parser"]["processed"] == 2);
        AssertTrue("error-handler invoked for bad", pstats["error-handler"]["processed"] == 1);
        AssertTrue("sink still 1", pstats["sink"]["processed"] == 1);
    }

    // --- Scoped Context ---

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

    // --- Provider Lifecycle ---

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

    // --- Scenarios (Execute-based) ---

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
        AssertTrue("tag-env processed 1", stats["tag-env"]["processed"] == 1);
        AssertTrue("logger processed 1", stats["logger"]["processed"] == 1);
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
        AssertTrue("tagger processed 1", stats["tagger"]["processed"] == 1);
        AssertTrue("dest-a processed 1", stats["dest-a"]["processed"] == 1);
        AssertTrue("dest-b processed 1", stats["dest-b"]["processed"] == 1);
        AssertTrue("dest-c processed 1", stats["dest-c"]["processed"] == 1);
    }

    // --- Phase 2: New Processors and Connectors ---

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

    static void TestMaxHopCycleDetection()
    {
        Console.WriteLine("--- Max Hop Cycle Detection via Execute ---");
        var ctx = TestContext();
        var logProv = new LoggingProvider(); logProv.Enable();
        ctx.AddProvider(logProv);

        // Create a cycle: A -> B -> A with max_hops = 5
        var config = new Dictionary<string, object?>
        {
            ["defaults"] = new Dictionary<string, object?>
            {
                ["max_hops"] = 5
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
        var totalProcessed = stats["proc-a"]["processed"] + stats["proc-b"]["processed"];
        AssertTrue("processed limited by max hops", totalProcessed <= 5);
        AssertTrue("errors recorded for hop limit", stats["proc-a"]["errors"] + stats["proc-b"]["errors"] >= 1);
    }

    // ===== E2E INTEGRATION TESTS =====

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

    // --- StdLib expansion: Avro codec ---

    static void TestAvroZigzagVarint()
    {
        Console.WriteLine("--- Avro: Zigzag Varint ---");
        // Zigzag encode/decode roundtrip
        AssertIntEqual("zigzag 0", AvroEncoding.ZigzagDecode(AvroEncoding.ZigzagEncode(0)), 0);
        AssertIntEqual("zigzag -1", (int)AvroEncoding.ZigzagDecode(AvroEncoding.ZigzagEncode(-1L)), -1);
        AssertIntEqual("zigzag 42", (int)AvroEncoding.ZigzagDecode(AvroEncoding.ZigzagEncode(42L)), 42);
        AssertIntEqual("zigzag -100", (int)AvroEncoding.ZigzagDecode(AvroEncoding.ZigzagEncode(-100L)), -100);

        // Varint write/read roundtrip
        using var ms = new MemoryStream();
        AvroEncoding.WriteVarint(ms, 300);
        var (val, n) = AvroEncoding.ReadVarint(ms.ToArray());
        AssertTrue("varint 300 roundtrip", val == 300);
        AssertTrue("varint bytes > 1", n > 1); // 300 needs 2+ bytes

        ms.SetLength(0);
        AvroEncoding.WriteVarint(ms, -1);
        var (val2, _) = AvroEncoding.ReadVarint(ms.ToArray());
        AssertTrue("varint -1 roundtrip", val2 == -1);
    }

    static void TestAvroRoundtrip()
    {
        Console.WriteLine("--- Avro: Single Record Roundtrip ---");
        var schema = new Schema("order", [
            new Field("name", FieldType.String),
            new Field("amount", FieldType.Int),
            new Field("price", FieldType.Double),
            new Field("active", FieldType.Boolean)
        ]);

        var record = new GenericRecord(schema);
        record.SetField("name", "Alice");
        record.SetField("amount", 42);
        record.SetField("price", 19.99);
        record.SetField("active", true);

        var writer = new AvroRecordWriter();
        var bytes = writer.Write([record], schema);
        AssertTrue("avro bytes not empty", bytes.Length > 0);

        var reader = new AvroRecordReader();
        var records = reader.Read(bytes, schema);
        AssertIntEqual("avro record count", records.Count, 1);
        AssertEqual("name", RecordHelpers.GetFieldString(records[0], "name"), "Alice");
        AssertIntEqual("amount", (int)(records[0].GetField("amount") ?? 0), 42);
        AssertTrue("price", Math.Abs((double)(records[0].GetField("price") ?? 0.0) - 19.99) < 0.001);
        AssertTrue("active", (bool)(records[0].GetField("active") ?? false));
    }

    static void TestAvroMultipleRecords()
    {
        Console.WriteLine("--- Avro: Multiple Records ---");
        var schema = new Schema("item", [
            new Field("id", FieldType.Long),
            new Field("label", FieldType.String)
        ]);

        var records = new List<GenericRecord>();
        for (int i = 0; i < 5; i++)
        {
            var r = new GenericRecord(schema);
            r.SetField("id", (long)i);
            r.SetField("label", $"item-{i}");
            records.Add(r);
        }

        var writer = new AvroRecordWriter();
        var bytes = writer.Write(records, schema);
        var reader = new AvroRecordReader();
        var decoded = reader.Read(bytes, schema);

        AssertIntEqual("5 records decoded", decoded.Count, 5);
        AssertEqual("first label", RecordHelpers.GetFieldString(decoded[0], "label"), "item-0");
        AssertEqual("last label", RecordHelpers.GetFieldString(decoded[4], "label"), "item-4");
    }

    // --- StdLib expansion: CSV codec ---

    static void TestCsvRoundtrip()
    {
        Console.WriteLine("--- CSV: Roundtrip ---");
        var schema = new Schema("data", [
            new Field("name", FieldType.String),
            new Field("age", FieldType.String),
            new Field("city", FieldType.String)
        ]);

        var records = new List<GenericRecord>();
        var r1 = new GenericRecord(schema);
        r1.SetField("name", "Alice"); r1.SetField("age", "30"); r1.SetField("city", "Portland");
        records.Add(r1);
        var r2 = new GenericRecord(schema);
        r2.SetField("name", "Bob"); r2.SetField("age", "25"); r2.SetField("city", "Seattle");
        records.Add(r2);

        var writer = new CsvRecordWriter();
        var bytes = writer.Write(records, schema);
        var csv = Encoding.UTF8.GetString(bytes);
        AssertTrue("csv has header", csv.StartsWith("name,age,city"));
        AssertTrue("csv has Alice", csv.Contains("Alice"));

        var reader = new CsvRecordReader();
        var decoded = reader.Read(bytes, new Schema("data", []));
        AssertIntEqual("csv 2 records", decoded.Count, 2);
        AssertEqual("csv name", RecordHelpers.GetFieldString(decoded[0], "name"), "Alice");
        AssertEqual("csv city", RecordHelpers.GetFieldString(decoded[1], "city"), "Seattle");
    }

    static void TestCsvQuotedFields()
    {
        Console.WriteLine("--- CSV: Quoted Fields ---");
        var csv = "name,note\nAlice,\"has a, comma\"\nBob,\"says \"\"hello\"\"\"\n";
        var reader = new CsvRecordReader();
        var records = reader.Read(Encoding.UTF8.GetBytes(csv), new Schema("test", []));
        AssertIntEqual("quoted 2 records", records.Count, 2);
        AssertEqual("comma in field", RecordHelpers.GetFieldString(records[0], "note"), "has a, comma");
        AssertEqual("escaped quotes", RecordHelpers.GetFieldString(records[1], "note"), "says \"hello\"");
    }

    static void TestCsvNoHeader()
    {
        Console.WriteLine("--- CSV: No Header ---");
        var csv = "Alice,30\nBob,25\n";
        var reader = new CsvRecordReader(',', false);
        var records = reader.Read(Encoding.UTF8.GetBytes(csv), new Schema("test", []));
        AssertIntEqual("no-header 2 records", records.Count, 2);
        AssertEqual("col0", RecordHelpers.GetFieldString(records[0], "col0"), "Alice");
        AssertEqual("col1", RecordHelpers.GetFieldString(records[1], "col1"), "25");
    }

    // --- StdLib expansion: text processors ---

    static void TestReplaceText()
    {
        Console.WriteLine("--- ReplaceText ---");
        var store = new MemoryContentStore();
        var proc = new ReplaceText(@"\d+", "NUM", "all", store);
        var ff = FlowFile.Create("order 123 has 456 items"u8, new());
        var result = proc.Process(ff);
        AssertTrue("replace returns single", result is SingleResult);
        var outFf = ((SingleResult)result).FlowFile;
        var (data, _) = ContentHelpers.Resolve(store, outFf.Content);
        AssertEqual("replaced all", Encoding.UTF8.GetString(data), "order NUM has NUM items");

        // First-only mode
        var proc2 = new ReplaceText(@"\d+", "NUM", "first", store);
        var ff2 = FlowFile.Create("order 123 has 456 items"u8, new());
        var result2 = proc2.Process(ff2);
        var outFf2 = ((SingleResult)result2).FlowFile;
        var (data2, _) = ContentHelpers.Resolve(store, outFf2.Content);
        AssertEqual("replaced first", Encoding.UTF8.GetString(data2), "order NUM has 456 items");
    }

    static void TestExtractText()
    {
        Console.WriteLine("--- ExtractText ---");
        var store = new MemoryContentStore();
        // Named groups
        var proc = new ExtractText(@"order (?<orderId>\d+) for (?<customer>\w+)", "", store);
        var ff = FlowFile.Create("order 789 for Alice"u8, new());
        var result = proc.Process(ff);
        var outFf = ((SingleResult)result).FlowFile;
        outFf.Attributes.TryGetValue("orderId", out var orderId);
        outFf.Attributes.TryGetValue("customer", out var customer);
        AssertEqual("extract orderId", orderId ?? "", "789");
        AssertEqual("extract customer", customer ?? "", "Alice");

        // No match -> pass through
        var ff2 = FlowFile.Create("no match here"u8, new());
        var result2 = proc.Process(ff2);
        AssertTrue("no match pass through", result2 is SingleResult);
    }

    static void TestSplitText()
    {
        Console.WriteLine("--- SplitText ---");
        var store = new MemoryContentStore();
        var proc = new SplitText(@"\n", 0, store);
        var ff = FlowFile.Create("line1\nline2\nline3"u8, new());
        var result = proc.Process(ff);
        AssertTrue("split returns multiple", result is MultipleResult);
        var multi = (MultipleResult)result;
        AssertIntEqual("split count", multi.FlowFiles.Count, 3);
    }

    // --- StdLib expansion: record conversion processors ---

    static void TestConvertAvroToRecord()
    {
        Console.WriteLine("--- ConvertAvroToRecord ---");
        var store = new MemoryContentStore();
        // First encode some records
        var schema = new Schema("order", [
            new Field("name", FieldType.String),
            new Field("qty", FieldType.Int)
        ]);
        var rec = new GenericRecord(schema);
        rec.SetField("name", "Widget");
        rec.SetField("qty", 10);
        var avroBytes = new AvroRecordWriter().Write([rec], schema);

        var proc = new ConvertAvroToRecord("order", "name:string,qty:int", store);
        var ff = FlowFile.Create(avroBytes, new());
        var result = proc.Process(ff);
        AssertTrue("avro->record returns single", result is SingleResult);
        var outFf = ((SingleResult)result).FlowFile;
        AssertTrue("content is RecordContent", outFf.Content is RecordContent);
        var rc = (RecordContent)outFf.Content;
        AssertIntEqual("1 record", rc.Records.Count, 1);
        AssertEqual("name field", rc.Records[0]["name"]?.ToString() ?? "", "Widget");
        AssertIntEqual("qty field", Convert.ToInt32(rc.Records[0]["qty"]), 10);
    }

    static void TestConvertRecordToAvro()
    {
        Console.WriteLine("--- ConvertRecordToAvro ---");
        var rc = new RecordContent(
            new() { ["name"] = "order" },
            [new() { ["item"] = (object?)"Gadget", ["count"] = (object?)5 }]);
        var ff = FlowFile.CreateWithContent(rc, new());
        var proc = new ConvertRecordToAvro();
        var result = proc.Process(ff);
        AssertTrue("record->avro returns single", result is SingleResult);
        var outFf = ((SingleResult)result).FlowFile;
        AssertTrue("output is Raw", outFf.Content is Raw);
        AssertTrue("has avro.schema attr", outFf.Attributes.ContainsKey("avro.schema"));
    }

    static void TestConvertCSVToRecord()
    {
        Console.WriteLine("--- ConvertCSVToRecord ---");
        var store = new MemoryContentStore();
        var csv = "name,age\nAlice,30\nBob,25\n";
        var proc = new ConvertCSVToRecord("people", ',', true, store);
        var ff = FlowFile.Create(Encoding.UTF8.GetBytes(csv), new());
        var result = proc.Process(ff);
        AssertTrue("csv->record returns single", result is SingleResult);
        var outFf = ((SingleResult)result).FlowFile;
        AssertTrue("content is RecordContent", outFf.Content is RecordContent);
        var rc = (RecordContent)outFf.Content;
        AssertIntEqual("2 records", rc.Records.Count, 2);
        AssertEqual("first name", rc.Records[0]["name"]?.ToString() ?? "", "Alice");
    }

    static void TestConvertRecordToCSV()
    {
        Console.WriteLine("--- ConvertRecordToCSV ---");
        var rc = new RecordContent(
            new() { ["name"] = "data" },
            [
                new() { ["x"] = (object?)"hello", ["y"] = (object?)"world" },
                new() { ["x"] = (object?)"foo", ["y"] = (object?)"bar" }
            ]);
        var ff = FlowFile.CreateWithContent(rc, new());
        var proc = new ConvertRecordToCSV(',', true);
        var result = proc.Process(ff);
        var outFf = ((SingleResult)result).FlowFile;
        var (data, _) = ContentHelpers.Resolve(new MemoryContentStore(), outFf.Content);
        var csv = Encoding.UTF8.GetString(data);
        AssertTrue("csv has header", csv.Contains("x,y") || csv.Contains("y,x"));
        AssertTrue("csv has data", csv.Contains("hello"));
    }

    static void TestAvroJsonRoundtrip()
    {
        Console.WriteLine("--- Avro <-> JSON cross-format ---");
        var store = new MemoryContentStore();
        // JSON -> Record -> Avro -> Record -> JSON
        var json = "[{\"name\":\"Eve\",\"score\":99}]";
        var jsonToRec = new ConvertJSONToRecord("test", store);
        var ff1 = FlowFile.Create(Encoding.UTF8.GetBytes(json), new());
        var r1 = jsonToRec.Process(ff1);
        AssertTrue("json->record ok", r1 is SingleResult);
        var recFf = ((SingleResult)r1).FlowFile;

        var recToAvro = new ConvertRecordToAvro();
        var r2 = recToAvro.Process(recFf);
        AssertTrue("record->avro ok", r2 is SingleResult);
        var avroFf = ((SingleResult)r2).FlowFile;
        AssertTrue("avro output is Raw", avroFf.Content is Raw);

        // Read avro.schema attribute to decode back
        avroFf.Attributes.TryGetValue("avro.schema", out var schemaDef);
        AssertTrue("has schema attr", !string.IsNullOrEmpty(schemaDef));

        var avroToRec = new ConvertAvroToRecord("test", schemaDef ?? "", store);
        var r3 = avroToRec.Process(avroFf);
        AssertTrue("avro->record ok", r3 is SingleResult);
        var recFf2 = ((SingleResult)r3).FlowFile;

        var recToJson = new ConvertRecordToJSON();
        var r4 = recToJson.Process(recFf2);
        var jsonFf = ((SingleResult)r4).FlowFile;
        var (jsonBytes, _) = ContentHelpers.Resolve(store, jsonFf.Content);
        var finalJson = Encoding.UTF8.GetString(jsonBytes);
        AssertTrue("roundtrip has Eve", finalJson.Contains("Eve"));
    }

    // --- StdLib expansion: expression processors ---

    static void TestEvaluateExpression()
    {
        Console.WriteLine("--- EvaluateExpression ---");
        var exprs = new Dictionary<string, string>
        {
            ["greeting"] = "Hello ${name}!",
            ["upper_env"] = "${env:toUpper()}",
            ["literal"] = "static-value"
        };
        var proc = new EvaluateExpression(exprs);
        var ff = FlowFile.Create("x"u8, new() { ["name"] = "Alice", ["env"] = "dev" });
        var result = proc.Process(ff);
        var outFf = ((SingleResult)result).FlowFile;
        outFf.Attributes.TryGetValue("greeting", out var greeting);
        outFf.Attributes.TryGetValue("upper_env", out var upperEnv);
        outFf.Attributes.TryGetValue("literal", out var literal);
        AssertEqual("greeting", greeting ?? "", "Hello Alice!");
        AssertEqual("upper_env", upperEnv ?? "", "DEV");
        AssertEqual("literal", literal ?? "", "static-value");
    }

    static void TestEvaluateExpressionFunctions()
    {
        Console.WriteLine("--- EvaluateExpression: Functions ---");
        var attrs = AttributeMap.FromDict(new() { ["val"] = "  Hello World  ", ["empty"] = "" });

        AssertEqual("toLower", EvaluateExpression.Evaluate("${val:toLower()}", attrs), "  hello world  ");
        AssertEqual("trim", EvaluateExpression.Evaluate("${val:trim()}", attrs), "Hello World");
        AssertEqual("length", EvaluateExpression.Evaluate("${val:length()}", attrs), "15");
        AssertEqual("replace", EvaluateExpression.Evaluate("${val:replace('World','Earth')}", attrs), "  Hello Earth  ");
        AssertEqual("append", EvaluateExpression.Evaluate("${val:trim():append('!')}", attrs), "Hello World!");
        AssertEqual("defaultIfEmpty", EvaluateExpression.Evaluate("${empty:defaultIfEmpty('fallback')}", attrs), "fallback");
        AssertEqual("contains true", EvaluateExpression.Evaluate("${val:contains('Hello')}", attrs), "true");
        AssertEqual("contains false", EvaluateExpression.Evaluate("${val:contains('xyz')}", attrs), "false");
    }

    static void TestTransformRecord()
    {
        Console.WriteLine("--- TransformRecord ---");
        var rc = new RecordContent(
            new() { ["name"] = "test" },
            [new() { ["first_name"] = (object?)"alice", ["age"] = (object?)"30", ["temp"] = (object?)"x" }]);
        var ff = FlowFile.CreateWithContent(rc, new());

        var proc = new TransformRecord("rename:first_name:name;remove:temp;add:source:api;toUpper:name;default:missing:none");
        var result = proc.Process(ff);
        var outFf = ((SingleResult)result).FlowFile;
        var outRc = (RecordContent)outFf.Content;
        var rec = outRc.Records[0];
        AssertEqual("renamed + upper", rec["name"]?.ToString() ?? "", "ALICE");
        AssertFalse("temp removed", rec.ContainsKey("temp"));
        AssertEqual("added source", rec["source"]?.ToString() ?? "", "api");
        AssertEqual("default missing", rec["missing"]?.ToString() ?? "", "none");
        AssertEqual("age preserved", rec["age"]?.ToString() ?? "", "30");
    }

    // --- DAG validator + connection tests ---

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
        AssertTrue("a has 1", stats["a"]["processed"] == 1);
        AssertTrue("b has 1", stats["b"]["processed"] == 1);
        AssertTrue("c has 1", stats["c"]["processed"] == 1);
        AssertTrue("src processed", stats["src"]["processed"] == 1);
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
        AssertTrue("sink processed 1", stats["sink"]["processed"] == 1);
        AssertTrue("sink no errors", stats["sink"]["errors"] == 0);
    }

    // --- Comprehensive processor pipeline scenarios ---

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

    // --- Hot reload tests ---

    static (ZincFlow.Fabric.Fabric, ProcessorContext, Registry) CreateFabricWithConfig(Dictionary<string, object?> config)
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

    static Dictionary<string, object?> MakeFlowConfig(Dictionary<string, object?> processors)
    {
        var flow = new Dictionary<string, object?> { ["processors"] = processors };
        return new Dictionary<string, object?> { ["flow"] = flow };
    }

    static Dictionary<string, object?> MakeProc(string type, Dictionary<string, string> config,
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
        AssertTrue("tag-env processed", stats["tag-env"]["processed"] == 1);
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

    // ===== NEW EXECUTE-BASED TESTS =====

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
        AssertTrue("tag processed", stats["tag"]["processed"] == 1);
        AssertTrue("tag no errors", stats["tag"]["errors"] == 0);
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
        AssertTrue("tag processed", stats["tag"]["processed"] == 1);
        AssertTrue("enrich processed", stats["enrich"]["processed"] == 1);
        AssertTrue("sink processed", stats["sink"]["processed"] == 1);
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
        AssertTrue("source processed 1", stats["source"]["processed"] == 1);
        AssertTrue("branch-a processed 1", stats["branch-a"]["processed"] == 1);
        AssertTrue("branch-b processed 1", stats["branch-b"]["processed"] == 1);
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
                        ["config"] = new Dictionary<string, object?> { ["schema_name"] = "data" },
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
        AssertTrue("parser processed", stats["parser"]["processed"] == 1);
        AssertTrue("error-sink received failure", stats["error-sink"]["processed"] == 1);
        AssertTrue("sink not invoked", stats["sink"]["processed"] == 0);
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
                        ["config"] = new Dictionary<string, object?> { ["schema_name"] = "data" },
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
        AssertTrue("parser processed", stats["parser"]["processed"] == 1);
        AssertTrue("parser error counted", stats["parser"]["errors"] == 1);
        AssertTrue("sink not invoked", stats["sink"]["processed"] == 0);
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
        AssertTrue("source processed", stats["source"]["processed"] == 1);
        // disabled-proc is skipped, FlowFile is returned to pool
        AssertTrue("disabled-proc not processed", stats["disabled-proc"]["processed"] == 0);
        AssertTrue("sink not reached", stats["sink"]["processed"] == 0);
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
        AssertTrue("splitter processed 1", stats["splitter"]["processed"] == 1);
        AssertTrue("sink processed 3 (one per split)", stats["sink"]["processed"] == 3);
    }

    static void TestExecuteMaxHops()
    {
        Console.WriteLine("--- Execute: Max Hops enforcement ---");
        var ctx = TestContext();
        var logProv = new LoggingProvider(); logProv.Enable();
        ctx.AddProvider(logProv);

        // Cycle with low max_hops
        var config = new Dictionary<string, object?>
        {
            ["defaults"] = new Dictionary<string, object?>
            {
                ["max_hops"] = 3
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
        var totalProcessed = stats["a"]["processed"] + stats["b"]["processed"];
        AssertTrue("processed limited by max hops", totalProcessed <= 3);
        var totalErrors = stats["a"]["errors"] + stats["b"]["errors"];
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
                ["max_concurrent_executions"] = 1
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

        // With max_concurrent_executions=1, a single synchronous Execute should succeed
        var ff1 = FlowFile.Create("test1"u8, new());
        var ok1 = fab.Execute(ff1, "tag");
        AssertTrue("first execute ok", ok1);

        // Second synchronous execute should also work (first completed)
        var ff2 = FlowFile.Create("test2"u8, new());
        var ok2 = fab.Execute(ff2, "tag");
        AssertTrue("second execute ok (first finished)", ok2);

        var stats = fab.GetProcessorStats();
        AssertTrue("tag processed 2", stats["tag"]["processed"] == 2);
    }

    // --- Failure scenarios and edge cases ---

    class ThrowingProcessor : IProcessor
    {
        public ProcessorResult Process(FlowFile ff) => throw new ArgumentException("test exception");
    }

    class CustomRouteProcessor : IProcessor
    {
        public ProcessorResult Process(FlowFile ff) => RoutedResult.Rent("custom_route", ff);
    }

    static ZincFlow.Fabric.Fabric BuildFabricWithCustom(
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
        AssertTrue("exception: thrower errors=1", stats1["thrower"]["errors"] == 1);
        AssertTrue("exception: error-handler received flowfile", stats1["error-handler"]["processed"] == 1);
        AssertTrue("exception: sink not invoked", stats1["sink"]["processed"] == 0);

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
        AssertTrue("exception no handler: thrower errors=1", stats2["thrower"]["errors"] == 1);
        AssertTrue("exception no handler: sink not invoked", stats2["sink"]["processed"] == 0);
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
                        ["config"] = new Dictionary<string, object?> { ["schema_name"] = "data" },
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
        AssertTrue("fan-out mixed: source processed=1", stats["source"]["processed"] == 1);
        AssertTrue("fan-out mixed: good-branch processed=1", stats["good-branch"]["processed"] == 1);
        AssertTrue("fan-out mixed: bad-branch processed=1", stats["bad-branch"]["processed"] == 1);
        AssertTrue("fan-out mixed: error-handler processed=1", stats["error-handler"]["processed"] == 1);
        AssertTrue("fan-out mixed: never-reached processed=0", stats["never-reached"]["processed"] == 0);
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
                        ["config"] = new Dictionary<string, object?> { ["schema_name"] = "data" },
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
        AssertTrue("partial failure: A processed=1", stats["A"]["processed"] == 1);
        AssertTrue("partial failure: B processed=1", stats["B"]["processed"] == 1);
        AssertTrue("partial failure: C processed=1", stats["C"]["processed"] == 1);
        AssertTrue("partial failure: D processed=0", stats["D"]["processed"] == 0);
        AssertTrue("partial failure: E processed=0", stats["E"]["processed"] == 0);
        AssertTrue("partial failure: F processed=1", stats["F"]["processed"] == 1);
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
        AssertTrue("routed no conn: router processed=1", stats["router"]["processed"] == 1);
        AssertTrue("routed no conn: router errors=0", stats["router"]["errors"] == 0);
        AssertTrue("routed no conn: sink not invoked", stats["sink"]["processed"] == 0);
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
                        ["config"] = new Dictionary<string, object?> { ["schema_name"] = "data" },
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
        AssertTrue("dual sink: parser processed=1 (valid)", stats1["parser"]["processed"] == 1);
        AssertTrue("dual sink: json-sink processed=1", stats1["json-sink"]["processed"] == 1);
        AssertTrue("dual sink: error-sink processed=0", stats1["error-sink"]["processed"] == 0);

        // Send invalid data -> error-sink gets it
        var ff2 = FlowFile.Create("this is not json"u8, new());
        var ok2 = fab.Execute(ff2, "parser");
        AssertTrue("dual sink: invalid data executes", ok2);
        var stats2 = fab.GetProcessorStats();
        AssertTrue("dual sink: parser processed=2 (both)", stats2["parser"]["processed"] == 2);
        AssertTrue("dual sink: json-sink still=1", stats2["json-sink"]["processed"] == 1);
        AssertTrue("dual sink: error-sink processed=1", stats2["error-sink"]["processed"] == 1);
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
        AssertTrue("split single line: splitter processed=1", stats1["splitter"]["processed"] == 1);
        AssertTrue("split single line: sink processed=1 (pass-through)", stats1["sink"]["processed"] == 1);

        // Case 2: Empty content -> SplitText splits "" by \n -> [""] -> length 1 -> SingleResult pass-through
        var ctx2 = TestContext();
        var fab2 = BuildFabric(config, ctx2);
        var ff2 = FlowFile.Create(""u8, new());
        var ok2 = fab2.Execute(ff2, "splitter");
        AssertTrue("split empty: execute ok", ok2);
        var stats2 = fab2.GetProcessorStats();
        AssertTrue("split empty: splitter processed=1", stats2["splitter"]["processed"] == 1);
        AssertTrue("split empty: sink processed=1 (pass-through)", stats2["sink"]["processed"] == 1);
    }

    // --- PollingSource tests ---

    /// <summary>
    /// Test sink that captures FlowFiles for content verification.
    /// Snapshots attributes (via TryGetValue) and content bytes before returning.
    /// </summary>
    class CaptureSink : IProcessor
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
            List<Dictionary<string, object?>>? records = null;
            if (ff.Content is RecordContent rc)
                records = rc.Records;
            Captured.Add(new CapturedFlowFile(attrs, data, records));
            return SingleResult.Rent(ff);
        }
    }

    record CapturedFlowFile(
        Dictionary<string, string> Attrs,
        byte[]? Data,
        List<Dictionary<string, object?>>? Records
    )
    {
        public string Text => Data is not null ? Encoding.UTF8.GetString(Data) : "";
    }

    class TestPoller : PollingSource
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

    // --- E2E content-verifying scenarios ---

    /// <summary>
    /// Full ETL pipeline: CSV → parse → transform (uppercase names) → enrich (add env) → CSV output.
    /// Verifies actual output content, not just processor stats.
    /// </summary>
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

    /// <summary>
    /// Fan-out to 3 branches, verify each branch gets correct attributes.
    /// </summary>
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

    /// <summary>
    /// Cascading failure: A → B (fails) → failure:C (also fails) → failure:D (catches).
    /// Verifies failure routing chains work correctly.
    /// </summary>
    static void TestE2ECascadingFailure()
    {
        Console.WriteLine("--- E2E: Cascading failure routing ---");
        var ctx = TestContext();
        var logProv = new LoggingProvider(); logProv.Enable();
        ctx.AddProvider(logProv);

        // A (UpdateAttribute) → B (ConvertJSONToRecord, will fail on non-JSON)
        // B failure → C (also ConvertJSONToRecord, will also fail)
        // C failure → D (UpdateAttribute, catches the error)
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

    /// <summary>
    /// Hot reload changes processor config, verify output data changes.
    /// Before reload: UpdateAttribute sets env=dev. After reload: env=prod.
    /// </summary>
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

    /// <summary>
    /// Verify attributes accumulate through a multi-hop pipeline.
    /// Each processor adds an attribute; final sink should have all of them.
    /// </summary>
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
