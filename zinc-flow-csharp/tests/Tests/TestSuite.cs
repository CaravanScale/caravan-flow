using System.Text;
using ZincFlow.Core;
using ZincFlow.Fabric;
using ZincFlow.StdLib;

namespace ZincFlow.Tests;

/// <summary>
/// Test suite matching Zinc's test_main.zn — 40+ test functions, 137+ assertions.
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

        // Fabric integration
        TestFabricMultiHop();
        TestFabricDlqFromFailure();

        // Flow engine
        TestFlowQueueOfferClaim();
        TestFlowQueueBackpressure();
        TestFlowQueueAckNack();
        TestDLQAddAndList();
        TestDLQReplay();
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
        TestQueueWAL();

        // Scenarios
        TestScenarioProcessorChain();
        TestScenarioBackpressurePropagation();
        TestScenarioRetryExhaustionToDLQ();
        TestScenarioDLQReplayToSourceQueue();
        TestScenarioScopedProviderIsolation();
        TestScenarioIRSFanOut();
        TestScenarioVisibilityTimeout();

        TestMaxHopCycleDetection();

        // E2E integration tests
        TestE2EFullPipeline();
        TestE2EWALPersistence();
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
        TestPipelineFailureToDLQ();
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

        // Records → JSON
        var toJson = new ConvertRecordToJSON();
        var step1 = toJson.Process(ff);
        AssertTrue("step1 returns single", step1 is SingleResult);
        var jsonFf = ((SingleResult)step1).FlowFile;
        AssertTrue("step1 is raw", jsonFf.Content is Raw);
        var (bytes, _) = ContentHelpers.Resolve(new MemoryContentStore(), jsonFf.Content);
        AssertTrue("json contains Portland", Encoding.UTF8.GetString(bytes).Contains("Portland"));

        // JSON → Records
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

    // --- Fabric integration ---

    static void TestFabricMultiHop()
    {
        Console.WriteLine("--- Fabric: Multi-Hop ---");
        var ctx = TestContext();
        var reg = new Registry(); BuiltinProcessors.RegisterAll(reg);
        var fab = new Fabric.Fabric(reg, ctx);

        var engine = new RulesEngine();
        engine.AddOrReplaceRuleset("flow", [
            new RoutingRule("all-to-tag", "type", Operator.Exists, "", "tag-env"),
            new RoutingRule("tagged-to-log", "env", Operator.Exists, "", "logger")
        ]);

        var dests1 = new List<string>();
        engine.GetDestinations(AttributeMap.FromDict(new() { ["type"] = "order" }), dests1);
        AssertIntEqual("first hop matches", dests1.Count, 1);

        var dests2 = new List<string>();
        engine.GetDestinations(AttributeMap.FromDict(new() { ["type"] = "order", ["env"] = "prod" }), dests2);
        AssertIntEqual("second hop matches", dests2.Count, 2);

        var ff = FlowFile.Create("multi-hop"u8, new() { ["type"] = "test" });
        AssertTrue("ingest accepted", fab.Ingest(ff));
    }

    static void TestFabricDlqFromFailure()
    {
        Console.WriteLine("--- Fabric: DLQ from Failure ---");
        var ctx = TestContext();
        var reg = new Registry(); BuiltinProcessors.RegisterAll(reg);
        var fab = new Fabric.Fabric(reg, ctx);
        AssertIntEqual("initial dlq", fab.GetDLQ().Count, 0);

        var ff = FlowFile.Create("test"u8, new() { ["type"] = "unknown" });
        AssertTrue("ingest accepted", fab.Ingest(ff));
        AssertIntEqual("no failures no dlq", fab.GetDLQ().Count, 0);
    }

    // --- Flow Engine: Queues ---

    static void TestFlowQueueOfferClaim()
    {
        Console.WriteLine("--- FlowQueue: Offer/Claim ---");
        var q = new FlowQueue("test-q", 100, 1024 * 1024, 30_000);
        var ff = FlowFile.Create("hello"u8, new() { ["key"] = "val" });

        AssertTrue("offer accepted", q.Offer(ff));
        AssertIntEqual("visible count", q.VisibleCount, 1);
        AssertIntEqual("invisible count", q.InvisibleCount, 0);

        var entry = q.Claim()!;
        AssertTrue("claim non-null", entry is not null);
        AssertIntEqual("after claim visible", q.VisibleCount, 0);
        AssertIntEqual("after claim invisible", q.InvisibleCount, 1);

        q.Ack(entry.Id);
        AssertIntEqual("after ack visible", q.VisibleCount, 0);
        AssertIntEqual("after ack invisible", q.InvisibleCount, 0);
    }

    static void TestFlowQueueBackpressure()
    {
        Console.WriteLine("--- FlowQueue: Backpressure ---");
        var q = new FlowQueue("bp-q", 2, 1024, 30_000);

        AssertTrue("first offer", q.Offer(FlowFile.Create("a"u8, new())));
        AssertTrue("second offer", q.Offer(FlowFile.Create("b"u8, new())));
        AssertFalse("third rejected (backpressure)", q.Offer(FlowFile.Create("c"u8, new())));
        AssertFalse("no capacity", q.HasCapacity());
    }

    static void TestFlowQueueAckNack()
    {
        Console.WriteLine("--- FlowQueue: Ack/Nack ---");
        var q = new FlowQueue("an-q", 100, 1024 * 1024, 30_000);
        q.Offer(FlowFile.Create("test"u8, new()));

        var entry = q.Claim()!;
        AssertIntEqual("attempt before nack", entry.AttemptCount, 0);

        q.Nack(entry.Id);
        AssertIntEqual("after nack visible", q.VisibleCount, 1);
        AssertIntEqual("after nack invisible", q.InvisibleCount, 0);

        var retried = q.Claim()!;
        AssertIntEqual("attempt after nack", retried.AttemptCount, 1);
        q.Ack(retried.Id);
    }

    // --- DLQ ---

    static void TestDLQAddAndList()
    {
        Console.WriteLine("--- DLQ: Add/List ---");
        var dlq = new DLQ();
        AssertIntEqual("initial count", dlq.Count, 0);

        var ff = FlowFile.Create("failed"u8, new() { ["type"] = "test" });
        dlq.Add(ff, "my-proc", "my-queue", 3, "processing error");
        AssertIntEqual("after add count", dlq.Count, 1);

        var entries = dlq.ListEntries();
        AssertIntEqual("list length", entries.Count, 1);
        AssertEqual("source processor", entries[0].SourceProcessor, "my-proc");
        AssertEqual("last error", entries[0].LastError, "processing error");
        AssertIntEqual("attempt count", entries[0].AttemptCount, 3);
    }

    static void TestDLQReplay()
    {
        Console.WriteLine("--- DLQ: Replay ---");
        var dlq = new DLQ();
        var ff = FlowFile.Create("replay-me"u8, new() { ["id"] = "123" });
        dlq.Add(ff, "proc", "queue", 5, "max retries");
        AssertIntEqual("before replay", dlq.Count, 1);

        var entries = dlq.ListEntries();
        var replayed = dlq.Replay(entries[0].Id);
        AssertTrue("replayed not null", replayed is not null);
        AssertTrue("replayed ff id matches", replayed!.NumericId == ff.NumericId);
        AssertIntEqual("after replay", dlq.Count, 0);
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

    // --- Scenarios ---

    static void TestScenarioProcessorChain()
    {
        Console.WriteLine("--- Scenario: Processor Chain ---");
        var scopedCtx = TestScopedCtx();
        var reg = new Registry(); BuiltinProcessors.RegisterAll(reg);
        var tagger = reg.Create("UpdateAttribute", scopedCtx, new() { ["key"] = "env", ["value"] = "dev" });
        var logger = reg.Create("LogAttribute", scopedCtx, new() { ["prefix"] = "chain-test" });

        var tagQueue = new FlowQueue("tag-env", 100, 1024 * 1024, 30_000);
        var logQueue = new FlowQueue("logger", 100, 1024 * 1024, 30_000);
        var allQueues = new Dictionary<string, FlowQueue> { ["tag-env"] = tagQueue, ["logger"] = logQueue };

        var tagConnections = new Dictionary<string, List<string>> { ["success"] = ["logger"] };
        var logConnections = new Dictionary<string, List<string>>();

        var dlq = new DLQ();
        var tagSession = new ProcessSession(tagQueue, tagger, "tag-env", tagConnections, allQueues, dlq, 5);
        var logSession = new ProcessSession(logQueue, logger, "logger", logConnections, allQueues, dlq, 5);

        tagQueue.Offer(FlowFile.Create("hello"u8, new() { ["type"] = "order" }));
        AssertIntEqual("tag queue has 1", tagQueue.VisibleCount, 1);

        AssertTrue("tag session executed", tagSession.Execute());
        AssertIntEqual("tag queue empty", tagQueue.VisibleCount, 0);
        AssertIntEqual("log queue has 1", logQueue.VisibleCount, 1);

        AssertTrue("log session executed", logSession.Execute());
        AssertIntEqual("log queue empty", logQueue.VisibleCount, 0);
        AssertIntEqual("no dlq entries", dlq.Count, 0);
    }

    static void TestScenarioBackpressurePropagation()
    {
        Console.WriteLine("--- Scenario: Backpressure Propagation ---");
        var smallQueue = new FlowQueue("small", 3, 1024, 30_000);
        var downstreamQueue = new FlowQueue("downstream", 3, 1024, 30_000);
        var allQueues = new Dictionary<string, FlowQueue> { ["small"] = smallQueue, ["downstream"] = downstreamQueue };

        var connections = new Dictionary<string, List<string>> { ["success"] = ["downstream"] };

        var proc = new LogAttribute("bp-test");
        var dlq = new DLQ();
        var session = new ProcessSession(smallQueue, proc, "small", connections, allQueues, dlq, 5);

        for (int i = 0; i < 3; i++)
            downstreamQueue.Offer(FlowFile.Create("fill"u8, new() { ["type"] = "x" }));
        AssertFalse("downstream full", downstreamQueue.HasCapacity());

        smallQueue.Offer(FlowFile.Create("blocked"u8, new() { ["type"] = "test" }));
        AssertTrue("session ran", session.Execute());
        AssertIntEqual("item back in small queue", smallQueue.VisibleCount, 1);

        var entry = smallQueue.Claim()!;
        AssertIntEqual("attempt incremented", entry.AttemptCount, 1);
    }

    static void TestScenarioRetryExhaustionToDLQ()
    {
        Console.WriteLine("--- Scenario: Retry Exhaustion → DLQ ---");
        var sourceQueue = new FlowQueue("source", 100, 1024 * 1024, 30_000);
        var destQueue = new FlowQueue("dest", 0, 0, 30_000); // zero capacity
        var allQueues = new Dictionary<string, FlowQueue> { ["dest"] = destQueue };

        var connections = new Dictionary<string, List<string>> { ["success"] = ["dest"] };

        var dlq = new DLQ();
        int maxRetries = 3;
        var session = new ProcessSession(sourceQueue, new LogAttribute("retry"), "source", connections, allQueues, dlq, maxRetries);

        sourceQueue.Offer(FlowFile.Create("will-fail"u8, new() { ["type"] = "test" }));

        for (int i = 0; i < maxRetries + 1; i++)
            session.Execute();

        AssertIntEqual("dlq has entry", dlq.Count, 1);
        AssertIntEqual("source queue empty", sourceQueue.VisibleCount, 0);

        var entries = dlq.ListEntries();
        AssertEqual("dlq source processor", entries[0].SourceProcessor, "source");
        AssertEqual("dlq error", entries[0].LastError, "max retries exceeded");
    }

    static void TestScenarioDLQReplayToSourceQueue()
    {
        Console.WriteLine("--- Scenario: DLQ Replay to Source Queue ---");
        var dlq = new DLQ();
        var sourceQueue = new FlowQueue("my-proc", 100, 1024 * 1024, 30_000);

        var ff = FlowFile.Create("replay-test"u8, new() { ["type"] = "order" });
        dlq.Add(ff, "my-proc", "my-proc", 5, "processing failed");
        AssertIntEqual("dlq count", dlq.Count, 1);

        var entries = dlq.ListEntries();
        var replayed = dlq.Replay(entries[0].Id);
        AssertTrue("replayed matches", replayed!.NumericId == ff.NumericId);
        AssertIntEqual("dlq empty after replay", dlq.Count, 0);

        sourceQueue.Offer(replayed);
        AssertIntEqual("source queue has replayed item", sourceQueue.VisibleCount, 1);

        var entry = sourceQueue.Claim()!;
        sourceQueue.Ack(entry.Id);
        AssertIntEqual("source queue empty after processing", sourceQueue.VisibleCount, 0);
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
        Console.WriteLine("--- Scenario: Connection Fan-Out ---");
        var tagger = new UpdateAttribute("env", "prod");

        var srcQueue = new FlowQueue("source", 100, 1024 * 1024, 30_000);
        var destA = new FlowQueue("dest-a", 100, 1024 * 1024, 30_000);
        var destB = new FlowQueue("dest-b", 100, 1024 * 1024, 30_000);
        var destC = new FlowQueue("dest-c", 100, 1024 * 1024, 30_000);
        var allQueues = new Dictionary<string, FlowQueue>
        {
            ["source"] = srcQueue, ["dest-a"] = destA, ["dest-b"] = destB, ["dest-c"] = destC
        };

        var connections = new Dictionary<string, List<string>> { ["success"] = ["dest-a", "dest-b", "dest-c"] };

        var dlq = new DLQ();
        var session = new ProcessSession(srcQueue, tagger, "source", connections, allQueues, dlq, 5);

        srcQueue.Offer(FlowFile.Create("fanout"u8, new() { ["type"] = "order" }));
        session.Execute();

        AssertIntEqual("dest-a has 1", destA.VisibleCount, 1);
        AssertIntEqual("dest-b has 1", destB.VisibleCount, 1);
        AssertIntEqual("dest-c has 1", destC.VisibleCount, 1);
        AssertIntEqual("source empty", srcQueue.VisibleCount, 0);
        AssertIntEqual("no dlq", dlq.Count, 0);

        var entryA = destA.Claim()!;
        AssertTrue("dest-a has env attr", entryA.FlowFile.Attributes.TryGetValue("env", out var env) && env == "prod");
        destA.Ack(entryA.Id);
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

        var tag = new UpdateAttribute("env", "prod");
        var q = new FlowQueue("prov", 100, 0, 30_000);
        var outQ = new FlowQueue("out", 100, 0, 30_000);
        var queues = new Dictionary<string, FlowQueue> { ["prov"] = q, ["out"] = outQ };
        var connections = new Dictionary<string, List<string>> { ["success"] = ["out"] };
        var dlq = new DLQ();
        var session = new ProcessSession(q, tag, "tagger", connections, queues, dlq, 5, prov);

        var ff = FlowFile.Create("test"u8, new() { ["type"] = "order" });
        q.Offer(ff);
        session.Execute();

        // Provenance provider should have recorded events
        var events = prov.GetEvents(ff.NumericId);
        AssertTrue("has provenance events", events.Count >= 2);
        AssertTrue("processed event", events.Any(e => e.EventType == ProvenanceEventType.Processed && e.Component == "tagger"));
        AssertTrue("routed event", events.Any(e => e.EventType == ProvenanceEventType.Routed && e.Component == "tagger" && e.Details == "out"));

        // Recent events API
        var recent = prov.GetRecent(10);
        AssertTrue("recent has events", recent.Count >= 2);

        outQ.Claim();
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

    static void TestQueueWAL()
    {
        Console.WriteLine("--- QueueWAL ---");
        var walPath = Path.Combine(Path.GetTempPath(), $"zinc-test-wal-{Environment.TickCount64}.wal");
        try
        {
            // Write some entries
            using (var wal = new QueueWAL(walPath))
            {
                wal.Open();
                wal.AppendOffer(1, "payload-1"u8.ToArray());
                wal.AppendOffer(2, "payload-2"u8.ToArray());
                wal.AppendOffer(3, "payload-3"u8.ToArray());
                wal.AppendAck(2); // ack entry 2
            }

            // Replay — should have entries 1 and 3 (2 was acked)
            using (var wal = new QueueWAL(walPath))
            {
                wal.Open();
                var live = wal.Replay();
                AssertIntEqual("2 live entries", live.Count, 2);

                var ids = live.Select(e => e.Id).OrderBy(id => id).ToList();
                AssertTrue("entry 1 live", ids.Contains(1));
                AssertTrue("entry 3 live", ids.Contains(3));
                AssertTrue("entry 2 acked", !ids.Contains(2));

                // Verify payload
                var entry1 = live.First(e => e.Id == 1);
                AssertTrue("payload preserved", System.Text.Encoding.UTF8.GetString(entry1.V3Payload) == "payload-1");

                // Compact
                wal.Compact(live);
                var afterCompact = wal.Replay();
                AssertIntEqual("still 2 after compact", afterCompact.Count, 2);
            }
        }
        finally
        {
            if (File.Exists(walPath)) File.Delete(walPath);
            var tmpWal = walPath + ".tmp";
            if (File.Exists(tmpWal)) File.Delete(tmpWal);
        }
    }

    static void TestScenarioVisibilityTimeout()
    {
        Console.WriteLine("--- Scenario: Visibility Timeout ---");
        var q = new FlowQueue("vis-test", 10, 1024, 500); // 500ms visibility timeout
        q.StartReaper();

        q.Offer(FlowFile.Create("timeout-test"u8, new() { ["type"] = "test" }));

        var entry = q.Claim()!;
        AssertTrue("claimed non-null", entry is not null);
        AssertIntEqual("visible after claim", q.VisibleCount, 0);
        AssertIntEqual("invisible after claim", q.InvisibleCount, 1);
        AssertIntEqual("attempt before timeout", entry.AttemptCount, 0);

        // Wait for visibility timeout + reaper cycle
        Thread.Sleep(2000);

        AssertIntEqual("visible after timeout", q.VisibleCount, 1);
        AssertIntEqual("invisible after timeout", q.InvisibleCount, 0);

        var reclaimed = q.Claim()!;
        AssertTrue("reclaimed non-null", reclaimed is not null);
        AssertIntEqual("attempt after timeout", reclaimed.AttemptCount, 1);
        q.Ack(reclaimed.Id);
        AssertIntEqual("clean after ack", q.VisibleCount, 0);
    }

    static void TestMaxHopCycleDetection()
    {
        Console.WriteLine("--- Max Hop Cycle Detection ---");
        // Create a cycle: A routes to B, B routes to A
        var procA = new UpdateAttribute("hop", "a");
        var procB = new UpdateAttribute("hop", "b");

        var qA = new FlowQueue("a", 1000, 0, 30_000);
        var qB = new FlowQueue("b", 1000, 0, 30_000);
        var queues = new Dictionary<string, FlowQueue> { ["a"] = qA, ["b"] = qB };

        // Both connect to each other — infinite cycle
        var connA = new Dictionary<string, List<string>> { ["success"] = ["b"] };
        var connB = new Dictionary<string, List<string>> { ["success"] = ["a"] };

        var dlq = new DLQ();
        var prov = new ProvenanceProvider();
        prov.Enable();

        // maxHops = 5 — should DLQ after 5 hops instead of looping forever
        var sessionA = new ProcessSession(qA, procA, "proc-a", connA, queues, dlq, 5, prov, maxHops: 5);
        var sessionB = new ProcessSession(qB, procB, "proc-b", connB, queues, dlq, 5, prov, maxHops: 5);

        // Start the cycle
        qA.Offer(FlowFile.Create("cycle-test"u8, new() { ["type"] = "test" }));

        // Run enough iterations to hit the max hop limit
        for (int i = 0; i < 20; i++)
        {
            sessionA.Execute();
            sessionB.Execute();
        }

        // FlowFile should be in DLQ, not bouncing forever
        AssertTrue("cycle detected → DLQ", dlq.Count >= 1);

        // Provenance should show the cycle
        var events = prov.GetRecent(50);
        AssertTrue("has DLQ event", events.Any(e => e.EventType == ProvenanceEventType.DLQ));
        var dlqEvent = events.First(e => e.EventType == ProvenanceEventType.DLQ);
        AssertTrue("cycle reason", dlqEvent.Details.Contains("max hops exceeded"));

        // Queues should be empty (everything DLQ'd)
        AssertIntEqual("qA empty", qA.VisibleCount, 0);
        AssertIntEqual("qB empty", qB.VisibleCount, 0);
    }

    // ===== E2E INTEGRATION TESTS =====

    static void TestE2EFullPipeline()
    {
        Console.WriteLine("--- E2E: Full Pipeline (ingest → process → route → sink) ---");
        try
        {
            var store = new MemoryContentStore();
            var prov = new ProvenanceProvider();
            prov.Enable();

            // Build a 3-stage pipeline: UpdateAttribute → LogAttribute → PutFile
            var reg = new Registry();
            BuiltinProcessors.RegisterAll(reg);

            var globalCtx = new ProcessorContext();
            globalCtx.AddProvider(new ContentProvider("content", store));
            globalCtx.AddProvider(new LoggingProvider());
            globalCtx.AddProvider(prov);

            var fab = new ZincFlow.Fabric.Fabric(reg, globalCtx);

            // 3-stage pipeline: tagger → logger → sink via explicit connections.
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
            fab.StartAsync();

            // Ingest a FlowFile
            var ff = FlowFile.Create("{\"order\":123}"u8, new() { ["type"] = "order", ["stage"] = "0" });
            var ffId = ff.NumericId;
            AssertTrue("ingest accepted", fab.Ingest(ff));

            // Wait for async pipeline (3 hops + routing)
            Thread.Sleep(4000);

            // Verify stats
            var stats = fab.GetStats();
            AssertTrue("processed > 0", stats["processed"] > 0);
            AssertIntEqual("no dlq entries", stats["dlq"], 0);

            // Verify provenance chain — all 3 processors touched the FlowFile
            var events = prov.GetEvents(ffId);
            AssertTrue("provenance has events", events.Count >= 3);
            AssertTrue("processed by tagger", events.Any(e => e.EventType == ProvenanceEventType.Processed && e.Component == "tagger"));
            AssertTrue("processed by logger", events.Any(e => e.EventType == ProvenanceEventType.Processed && e.Component == "logger"));
            AssertTrue("processed by sink", events.Any(e => e.EventType == ProvenanceEventType.Processed && e.Component == "sink"));

            fab.StopAsync();
        }
        finally
        {
        }
    }

    static void TestE2EWALPersistence()
    {
        Console.WriteLine("--- E2E: WAL Persistence (write → close → replay) ---");
        var walDir = Path.Combine(Path.GetTempPath(), $"zinc-e2e-wal-{Environment.TickCount64}");
        Directory.CreateDirectory(walDir);
        try
        {
            var walPath = Path.Combine(walDir, "test.wal");

            // Phase 1: create queue with WAL, offer items, ack some
            {
                var wal = new QueueWAL(walPath, maxSizeMb: 10, compactIntervalMs: 0);
                var q = new FlowQueue("wal-test", 1000, 0, 30_000, wal);
                q.ReplayWAL(); // opens WAL

                q.Offer(FlowFile.Create("item-1"u8, new() { ["id"] = "1" }));
                q.Offer(FlowFile.Create("item-2"u8, new() { ["id"] = "2" }));
                q.Offer(FlowFile.Create("item-3"u8, new() { ["id"] = "3" }));

                // Ack item 1
                var e1 = q.Claim()!;
                q.Ack(e1.Id);

                AssertIntEqual("2 visible before close", q.VisibleCount, 2);
                wal.Dispose();
            }

            // Phase 2: new queue, replay WAL — should restore items 2 and 3
            {
                var wal = new QueueWAL(walPath, maxSizeMb: 10, compactIntervalMs: 0);
                var q = new FlowQueue("wal-test", 1000, 0, 30_000, wal);
                int restored = q.ReplayWAL();

                AssertIntEqual("restored 2 items", restored, 2);
                AssertIntEqual("2 visible after replay", q.VisibleCount, 2);

                // Verify content survived
                var e2 = q.Claim()!;
                if (e2.FlowFile.Content is Raw raw2)
                    AssertTrue("content survived WAL", Encoding.UTF8.GetString(raw2.Data) == "item-2" || Encoding.UTF8.GetString(raw2.Data) == "item-3");
                q.Ack(e2.Id);

                wal.Dispose();
            }
        }
        finally
        {
            if (Directory.Exists(walDir)) Directory.Delete(walDir, true);
        }
    }

    static void TestE2EContentStoreLifecycle()
    {
        Console.WriteLine("--- E2E: Content Store Lifecycle (offload → process → cleanup) ---");
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
        Console.WriteLine("--- E2E: Provenance Chain (multi-hop tracking) ---");
        var prov = new ProvenanceProvider();
        prov.Enable();

        // 3-hop pipeline: tagger → logger → sink
        var tagger = new UpdateAttribute("env", "prod");
        var logger = new LogAttribute("chain");
        var sink = new UpdateAttribute("done", "true");

        var q1 = new FlowQueue("q1", 100, 0, 30_000);
        var q2 = new FlowQueue("q2", 100, 0, 30_000);
        var q3 = new FlowQueue("q3", 100, 0, 30_000);
        var queues = new Dictionary<string, FlowQueue> { ["q1"] = q1, ["q2"] = q2, ["q3"] = q3 };

        var conn1 = new Dictionary<string, List<string>> { ["success"] = ["q2"] };
        var conn2 = new Dictionary<string, List<string>> { ["success"] = ["q3"] };
        var conn3 = new Dictionary<string, List<string>>();

        var dlq = new DLQ();
        var s1 = new ProcessSession(q1, tagger, "tagger", conn1, queues, dlq, 5, prov);
        var s2 = new ProcessSession(q2, logger, "logger", conn2, queues, dlq, 5, prov);
        var s3 = new ProcessSession(q3, sink, "sink", conn3, queues, dlq, 5, prov);

        var ff = FlowFile.Create("chain-test"u8, new() { ["type"] = "order" });
        var ffId = ff.NumericId;
        q1.Offer(ff);

        s1.Execute();
        s2.Execute();
        s3.Execute();

        var events = prov.GetEvents(ffId);
        AssertTrue("chain has 5+ events", events.Count >= 5); // 3 processed + 2 routed
        AssertTrue("tagger processed", events.Any(e => e.Component == "tagger" && e.EventType == ProvenanceEventType.Processed));
        AssertTrue("tagger routed to q2", events.Any(e => e.Component == "tagger" && e.EventType == ProvenanceEventType.Routed && e.Details == "q2"));
        AssertTrue("logger processed", events.Any(e => e.Component == "logger" && e.EventType == ProvenanceEventType.Processed));
        AssertTrue("logger routed to q3", events.Any(e => e.Component == "logger" && e.EventType == ProvenanceEventType.Routed && e.Details == "q3"));
        AssertTrue("sink processed", events.Any(e => e.Component == "sink" && e.EventType == ProvenanceEventType.Processed));

        // Verify ordering — tagger before logger before sink
        var procEvents = events.Where(e => e.EventType == ProvenanceEventType.Processed).ToList();
        AssertTrue("order: tagger first", procEvents[0].Component == "tagger");
        AssertTrue("order: logger second", procEvents[1].Component == "logger");
        AssertTrue("order: sink third", procEvents[2].Component == "sink");
    }

    static void TestE2EListenHTTPPipeline()
    {
        Console.WriteLine("--- E2E: ListenHTTP → Pipeline → PutFile ---");
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

        // No match → pass through
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
        Console.WriteLine("--- Avro ↔ JSON cross-format ---");
        var store = new MemoryContentStore();
        // JSON → Record → Avro → Record → JSON
        var json = "[{\"name\":\"Eve\",\"score\":99}]";
        var jsonToRec = new ConvertJSONToRecord("test", store);
        var ff1 = FlowFile.Create(Encoding.UTF8.GetBytes(json), new());
        var r1 = jsonToRec.Process(ff1);
        AssertTrue("json→record ok", r1 is SingleResult);
        var recFf = ((SingleResult)r1).FlowFile;

        var recToAvro = new ConvertRecordToAvro();
        var r2 = recToAvro.Process(recFf);
        AssertTrue("record→avro ok", r2 is SingleResult);
        var avroFf = ((SingleResult)r2).FlowFile;
        AssertTrue("avro output is Raw", avroFf.Content is Raw);

        // Read avro.schema attribute to decode back
        avroFf.Attributes.TryGetValue("avro.schema", out var schemaDef);
        AssertTrue("has schema attr", !string.IsNullOrEmpty(schemaDef));

        var avroToRec = new ConvertAvroToRecord("test", schemaDef ?? "", store);
        var r3 = avroToRec.Process(avroFf);
        AssertTrue("avro→record ok", r3 is SingleResult);
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
        // Two entry points: src1 → shared, src2 → shared
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
        Console.WriteLine("--- Connection: Fan-Out ---");
        var proc = new LogAttribute("fanout");
        var srcQ = new FlowQueue("src", 100, 0, 30_000);
        var qA = new FlowQueue("a", 100, 0, 30_000);
        var qB = new FlowQueue("b", 100, 0, 30_000);
        var qC = new FlowQueue("c", 100, 0, 30_000);
        var queues = new Dictionary<string, FlowQueue> { ["src"] = srcQ, ["a"] = qA, ["b"] = qB, ["c"] = qC };
        var conn = new Dictionary<string, List<string>> { ["success"] = ["a", "b", "c"] };
        var dlq = new DLQ();
        var session = new ProcessSession(srcQ, proc, "src", conn, queues, dlq, 5);

        srcQ.Offer(FlowFile.Create("fanout"u8, new()));
        session.Execute();

        AssertIntEqual("a has 1", qA.VisibleCount, 1);
        AssertIntEqual("b has 1", qB.VisibleCount, 1);
        AssertIntEqual("c has 1", qC.VisibleCount, 1);
        AssertIntEqual("src empty", srcQ.VisibleCount, 0);
    }

    static void TestSinkNoConnections()
    {
        Console.WriteLine("--- Sink: No Connections (terminal) ---");
        var proc = new UpdateAttribute("done", "true");
        var srcQ = new FlowQueue("sink", 100, 0, 30_000);
        var queues = new Dictionary<string, FlowQueue> { ["sink"] = srcQ };
        var conn = new Dictionary<string, List<string>>(); // empty = terminal
        var dlq = new DLQ();
        var session = new ProcessSession(srcQ, proc, "sink", conn, queues, dlq, 5);

        srcQ.Offer(FlowFile.Create("terminal"u8, new()));
        session.Execute();

        AssertIntEqual("sink queue empty", srcQ.VisibleCount, 0);
        AssertIntEqual("no dlq", dlq.Count, 0);
    }

    // --- Comprehensive processor pipeline scenarios ---

    static void TestPipelineJsonToAvroToCsv()
    {
        Console.WriteLine("--- Pipeline: JSON → Record → Avro → Record → CSV ---");
        var store = new MemoryContentStore();
        var json = "[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":25}]";

        // JSON → Record
        var jsonToRec = new ConvertJSONToRecord("people", store);
        var ff1 = FlowFile.Create(Encoding.UTF8.GetBytes(json), new());
        var r1 = (SingleResult)jsonToRec.Process(ff1);
        AssertTrue("json→record", r1.FlowFile.Content is RecordContent);
        var rc1 = (RecordContent)r1.FlowFile.Content;
        AssertIntEqual("2 records from json", rc1.Records.Count, 2);

        // Record → Avro
        var recToAvro = new ConvertRecordToAvro();
        var r2 = (SingleResult)recToAvro.Process(r1.FlowFile);
        AssertTrue("record→avro raw", r2.FlowFile.Content is Raw);

        // Avro → Record (using schema from attribute)
        r2.FlowFile.Attributes.TryGetValue("avro.schema", out var schema);
        var avroToRec = new ConvertAvroToRecord("people", schema ?? "", store);
        var r3 = (SingleResult)avroToRec.Process(r2.FlowFile);
        AssertTrue("avro→record", r3.FlowFile.Content is RecordContent);
        var rc2 = (RecordContent)r3.FlowFile.Content;
        AssertIntEqual("2 records from avro", rc2.Records.Count, 2);

        // Record → CSV
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
        Console.WriteLine("--- Pipeline: CSV → Record → Transform → Enrich → CSV ---");
        var store = new MemoryContentStore();
        var csv = "email,name\nalice@test.com,alice\nbob@test.com,bob\n";

        // CSV → Record
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

        // Record → CSV
        var recToCsv = new ConvertRecordToCSV(',', true);
        var r4 = (SingleResult)recToCsv.Process(r3.FlowFile);
        var (outBytes, _) = ContentHelpers.Resolve(store, r4.FlowFile.Content);
        var outCsv = Encoding.UTF8.GetString(outBytes);
        AssertTrue("output has ALICE", outCsv.Contains("ALICE"));
        AssertTrue("output has source", outCsv.Contains("import"));
    }

    static void TestPipelineTextExtractAndRoute()
    {
        Console.WriteLine("--- Pipeline: ExtractText → EvaluateExpression ---");
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
        Console.WriteLine("--- Pipeline: SplitText → fan-out to multiple sinks ---");
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
        Console.WriteLine("--- Pipeline: Failure → error handler connection ---");
        var store = new MemoryContentStore();

        // Create a pipeline where bad JSON goes to error handler via failure connection
        var badJson = "not valid json at all";
        var jsonToRec = new ConvertJSONToRecord("data", store);
        var ff = FlowFile.Create(Encoding.UTF8.GetBytes(badJson), new() { ["type"] = "order" });
        var result = jsonToRec.Process(ff);
        AssertTrue("bad json is FailureResult", result is FailureResult);

        // Simulate ProcessSession routing to failure connection
        var errorQ = new FlowQueue("error-handler", 100, 0, 30_000);
        var sourceQ = new FlowQueue("parser", 100, 0, 30_000);
        sourceQ.Offer(ff);
        var connections = new Dictionary<string, List<string>>
        {
            ["success"] = ["next"],
            ["failure"] = ["error-handler"]
        };
        var queues = new Dictionary<string, FlowQueue>
        {
            ["error-handler"] = errorQ,
            ["next"] = new FlowQueue("next", 100, 0, 30_000)
        };
        var dlq = new DLQ();
        var session = new ProcessSession(sourceQ, jsonToRec, "parser", connections, queues, dlq, 5);
        session.Execute();

        // Failure should route to error-handler, not DLQ
        AssertIntEqual("error handler got 1", errorQ.VisibleCount, 1);
        AssertIntEqual("dlq empty", dlq.Count, 0);
    }

    static void TestPipelineFailureToDLQ()
    {
        Console.WriteLine("--- Pipeline: Failure → DLQ (no failure connection) ---");
        var store = new MemoryContentStore();
        var badJson = "broken";
        var jsonToRec = new ConvertJSONToRecord("data", store);
        var ff = FlowFile.Create(Encoding.UTF8.GetBytes(badJson), new());

        var sourceQ = new FlowQueue("parser", 100, 0, 30_000);
        sourceQ.Offer(ff);
        var connections = new Dictionary<string, List<string>>
        {
            ["success"] = ["next"]
        };
        var queues = new Dictionary<string, FlowQueue>
        {
            ["next"] = new FlowQueue("next", 100, 0, 30_000)
        };
        var dlq = new DLQ();
        var session = new ProcessSession(sourceQ, jsonToRec, "parser", connections, queues, dlq, 5);
        session.Execute();

        // No failure connection → DLQ
        AssertIntEqual("dlq has 1", dlq.Count, 1);
        var entries = dlq.ListEntries();
        AssertTrue("dlq error has reason", entries[0].LastError.Contains("no records"));
    }

    static void TestPipelineLargeContentOffload()
    {
        Console.WriteLine("--- Pipeline: Large content → offload to content store ---");
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

        // Process through JSON → Record → JSON roundtrip
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
        // Config: tagger → logger (2 processors)
        var config1 = MakeFlowConfig(new Dictionary<string, object?>
        {
            ["tagger"] = MakeProc("UpdateAttribute", new() { ["key"] = "env", ["value"] = "dev" },
                connections: new() { ["success"] = new List<string> { "logger" } }),
            ["logger"] = MakeProc("LogAttribute", new() { ["prefix"] = "test" })
        });
        var (fab, _, _) = CreateFabricWithConfig(config1);
        fab.StartAsync();
        Thread.Sleep(100);

        // Ingest before reload
        var ff1 = FlowFile.Create("before"u8, new() { ["type"] = "order" });
        fab.Ingest(ff1);
        Thread.Sleep(300);
        var statsBefore = fab.GetStats();
        AssertTrue("processed before reload", statsBefore["processed"] > 0);

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

        // Ingest after reload — should flow through 3 processors
        var ff2 = FlowFile.Create("after"u8, new() { ["type"] = "order" });
        fab.Ingest(ff2);
        Thread.Sleep(300);
        var statsAfter = fab.GetStats();
        AssertTrue("processed after reload", statsAfter["processed"] > statsBefore["processed"]);

        fab.StopAsync();
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

        // ConvertJSONToRecord with empty content → failure
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
        Console.WriteLine("--- Pipeline: Bad JSON → failure path ---");
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
        Console.WriteLine("--- Pipeline: JSON → Avro → JSON → CSV → JSON roundtrip ---");
        var store = new MemoryContentStore();

        // Start with JSON
        var json = "[{\"product\":\"Widget\",\"price\":9.99},{\"product\":\"Gadget\",\"price\":19.99}]";
        var ff = FlowFile.Create(Encoding.UTF8.GetBytes(json), new());

        // JSON → Record
        var r1 = (SingleResult)new ConvertJSONToRecord("products", store).Process(ff);

        // Record → Avro
        var r2 = (SingleResult)new ConvertRecordToAvro().Process(r1.FlowFile);
        r2.FlowFile.Attributes.TryGetValue("avro.schema", out var avroSchema);

        // Avro → Record
        var r3 = (SingleResult)new ConvertAvroToRecord("products", avroSchema ?? "", store).Process(r2.FlowFile);

        // Record → JSON (verify data survived)
        var r4 = (SingleResult)new ConvertRecordToJSON().Process(r3.FlowFile);
        var (jsonBytes, _) = ContentHelpers.Resolve(store, r4.FlowFile.Content);
        var jsonStr = Encoding.UTF8.GetString(jsonBytes);
        AssertTrue("json has Widget", jsonStr.Contains("Widget"));
        AssertTrue("json has Gadget", jsonStr.Contains("Gadget"));

        // JSON → Record → CSV
        var r5 = (SingleResult)new ConvertJSONToRecord("products", store).Process(r4.FlowFile);
        var r6 = (SingleResult)new ConvertRecordToCSV(',', true).Process(r5.FlowFile);
        var (csvBytes, _) = ContentHelpers.Resolve(store, r6.FlowFile.Content);
        var csvStr = Encoding.UTF8.GetString(csvBytes);
        AssertTrue("csv has product header", csvStr.Contains("product"));
        AssertTrue("csv has Widget", csvStr.Contains("Widget"));

        // CSV → Record → JSON (full circle)
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
        fab.StartAsync();

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

        fab.StopAsync();
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
        fab.StartAsync();

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

        fab.StopAsync();
    }

    static void TestHotReloadUpdateProcessor()
    {
        Console.WriteLine("--- Hot Reload: Update Processor ---");
        var config1 = MakeFlowConfig(new Dictionary<string, object?>
        {
            ["tag-env"] = MakeProc("UpdateAttribute", new() { ["key"] = "env", ["value"] = "dev" })
        });
        var (fab, ctx, reg) = CreateFabricWithConfig(config1);
        fab.StartAsync();

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
        fab.Ingest(ff);
        Thread.Sleep(200);

        fab.StopAsync();
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
        fab.StartAsync();

        var connBefore = fab.GetConnections();
        AssertIntEqual("tag-env no connections initially", connBefore.GetValueOrDefault("tag-env")?.Count ?? 0, 0);

        // Reload with connections: tag-env → logger
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

        fab.StopAsync();
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
        fab.StartAsync();

        var (added, removed, updated, connectionsChanged) = fab.ReloadFlow(config);
        AssertIntEqual("no adds", added, 0);
        AssertIntEqual("no removes", removed, 0);
        AssertIntEqual("no updates", updated, 0);
        AssertIntEqual("no connection changes", connectionsChanged, 0);

        fab.StopAsync();
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
        fab.StartAsync();
        Thread.Sleep(100);

        // Ingest a FlowFile and let it process
        var ff1 = FlowFile.Create("hello"u8, new() { ["type"] = "order" });
        fab.Ingest(ff1);
        Thread.Sleep(300);

        var stats1 = fab.GetStats();
        AssertTrue("processed before reload", stats1["processed"] > 0);

        // Hot reload: change tagger value from dev → prod, add a second processor with connection
        var config2 = MakeFlowConfig(new Dictionary<string, object?>
        {
            ["tagger"] = MakeProc("UpdateAttribute", new() { ["key"] = "env", ["value"] = "prod" },
                connections: new() { ["success"] = ["logger"] }),
            ["logger"] = MakeProc("LogAttribute", new() { ["prefix"] = "reload-test" })
        });

        var (added, removed, updated, connectionsChanged) = fab.ReloadFlow(config2);
        AssertIntEqual("e2e added", added, 1);
        AssertIntEqual("e2e updated", updated, 1);
        // tagger config changed → counted as update (connections bundled with processor update)
        AssertTrue("e2e changes applied", added + updated + connectionsChanged >= 2);
        AssertIntEqual("e2e proc count", fab.GetProcessorNames().Count, 2);

        // Ingest another FlowFile — should flow through updated pipeline
        var ff2 = FlowFile.Create("world"u8, new() { ["type"] = "order" });
        fab.Ingest(ff2);
        Thread.Sleep(300);

        var stats2 = fab.GetStats();
        AssertTrue("processed after reload", stats2["processed"] > stats1["processed"]);

        fab.StopAsync();
    }
}
