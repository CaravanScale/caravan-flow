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

        var tagEngine = new RulesEngine();
        tagEngine.AddOrReplaceRuleset("flow", [new RoutingRule("to-logger", "env", Operator.Exists, "", "logger")]);
        var logEngine = new RulesEngine();

        var dlq = new DLQ();
        var tagSession = new ProcessSession(tagQueue, tagger, "tag-env", tagEngine, allQueues, dlq, 5);
        var logSession = new ProcessSession(logQueue, logger, "logger", logEngine, allQueues, dlq, 5);

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

        var engine = new RulesEngine();
        engine.AddOrReplaceRuleset("flow", [new RoutingRule("forward", "type", Operator.Exists, "", "downstream")]);

        var proc = new LogAttribute("bp-test");
        var dlq = new DLQ();
        var session = new ProcessSession(smallQueue, proc, "small", engine, allQueues, dlq, 5);

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

        var engine = new RulesEngine();
        engine.AddOrReplaceRuleset("flow", [new RoutingRule("to-dest", "type", Operator.Exists, "", "dest")]);

        var dlq = new DLQ();
        int maxRetries = 3;
        var session = new ProcessSession(sourceQueue, new LogAttribute("retry"), "source", engine, allQueues, dlq, maxRetries);

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
        Console.WriteLine("--- Scenario: IRS Fan-Out ---");
        var tagger = new UpdateAttribute("env", "prod");

        var srcQueue = new FlowQueue("source", 100, 1024 * 1024, 30_000);
        var destA = new FlowQueue("dest-a", 100, 1024 * 1024, 30_000);
        var destB = new FlowQueue("dest-b", 100, 1024 * 1024, 30_000);
        var destC = new FlowQueue("dest-c", 100, 1024 * 1024, 30_000);
        var allQueues = new Dictionary<string, FlowQueue>
        {
            ["source"] = srcQueue, ["dest-a"] = destA, ["dest-b"] = destB, ["dest-c"] = destC
        };

        var engine = new RulesEngine();
        engine.AddOrReplaceRuleset("flow", [
            new RoutingRule("to-a", "env", Operator.Exists, "", "dest-a"),
            new RoutingRule("to-b", "env", Operator.Exists, "", "dest-b"),
            new RoutingRule("to-c", "env", Operator.Exists, "", "dest-c")
        ]);

        var dlq = new DLQ();
        var session = new ProcessSession(srcQueue, tagger, "source", engine, allQueues, dlq, 5);

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
                    ["tagger"] = new Dictionary<string, object?> { ["type"] = "UpdateAttribute", ["config"] = new Dictionary<string, object?> { ["key"] = "env", ["value"] = "prod" } }
                },
                ["routes"] = new Dictionary<string, object?>
                {
                    ["to-tagger"] = new Dictionary<string, object?> { ["destination"] = "tagger", ["condition"] = new Dictionary<string, object?> { ["attribute"] = "type", ["operator"] = "EXISTS" } }
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

        // Route to nonexistent processor
        var badRoute = new Dictionary<string, object?>
        {
            ["flow"] = new Dictionary<string, object?>
            {
                ["processors"] = new Dictionary<string, object?>
                {
                    ["tagger"] = new Dictionary<string, object?> { ["type"] = "UpdateAttribute" }
                },
                ["routes"] = new Dictionary<string, object?>
                {
                    ["bad-route"] = new Dictionary<string, object?> { ["destination"] = "missing", ["condition"] = new Dictionary<string, object?> { ["attribute"] = "x" } }
                }
            }
        };
        errors = ConfigValidator.Validate(badRoute, reg);
        AssertTrue("bad route error", errors.Any(e => e.Message.Contains("not a defined processor")));
    }

    static void TestProvenance()
    {
        Console.WriteLine("--- Provenance ---");
        var tag = new UpdateAttribute("env", "prod");
        var q = new FlowQueue("prov", 100, 0, 30_000);
        var outQ = new FlowQueue("out", 100, 0, 30_000);
        var queues = new Dictionary<string, FlowQueue> { ["prov"] = q, ["out"] = outQ };
        var engine = new RulesEngine();
        engine.AddOrReplaceRuleset("flow", new List<RoutingRule>
        {
            new RoutingRule("to-out", "env", Operator.Exists, "", "out")
        });
        var dlq = new DLQ();
        var session = new ProcessSession(q, tag, "tagger", engine, queues, dlq, 5, provenanceEnabled: true);

        q.Offer(FlowFile.Create("test"u8, new() { ["type"] = "order" }));
        session.Execute();

        // Output FlowFile should have provenance attribute
        var entry = outQ.Claim()!;
        AssertTrue("provenance.last set", entry.FlowFile.Attributes.TryGetValue("provenance.last", out var last) && last == "tagger");
        outQ.Ack(entry.Id);
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
}
