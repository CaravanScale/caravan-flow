using System.Text;
using ZincFlow.Core;
using ZincFlow.Fabric;
using ZincFlow.StdLib;
using static ZincFlow.Tests.TestRunner;
using static ZincFlow.Tests.Helpers;

namespace ZincFlow.Tests;

public static class ProcessorTests
{
    public static void RunAll()
    {
        TestUpdateAttribute();
        TestLogAttribute();
        TestConvertJSONToRecord();
        TestConvertRecordToJSON();
        TestJSONRoundtrip();
        TestConvertJSONToRecordEmpty();
        TestRoutingEQ();
        TestRoutingNEQ();
        TestRoutingContains();
        TestRoutingStartsEndsWith();
        TestRoutingExists();
        TestRoutingComposite();
        TestRoutingNoMatch();
    }

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
}
