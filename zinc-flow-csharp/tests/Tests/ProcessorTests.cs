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
        TestRouteOnAttributeMatch();
        TestRouteOnAttributeNoMatch();
        TestRouteOnAttributeMultipleRoutes();
        TestRouteRecordBasic();
        TestRouteRecordUnmatched();
        TestRouteRecordFirstMatchWins();
        TestRouteRecordNonRecordContent();
        TestUpdateRecordBasic();
        TestUpdateRecordSequential();
        TestUpdateRecordSchemaless();
        TestSplitRecordFanout();
        TestSplitRecordSingle();
        TestSplitRecordNonRecordContent();
        TestExtractRecordField();
        TestExtractRecordFieldMissing();
        TestQueryRecordFilter();
        TestQueryRecordNoMatch();
        TestQueryRecordContains();
        TestFilterAttributeRemove();
        TestFilterAttributeKeep();
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
        var jsonSchema = new Schema("test", [new Field("name", FieldType.String)]);
        var jsonRec = new Record(jsonSchema);
        jsonRec.SetField("name", "Bob");
        var rc = new RecordContent(jsonSchema, [jsonRec]);
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
        var geoSchema = new Schema("geo", [new Field("city", FieldType.String)]);
        var geoRec = new Record(geoSchema);
        geoRec.SetField("city", "Portland");
        var rc = new RecordContent(geoSchema, [geoRec]);
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

    // --- RouteOnAttribute tests ---

    static void TestRouteOnAttributeMatch()
    {
        Console.WriteLine("--- RouteOnAttribute: Match ---");
        var proc = new RouteOnAttribute("premium:tier EQ premium;bulk:tier EQ bulk");
        var ff = FlowFile.Create("data"u8, new() { ["tier"] = "premium" });
        var result = proc.Process(ff);
        AssertTrue("returns routed", result is RoutedResult);
        var routed = (RoutedResult)result;
        AssertEqual("route is premium", routed.Route, "premium");
    }

    static void TestRouteOnAttributeNoMatch()
    {
        Console.WriteLine("--- RouteOnAttribute: No Match ---");
        var proc = new RouteOnAttribute("premium:tier EQ premium;bulk:tier EQ bulk");
        var ff = FlowFile.Create("data"u8, new() { ["tier"] = "free" });
        var result = proc.Process(ff);
        AssertTrue("returns routed", result is RoutedResult);
        var routed = (RoutedResult)result;
        AssertEqual("route is unmatched", routed.Route, "unmatched");
    }

    static void TestRouteOnAttributeMultipleRoutes()
    {
        Console.WriteLine("--- RouteOnAttribute: Multiple Routes ---");
        var proc = new RouteOnAttribute("premium:tier EQ premium;error:status CONTAINS fail;bulk:tier EQ bulk");

        // First match wins — tier=premium matches first
        var ff1 = FlowFile.Create("data"u8, new() { ["tier"] = "premium", ["status"] = "fail-timeout" });
        var result1 = proc.Process(ff1);
        AssertTrue("returns routed", result1 is RoutedResult);
        AssertEqual("first match wins", ((RoutedResult)result1).Route, "premium");

        // Only error matches
        var ff2 = FlowFile.Create("data"u8, new() { ["tier"] = "free", ["status"] = "fail-timeout" });
        var result2 = proc.Process(ff2);
        AssertTrue("returns routed", result2 is RoutedResult);
        AssertEqual("error route", ((RoutedResult)result2).Route, "error");

        // Bulk match
        var ff3 = FlowFile.Create("data"u8, new() { ["tier"] = "bulk" });
        var result3 = proc.Process(ff3);
        AssertTrue("returns routed", result3 is RoutedResult);
        AssertEqual("bulk route", ((RoutedResult)result3).Route, "bulk");
    }

    // --- RouteRecord tests ---

    static RecordContent MakePeopleRecords()
    {
        var schema = new Schema("person", [
            new Field("name", FieldType.String),
            new Field("age", FieldType.Long),
            new Field("tier", FieldType.String),
        ]);
        var rows = new (string name, long age, string tier)[]
        {
            ("alice", 30, "gold"),
            ("bob",   12, "silver"),
            ("cara",  45, "silver"),
            ("dan",    8, "bronze"),
        };
        var recs = new List<Record>();
        foreach (var r in rows)
        {
            var rec = new Record(schema);
            rec.SetField("name", r.name);
            rec.SetField("age", r.age);
            rec.SetField("tier", r.tier);
            recs.Add(rec);
        }
        return new RecordContent(schema, recs);
    }

    static void TestRouteRecordBasic()
    {
        Console.WriteLine("--- RouteRecord: partition by predicate ---");
        var proc = new RouteRecord("minors: age < 18; adults: age >= 18");
        var ff = FlowFile.CreateWithContent(MakePeopleRecords(), new());
        var result = proc.Process(ff);
        AssertTrue("returns multi-routed", result is MultiRoutedResult);
        var mr = (MultiRoutedResult)result;
        AssertEqual("two buckets", mr.Outputs.Count.ToString(), "2");

        var byRoute = mr.Outputs.ToDictionary(o => o.Route, o => (RecordContent)o.FlowFile.Content);
        AssertTrue("has minors route", byRoute.ContainsKey("minors"));
        AssertTrue("has adults route", byRoute.ContainsKey("adults"));
        AssertEqual("minors count", byRoute["minors"].Records.Count.ToString(), "2");
        AssertEqual("adults count", byRoute["adults"].Records.Count.ToString(), "2");
        AssertEqual("minor name 0", (string)byRoute["minors"].Records[0].GetField("name")!, "bob");
        AssertEqual("minor name 1", (string)byRoute["minors"].Records[1].GetField("name")!, "dan");
        AssertEqual("adult name 0", (string)byRoute["adults"].Records[0].GetField("name")!, "alice");
        AssertEqual("adult name 1", (string)byRoute["adults"].Records[1].GetField("name")!, "cara");
    }

    static void TestRouteRecordUnmatched()
    {
        Console.WriteLine("--- RouteRecord: unmatched bucket for non-matching records ---");
        var proc = new RouteRecord("gold: tier == \"gold\"");
        var ff = FlowFile.CreateWithContent(MakePeopleRecords(), new());
        var result = proc.Process(ff);
        AssertTrue("returns multi-routed", result is MultiRoutedResult);
        var byRoute = ((MultiRoutedResult)result).Outputs.ToDictionary(o => o.Route, o => (RecordContent)o.FlowFile.Content);
        AssertTrue("has gold route", byRoute.ContainsKey("gold"));
        AssertTrue("has unmatched route", byRoute.ContainsKey("unmatched"));
        AssertEqual("gold count", byRoute["gold"].Records.Count.ToString(), "1");
        AssertEqual("unmatched count", byRoute["unmatched"].Records.Count.ToString(), "3");
        AssertEqual("gold record", (string)byRoute["gold"].Records[0].GetField("name")!, "alice");
    }

    static void TestRouteRecordFirstMatchWins()
    {
        Console.WriteLine("--- RouteRecord: first match wins ---");
        // Both predicates would match an adult with tier=silver; minors wins by
        // being declared first (so ordering in config matters, as documented).
        var proc = new RouteRecord("seniors: age >= 40; silver: tier == \"silver\"");
        var ff = FlowFile.CreateWithContent(MakePeopleRecords(), new());
        var result = proc.Process(ff);
        var byRoute = ((MultiRoutedResult)result).Outputs.ToDictionary(o => o.Route, o => (RecordContent)o.FlowFile.Content);
        // cara is 45 and tier=silver — seniors should claim her, not silver.
        AssertEqual("seniors count", byRoute["seniors"].Records.Count.ToString(), "1");
        AssertEqual("seniors record", (string)byRoute["seniors"].Records[0].GetField("name")!, "cara");
        // bob is tier=silver (age 12, doesn't hit seniors), so silver claims him.
        AssertEqual("silver count", byRoute["silver"].Records.Count.ToString(), "1");
        AssertEqual("silver record", (string)byRoute["silver"].Records[0].GetField("name")!, "bob");
        // alice (gold) and dan (bronze) unmatched.
        AssertEqual("unmatched count", byRoute["unmatched"].Records.Count.ToString(), "2");
    }

    static void TestRouteRecordNonRecordContent()
    {
        Console.WriteLine("--- RouteRecord: non-record content passes through ---");
        var proc = new RouteRecord("x: age > 0");
        var ff = FlowFile.Create("raw bytes"u8, new());
        var result = proc.Process(ff);
        AssertTrue("passthrough as single", result is SingleResult);
    }

    // --- UpdateRecord tests ---

    static RecordContent MakeAmountRecords()
    {
        var schema = new Schema("order", [
            new Field("amount", FieldType.Double),
            new Field("region", FieldType.String),
        ]);
        var a = new Record(schema); a.SetField("amount", 100.0); a.SetField("region", "us");
        var b = new Record(schema); b.SetField("amount", 50.0); b.SetField("region", "eu");
        return new RecordContent(schema, [a, b]);
    }

    static void TestUpdateRecordBasic()
    {
        Console.WriteLine("--- UpdateRecord: derive single field ---");
        // amount / 10 keeps the arithmetic FP-exact for string comparison.
        var proc = new UpdateRecord("tax = amount / 10");
        var ff = FlowFile.CreateWithContent(MakeAmountRecords(), new());
        var result = proc.Process(ff);
        AssertTrue("returns single", result is SingleResult);
        var rc = (RecordContent)((SingleResult)result).FlowFile.Content;
        AssertEqual("two records", rc.Records.Count.ToString(), "2");
        AssertEqual("record 0 tax", rc.Records[0].GetField("tax")!.ToString(), "10");
        AssertEqual("record 1 tax", rc.Records[1].GetField("tax")!.ToString(), "5");
        AssertTrue("schema has tax", rc.Schema!.Fields.Any(f => f.Name == "tax"));
    }

    static void TestUpdateRecordSequential()
    {
        Console.WriteLine("--- UpdateRecord: later exprs see earlier writes ---");
        var proc = new UpdateRecord("tax = amount / 10; total = amount + tax");
        var ff = FlowFile.CreateWithContent(MakeAmountRecords(), new());
        var result = proc.Process(ff);
        var rc = (RecordContent)((SingleResult)result).FlowFile.Content;
        AssertEqual("record 0 total", rc.Records[0].GetField("total")!.ToString(), "110");
        AssertEqual("record 1 total", rc.Records[1].GetField("total")!.ToString(), "55");
    }

    static void TestUpdateRecordSchemaless()
    {
        Console.WriteLine("--- UpdateRecord: schemaless input → schemaless output ---");
        var r = new Record();
        r.SetField("x", 3L);
        r.SetField("y", 4L);
        var rc = new RecordContent([r]);
        var ff = FlowFile.CreateWithContent(rc, new());
        var proc = new UpdateRecord("sum = x + y");
        var result = proc.Process(ff);
        var outRc = (RecordContent)((SingleResult)result).FlowFile.Content;
        AssertEqual("sum", outRc.Records[0].GetField("sum")!.ToString(), "7");
    }

    // --- SplitRecord tests ---

    static void TestSplitRecordFanout()
    {
        Console.WriteLine("--- SplitRecord: N records -> N FlowFiles ---");
        var ff = FlowFile.CreateWithContent(MakePeopleRecords(), new() { ["source"] = "upstream" });
        var proc = new SplitRecord();
        var result = proc.Process(ff);
        AssertTrue("multiple result", result is MultipleResult);
        var outs = ((MultipleResult)result).FlowFiles;
        AssertEqual("four children", outs.Count.ToString(), "4");
        for (int i = 0; i < outs.Count; i++)
        {
            var child = outs[i];
            AssertTrue($"child {i} is record content", child.Content is RecordContent);
            var childRc = (RecordContent)child.Content;
            AssertEqual($"child {i} has 1 record", childRc.Records.Count.ToString(), "1");
            AssertTrue($"child {i} preserves source attr",
                child.Attributes.TryGetValue("source", out var s) && s == "upstream");
            AssertTrue($"child {i} has split.total",
                child.Attributes.TryGetValue("split.total", out var t) && t == "4");
            AssertTrue($"child {i} has split.index",
                child.Attributes.TryGetValue("split.index", out var _));
        }
        // Records preserve their order.
        AssertEqual("child 0 name", (string)((RecordContent)outs[0].Content).Records[0].GetField("name")!, "alice");
        AssertEqual("child 3 name", (string)((RecordContent)outs[3].Content).Records[0].GetField("name")!, "dan");
    }

    static void TestSplitRecordSingle()
    {
        Console.WriteLine("--- SplitRecord: 1 record -> 1 FlowFile ---");
        var schema = new Schema("x", [new Field("v", FieldType.Long)]);
        var r = new Record(schema); r.SetField("v", 42L);
        var ff = FlowFile.CreateWithContent(new RecordContent(schema, [r]), new());
        var result = new SplitRecord().Process(ff);
        AssertTrue("multiple result", result is MultipleResult);
        var outs = ((MultipleResult)result).FlowFiles;
        AssertEqual("one child", outs.Count.ToString(), "1");
        AssertTrue("index zero-padded to width 1",
            outs[0].Attributes.TryGetValue("split.index", out var i) && i == "0");
    }

    static void TestSplitRecordNonRecordContent()
    {
        Console.WriteLine("--- SplitRecord: raw content passes through ---");
        var ff = FlowFile.Create("bytes"u8, new());
        var result = new SplitRecord().Process(ff);
        AssertTrue("passes through as single", result is SingleResult);
    }

    // --- ExtractRecordField tests ---

    static void TestExtractRecordField()
    {
        Console.WriteLine("--- ExtractRecordField ---");
        var schema = new Schema("order", [new Field("name", FieldType.String), new Field("amount", FieldType.Double)]);
        var rec = new Record(schema);
        rec.SetField("name", "Alice");
        rec.SetField("amount", 42.5);
        var rc = new RecordContent(schema, [rec]);
        var ff = FlowFile.CreateWithContent(rc, new() { ["type"] = "order" });

        var proc = new ExtractRecordField("name:customer_name;amount:order_amount");
        var result = proc.Process(ff);
        AssertTrue("returns single", result is SingleResult);
        var outFf = ((SingleResult)result).FlowFile;
        AssertTrue("customer_name set", outFf.Attributes.TryGetValue("customer_name", out var n) && n == "Alice");
        AssertTrue("order_amount set", outFf.Attributes.TryGetValue("order_amount", out var a) && a == "42.5");
        AssertTrue("original attr preserved", outFf.Attributes.TryGetValue("type", out var t) && t == "order");
    }

    static void TestExtractRecordFieldMissing()
    {
        Console.WriteLine("--- ExtractRecordField: Missing field ---");
        var schema = new Schema("order", [new Field("name", FieldType.String)]);
        var rec = new Record(schema);
        rec.SetField("name", "Bob");
        var rc = new RecordContent(schema, [rec]);
        var ff = FlowFile.CreateWithContent(rc, new());

        // Ask for a field that doesn't exist
        var proc = new ExtractRecordField("name:customer_name;missing_field:missing_attr");
        var result = proc.Process(ff);
        AssertTrue("returns single", result is SingleResult);
        var outFf = ((SingleResult)result).FlowFile;
        AssertTrue("customer_name set", outFf.Attributes.TryGetValue("customer_name", out var n) && n == "Bob");
        AssertFalse("missing_attr not set", outFf.Attributes.ContainsKey("missing_attr"));
    }

    // --- QueryRecord tests ---

    static void TestQueryRecordFilter()
    {
        Console.WriteLine("--- QueryRecord: Filter ---");
        var schema = new Schema("data", [new Field("name", FieldType.String), new Field("score", FieldType.Double)]);
        var rec1 = new Record(schema); rec1.SetField("name", "Alice"); rec1.SetField("score", 85.0);
        var rec2 = new Record(schema); rec2.SetField("name", "Bob"); rec2.SetField("score", 45.0);
        var rec3 = new Record(schema); rec3.SetField("name", "Charlie"); rec3.SetField("score", 92.0);
        var rc = new RecordContent(schema, [rec1, rec2, rec3]);
        var ff = FlowFile.CreateWithContent(rc, new());

        var proc = new QueryRecord("$[?(@.score > 50)]");
        var result = proc.Process(ff);
        AssertTrue("returns single", result is SingleResult);
        var outFf = ((SingleResult)result).FlowFile;
        AssertTrue("output is records", outFf.Content is RecordContent);
        var outRc = (RecordContent)outFf.Content;
        AssertIntEqual("filtered to 2 records", outRc.Records.Count, 2);
        AssertEqual("first is Alice", outRc.Records[0].GetField("name")!.ToString()!, "Alice");
        AssertEqual("second is Charlie", outRc.Records[1].GetField("name")!.ToString()!, "Charlie");
    }

    static void TestQueryRecordNoMatch()
    {
        Console.WriteLine("--- QueryRecord: No Match ---");
        var schema = new Schema("data", [new Field("name", FieldType.String), new Field("score", FieldType.Double)]);
        var rec1 = new Record(schema); rec1.SetField("name", "Alice"); rec1.SetField("score", 30.0);
        var rec2 = new Record(schema); rec2.SetField("name", "Bob"); rec2.SetField("score", 25.0);
        var rc = new RecordContent(schema, [rec1, rec2]);
        var ff = FlowFile.CreateWithContent(rc, new());

        var proc = new QueryRecord("$[?(@.score > 90)]");
        var result = proc.Process(ff);
        AssertTrue("no match returns dropped", result is DroppedResult);
    }

    static void TestQueryRecordContains()
    {
        Console.WriteLine("--- QueryRecord: Contains ---");
        var schema = new Schema("data", [new Field("name", FieldType.String), new Field("email", FieldType.String)]);
        var rec1 = new Record(schema); rec1.SetField("name", "Alice"); rec1.SetField("email", "alice@test.com");
        var rec2 = new Record(schema); rec2.SetField("name", "Bob"); rec2.SetField("email", "bob@other.org");
        var rec3 = new Record(schema); rec3.SetField("name", "Charlie"); rec3.SetField("email", "charlie@test.com");
        var rc = new RecordContent(schema, [rec1, rec2, rec3]);
        var ff = FlowFile.CreateWithContent(rc, new());

        // JsonPath: =~ is a JsonPath regex-match operator supported by
        // Newtonsoft.Json — matches records whose email contains "test.com".
        var proc = new QueryRecord("$[?(@.email =~ /test\\.com/)]");
        var result = proc.Process(ff);
        AssertTrue("returns single", result is SingleResult);
        var outRc = (RecordContent)((SingleResult)result).FlowFile.Content;
        AssertIntEqual("filtered to 2 records", outRc.Records.Count, 2);
        AssertEqual("first is Alice", outRc.Records[0].GetField("name")!.ToString()!, "Alice");
        AssertEqual("second is Charlie", outRc.Records[1].GetField("name")!.ToString()!, "Charlie");
    }

    // --- FilterAttribute tests ---

    static void TestFilterAttributeRemove()
    {
        Console.WriteLine("--- FilterAttribute: Remove ---");
        var ff = FlowFile.Create("data"u8, new() { ["type"] = "order", ["env"] = "prod", ["secret"] = "password123" });

        var proc = new FilterAttribute("remove", "secret");
        var result = proc.Process(ff);
        AssertTrue("returns single", result is SingleResult);
        var outFf = ((SingleResult)result).FlowFile;
        AssertTrue("type preserved", outFf.Attributes.TryGetValue("type", out var t) && t == "order");
        AssertTrue("env preserved", outFf.Attributes.TryGetValue("env", out var e) && e == "prod");
        AssertFalse("secret removed", outFf.Attributes.ContainsKey("secret"));
    }

    static void TestFilterAttributeKeep()
    {
        Console.WriteLine("--- FilterAttribute: Keep ---");
        var ff = FlowFile.Create("data"u8, new() { ["type"] = "order", ["env"] = "prod", ["secret"] = "password123" });

        var proc = new FilterAttribute("keep", "type;env");
        var result = proc.Process(ff);
        AssertTrue("returns single", result is SingleResult);
        var outFf = ((SingleResult)result).FlowFile;
        AssertTrue("type kept", outFf.Attributes.TryGetValue("type", out var t) && t == "order");
        AssertTrue("env kept", outFf.Attributes.TryGetValue("env", out var e) && e == "prod");
        AssertFalse("secret dropped", outFf.Attributes.ContainsKey("secret"));
    }
}
