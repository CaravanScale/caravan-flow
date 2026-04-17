using System.Text;
using CaravanFlow.Core;
using CaravanFlow.Fabric;
using CaravanFlow.StdLib;
using static CaravanFlow.Tests.TestRunner;
using static CaravanFlow.Tests.Helpers;

namespace CaravanFlow.Tests;

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
        var jsonRec = new GenericRecord(jsonSchema);
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
        var geoRec = new GenericRecord(geoSchema);
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

    // --- ExtractRecordField tests ---

    static void TestExtractRecordField()
    {
        Console.WriteLine("--- ExtractRecordField ---");
        var schema = new Schema("order", [new Field("name", FieldType.String), new Field("amount", FieldType.Double)]);
        var rec = new GenericRecord(schema);
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
        var rec = new GenericRecord(schema);
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
        var rec1 = new GenericRecord(schema); rec1.SetField("name", "Alice"); rec1.SetField("score", 85.0);
        var rec2 = new GenericRecord(schema); rec2.SetField("name", "Bob"); rec2.SetField("score", 45.0);
        var rec3 = new GenericRecord(schema); rec3.SetField("name", "Charlie"); rec3.SetField("score", 92.0);
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
        var rec1 = new GenericRecord(schema); rec1.SetField("name", "Alice"); rec1.SetField("score", 30.0);
        var rec2 = new GenericRecord(schema); rec2.SetField("name", "Bob"); rec2.SetField("score", 25.0);
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
        var rec1 = new GenericRecord(schema); rec1.SetField("name", "Alice"); rec1.SetField("email", "alice@test.com");
        var rec2 = new GenericRecord(schema); rec2.SetField("name", "Bob"); rec2.SetField("email", "bob@other.org");
        var rec3 = new GenericRecord(schema); rec3.SetField("name", "Charlie"); rec3.SetField("email", "charlie@test.com");
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
