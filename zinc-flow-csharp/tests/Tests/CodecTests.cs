using System.Text;
using ZincFlow.Core;
using ZincFlow.Fabric;
using ZincFlow.StdLib;
using static ZincFlow.Tests.TestRunner;
using static ZincFlow.Tests.Helpers;

namespace ZincFlow.Tests;

public static class CodecTests
{
    public static void RunAll()
    {
        TestAvroZigzagVarint();
        TestAvroRoundtrip();
        TestAvroMultipleRecords();
        TestCsvRoundtrip();
        TestCsvQuotedFields();
        TestCsvNoHeader();
        TestReplaceText();
        TestExtractText();
        TestSplitText();
        TestConvertAvroToRecord();
        TestConvertRecordToAvro();
        TestConvertCSVToRecord();
        TestConvertRecordToCSV();
        TestAvroJsonRoundtrip();
        TestEvaluateExpression();
        TestEvaluateExpressionFunctions();
        TestTransformRecord();
    }

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
        AssertEqual("name field", rc.Records[0].GetField("name")?.ToString() ?? "", "Widget");
        AssertIntEqual("qty field", Convert.ToInt32(rc.Records[0].GetField("qty")), 10);
    }

    static void TestConvertRecordToAvro()
    {
        Console.WriteLine("--- ConvertRecordToAvro ---");
        var avroSchema = new Schema("order", [
            new Field("item", FieldType.String),
            new Field("count", FieldType.Int)
        ]);
        var rec1 = new GenericRecord(avroSchema);
        rec1.SetField("item", "Gadget");
        rec1.SetField("count", 5);
        var rc = new RecordContent(avroSchema, [rec1]);
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
        AssertEqual("first name", rc.Records[0].GetField("name")?.ToString() ?? "", "Alice");
    }

    static void TestConvertRecordToCSV()
    {
        Console.WriteLine("--- ConvertRecordToCSV ---");
        var csvSchema = new Schema("data", [
            new Field("x", FieldType.String),
            new Field("y", FieldType.String)
        ]);
        var csvRec1 = new GenericRecord(csvSchema);
        csvRec1.SetField("x", "hello");
        csvRec1.SetField("y", "world");
        var csvRec2 = new GenericRecord(csvSchema);
        csvRec2.SetField("x", "foo");
        csvRec2.SetField("y", "bar");
        var rc = new RecordContent(csvSchema, [csvRec1, csvRec2]);
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
        var trSchema = new Schema("test", [
            new Field("first_name", FieldType.String),
            new Field("age", FieldType.String),
            new Field("temp", FieldType.String)
        ]);
        var trRec = new GenericRecord(trSchema);
        trRec.SetField("first_name", "alice");
        trRec.SetField("age", "30");
        trRec.SetField("temp", "x");
        var rc = new RecordContent(trSchema, [trRec]);
        var ff = FlowFile.CreateWithContent(rc, new());

        var proc = new TransformRecord("rename:first_name:name;remove:temp;add:source:api;toUpper:name;default:missing:none");
        var result = proc.Process(ff);
        var outFf = ((SingleResult)result).FlowFile;
        var outRc = (RecordContent)outFf.Content;
        var rec = outRc.Records[0];
        AssertEqual("renamed + upper", rec.GetField("name")?.ToString() ?? "", "ALICE");
        AssertTrue("temp removed", rec.GetField("temp") is null);
        AssertEqual("added source", rec.GetField("source")?.ToString() ?? "", "api");
        AssertEqual("default missing", rec.GetField("missing")?.ToString() ?? "", "none");
        AssertEqual("age preserved", rec.GetField("age")?.ToString() ?? "", "30");
    }
}
