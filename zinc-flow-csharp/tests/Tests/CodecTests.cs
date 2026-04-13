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
        TestAvroSchemaJsonParse();
        TestOCFRoundtripNullCodec();
        TestOCFRoundtripDeflateCodec();
        TestOCFReadPrebuilt();
        TestOCFProcessorRoundtrip();
        TestLogicalTypesSchemaRoundtrip();
        TestLogicalTypeHelpers();
        TestOCFLogicalTypePreserved();
        TestDecimalBytesRoundtrip();
        TestEvaluateExpression();
        TestEvaluateExpressionFunctions();
        TestTransformRecord();
        TestTransformRecordCompute();
        TestTransformRecordPreservesTypes();
        TestTransformRecordChainedCompute();
        TestNestedFieldPathsRead();
        TestNestedPathsInQueryRecord();
        TestNestedPathsInExtractRecordField();
        TestNestedPathsInComputeExpression();
        TestSetByPathCreatesIntermediates();
        TestSchemaEvolutionPromotion();
        TestSchemaEvolutionFieldAddedWithDefault();
        TestSchemaEvolutionFieldAddedNoDefault();
        TestSchemaEvolutionFieldRemoved();
        TestSchemaEvolutionIncompatible();
        TestOCFRoundtripWithEvolution();
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

    static void TestAvroSchemaJsonParse()
    {
        Console.WriteLine("--- Avro: JSON schema parse/emit ---");
        var json = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[" +
                   "{\"name\":\"id\",\"type\":\"long\"}," +
                   "{\"name\":\"name\",\"type\":\"string\"}," +
                   "{\"name\":\"email\",\"type\":[\"null\",\"string\"]}," +
                   "{\"name\":\"active\",\"type\":\"boolean\"}]}";
        var schema = AvroSchemaJson.Parse(json);
        AssertEqual("schema name", schema.Name, "User");
        AssertIntEqual("field count", schema.Fields.Count, 4);
        AssertEqual("id is long", schema.Fields[0].FieldType.ToString(), "Long");
        AssertEqual("name is string", schema.Fields[1].FieldType.ToString(), "String");
        // Nullable-union ["null","string"] should resolve to String (nullability erased).
        AssertEqual("email resolves to string", schema.Fields[2].FieldType.ToString(), "String");
        AssertEqual("active is boolean", schema.Fields[3].FieldType.ToString(), "Boolean");

        var emitted = AvroSchemaJson.Emit(schema);
        AssertTrue("emitted has record type", emitted.Contains("\"type\":\"record\""));
        AssertTrue("emitted has User name", emitted.Contains("\"name\":\"User\""));
        AssertTrue("emitted has id field", emitted.Contains("\"name\":\"id\""));

        // Roundtrip: re-parse the emitted output.
        var reparsed = AvroSchemaJson.Parse(emitted);
        AssertIntEqual("reparse field count", reparsed.Fields.Count, 4);
    }

    static void TestOCFRoundtripNullCodec()
    {
        Console.WriteLine("--- OCF: roundtrip (null codec) ---");
        var schema = new Schema("user", [
            new Field("id", FieldType.Long),
            new Field("name", FieldType.String),
            new Field("score", FieldType.Double)
        ]);
        var records = new List<GenericRecord>();
        for (int i = 0; i < 3; i++)
        {
            var r = new GenericRecord(schema);
            r.SetField("id", (long)(i + 1));
            r.SetField("name", $"user-{i}");
            r.SetField("score", i * 1.5);
            records.Add(r);
        }

        var writer = new OCFWriter(AvroOCF.CodecNull);
        var bytes = writer.Write(records, schema);
        AssertTrue("ocf magic Obj", bytes.Length > 4 && bytes[0] == 0x4F && bytes[1] == 0x62 && bytes[2] == 0x6A && bytes[3] == 0x01);

        var reader = new OCFReader();
        var (decodedSchema, decoded) = reader.Read(bytes);
        AssertEqual("decoded schema name", decodedSchema.Name, "user");
        AssertIntEqual("decoded record count", decoded.Count, 3);
        AssertEqual("record 0 name", decoded[0].GetField("name")?.ToString() ?? "", "user-0");
        AssertIntEqual("record 2 id", (int)(long)(decoded[2].GetField("id") ?? 0L), 3);
    }

    static void TestOCFRoundtripDeflateCodec()
    {
        Console.WriteLine("--- OCF: roundtrip (deflate codec) ---");
        var schema = new Schema("event", [
            new Field("ts", FieldType.Long),
            new Field("message", FieldType.String)
        ]);
        var records = new List<GenericRecord>();
        // Many repetitive records so deflate compresses meaningfully.
        for (int i = 0; i < 50; i++)
        {
            var r = new GenericRecord(schema);
            r.SetField("ts", (long)(1700000000 + i));
            r.SetField("message", "login success login success login success");
            records.Add(r);
        }

        var writer = new OCFWriter(AvroOCF.CodecDeflate);
        var bytes = writer.Write(records, schema);

        var reader = new OCFReader();
        var (_, decoded) = reader.Read(bytes);
        AssertIntEqual("deflate decoded count", decoded.Count, 50);
        AssertEqual("deflate last message", decoded[49].GetField("message")?.ToString() ?? "", "login success login success login success");
    }

    static void TestOCFReadPrebuilt()
    {
        Console.WriteLine("--- OCF: rejects bad magic ---");
        var bad = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00 };
        var reader = new OCFReader();
        var threw = false;
        try { reader.Read(bad); }
        catch (InvalidOperationException) { threw = true; }
        AssertTrue("bad magic throws", threw);
    }

    static void TestLogicalTypesSchemaRoundtrip()
    {
        Console.WriteLine("--- Avro: logical types in schema parse/emit ---");
        var json = "{\"type\":\"record\",\"name\":\"Event\",\"fields\":[" +
                   "{\"name\":\"id\",\"type\":{\"type\":\"string\",\"logicalType\":\"uuid\"}}," +
                   "{\"name\":\"created_at\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}," +
                   "{\"name\":\"birthday\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}}," +
                   "{\"name\":\"start_time\",\"type\":{\"type\":\"int\",\"logicalType\":\"time-millis\"}}," +
                   "{\"name\":\"amount\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":12,\"scale\":2}}]}";
        var schema = AvroSchemaJson.Parse(json);
        var byName = schema.Fields.ToDictionary(f => f.Name, f => f);

        AssertTrue("uuid logical type", byName["id"].LogicalType == LogicalType.Uuid);
        AssertTrue("uuid underlying = String", byName["id"].FieldType == FieldType.String);
        AssertTrue("timestamp-millis logical", byName["created_at"].LogicalType == LogicalType.TimestampMillis);
        AssertTrue("timestamp-millis underlying = Long", byName["created_at"].FieldType == FieldType.Long);
        AssertTrue("date logical", byName["birthday"].LogicalType == LogicalType.Date);
        AssertTrue("date underlying = Int", byName["birthday"].FieldType == FieldType.Int);
        AssertTrue("time-millis logical", byName["start_time"].LogicalType == LogicalType.TimeMillis);
        AssertTrue("decimal logical", byName["amount"].LogicalType == LogicalType.Decimal);
        AssertTrue("decimal precision", byName["amount"].Precision == 12);
        AssertTrue("decimal scale", byName["amount"].Scale == 2);

        // Emit + reparse roundtrip
        var emitted = AvroSchemaJson.Emit(schema);
        AssertTrue("emitted has uuid", emitted.Contains("\"logicalType\":\"uuid\""));
        AssertTrue("emitted has timestamp-millis", emitted.Contains("\"logicalType\":\"timestamp-millis\""));
        AssertTrue("emitted has decimal precision", emitted.Contains("\"precision\":12"));
        AssertTrue("emitted has decimal scale", emitted.Contains("\"scale\":2"));

        var reparsed = AvroSchemaJson.Parse(emitted);
        var reByName = reparsed.Fields.ToDictionary(f => f.Name, f => f);
        AssertTrue("reparse uuid", reByName["id"].LogicalType == LogicalType.Uuid);
        AssertTrue("reparse decimal scale preserved", reByName["amount"].Scale == 2);
    }

    static void TestLogicalTypeHelpers()
    {
        Console.WriteLine("--- LogicalTypeHelpers: conversions ---");
        // timestamp-millis
        var dt = new DateTime(2024, 6, 15, 12, 30, 45, DateTimeKind.Utc);
        var millis = LogicalTypeHelpers.ToTimestampMillis(dt);
        var roundDt = LogicalTypeHelpers.FromTimestampMillis(millis);
        AssertTrue("timestamp-millis roundtrip ms", roundDt == dt);

        // timestamp-micros (DateTime resolution = 100ns; ensure micro precision survives)
        var dtMicro = new DateTime(2024, 6, 15, 12, 30, 45, DateTimeKind.Utc).AddTicks(1230); // 123 micros
        var micros = LogicalTypeHelpers.ToTimestampMicros(dtMicro);
        var roundMicro = LogicalTypeHelpers.FromTimestampMicros(micros);
        AssertTrue("timestamp-micros roundtrip", roundMicro == dtMicro);

        // date
        var d = new DateOnly(2024, 6, 15);
        var days = LogicalTypeHelpers.ToDate(d);
        AssertTrue("date roundtrip", LogicalTypeHelpers.FromDate(days) == d);

        // time-millis
        var t = new TimeOnly(14, 30, 0);
        var tMillis = LogicalTypeHelpers.ToTimeMillis(t);
        AssertTrue("time-millis roundtrip", LogicalTypeHelpers.FromTimeMillis(tMillis) == t);

        // time-micros
        var tMicro = new TimeOnly(14, 30, 0).Add(TimeSpan.FromTicks(5000)); // 500 micros
        var tMicros = LogicalTypeHelpers.ToTimeMicros(tMicro);
        AssertTrue("time-micros roundtrip", LogicalTypeHelpers.FromTimeMicros(tMicros) == tMicro);

        // uuid
        var g = Guid.NewGuid();
        AssertTrue("uuid roundtrip", LogicalTypeHelpers.FromUuid(LogicalTypeHelpers.ToUuid(g)) == g);
    }

    static void TestOCFLogicalTypePreserved()
    {
        Console.WriteLine("--- OCF: logical type metadata roundtrips through file ---");
        var schema = new Schema("event", [
            new Field("event_id", FieldType.String, logicalType: LogicalType.Uuid),
            new Field("ts", FieldType.Long, logicalType: LogicalType.TimestampMillis),
            new Field("event_date", FieldType.Int, logicalType: LogicalType.Date)
        ]);
        var rec = new GenericRecord(schema);
        var g = Guid.NewGuid();
        var dt = new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc);
        var d = new DateOnly(2024, 6, 15);
        rec.SetField("event_id", LogicalTypeHelpers.ToUuid(g));
        rec.SetField("ts", LogicalTypeHelpers.ToTimestampMillis(dt));
        rec.SetField("event_date", LogicalTypeHelpers.ToDate(d));

        var bytes = new OCFWriter(AvroOCF.CodecNull).Write([rec], schema);
        var (decodedSchema, decoded) = new OCFReader().Read(bytes);

        var byName = decodedSchema.Fields.ToDictionary(f => f.Name, f => f);
        AssertTrue("decoded uuid logical", byName["event_id"].LogicalType == LogicalType.Uuid);
        AssertTrue("decoded ts logical", byName["ts"].LogicalType == LogicalType.TimestampMillis);
        AssertTrue("decoded date logical", byName["event_date"].LogicalType == LogicalType.Date);

        // Values survive (they're stored as the underlying primitive)
        AssertTrue("uuid value", LogicalTypeHelpers.FromUuid(decoded[0].GetField("event_id")?.ToString() ?? "") == g);
        AssertTrue("ts value", LogicalTypeHelpers.FromTimestampMillis((long)(decoded[0].GetField("ts") ?? 0L)) == dt);
        AssertTrue("date value", LogicalTypeHelpers.FromDate(Convert.ToInt32(decoded[0].GetField("event_date") ?? 0)) == d);
    }

    static void TestDecimalBytesRoundtrip()
    {
        Console.WriteLine("--- LogicalTypeHelpers: decimal byte roundtrip ---");
        // Common scales
        decimal[] values = { 0m, 1m, -1m, 1234.56m, -987.65m, 99999999.99m, 0.01m };
        foreach (var v in values)
        {
            var bytes = LogicalTypeHelpers.ToDecimalBytes(v, 2);
            var back = LogicalTypeHelpers.FromDecimalBytes(bytes, 2);
            AssertTrue($"decimal({v}) roundtrip", back == v);
        }
        // Different scale
        var bigBytes = LogicalTypeHelpers.ToDecimalBytes(123.456789m, 6);
        AssertTrue("decimal scale 6 roundtrip", LogicalTypeHelpers.FromDecimalBytes(bigBytes, 6) == 123.456789m);
    }

    static void TestTransformRecordCompute()
    {
        Console.WriteLine("--- TransformRecord: compute (typed expressions) ---");
        var schema = new Schema("order", [
            new Field("price", FieldType.Double),
            new Field("quantity", FieldType.Long),
            new Field("region", FieldType.String)
        ]);
        var rec = new GenericRecord(schema);
        rec.SetField("price", 9.99);
        rec.SetField("quantity", 3L);
        rec.SetField("region", "US");
        var rc = new RecordContent(schema, [rec]);
        var ff = FlowFile.CreateWithContent(rc, new());

        // Computed total + conditional shipping fee + uppercased region label
        var proc = new TransformRecord(
            "compute:total:price * quantity;" +
            "compute:shipping:if(total > 25, 0, 5);" +
            "compute:label:upper(region) + \"-\" + string(round(total))");
        var result = proc.Process(ff);
        var outFf = ((SingleResult)result).FlowFile;
        var outRc = (RecordContent)outFf.Content;
        var outRec = outRc.Records[0];

        var total = outRec.GetField("total");
        AssertTrue("total is double type", total is double);
        AssertTrue("total value", total is double d && Math.Abs(d - 29.97) < 0.001);

        var shipping = outRec.GetField("shipping");
        AssertTrue("shipping is long type", shipping is long);
        AssertTrue("shipping = 0 (over threshold)", shipping is long s && s == 0);

        AssertEqual("label string", outRec.GetField("label")?.ToString() ?? "", "US-30");

        // Schema should reflect inferred types
        var schemaByName = outRc.Schema.Fields.ToDictionary(f => f.Name, f => f.FieldType);
        AssertTrue("schema total = Double", schemaByName.GetValueOrDefault("total") == FieldType.Double);
        AssertTrue("schema shipping = Long", schemaByName.GetValueOrDefault("shipping") == FieldType.Long);
        AssertTrue("schema label = String", schemaByName.GetValueOrDefault("label") == FieldType.String);
    }

    static void TestTransformRecordPreservesTypes()
    {
        Console.WriteLine("--- TransformRecord: preserves original field types ---");
        var schema = new Schema("user", [
            new Field("id", FieldType.Long),
            new Field("name", FieldType.String),
            new Field("age", FieldType.Int),
            new Field("balance", FieldType.Double),
            new Field("active", FieldType.Boolean)
        ]);
        var rec = new GenericRecord(schema);
        rec.SetField("id", 12345L);
        rec.SetField("name", "alice");
        rec.SetField("age", 30);
        rec.SetField("balance", 1500.50);
        rec.SetField("active", true);
        var ff = FlowFile.CreateWithContent(new RecordContent(schema, [rec]), new());

        // toUpper changes a String — schema should stay String. Untouched fields keep types.
        var proc = new TransformRecord("toUpper:name");
        var outRc = (RecordContent)((SingleResult)proc.Process(ff)).FlowFile.Content;
        var byName = outRc.Schema.Fields.ToDictionary(f => f.Name, f => f.FieldType);
        AssertTrue("id stays Long", byName["id"] == FieldType.Long);
        AssertTrue("age stays Int", byName["age"] == FieldType.Int);
        AssertTrue("balance stays Double", byName["balance"] == FieldType.Double);
        AssertTrue("active stays Boolean", byName["active"] == FieldType.Boolean);
        AssertTrue("name stays String", byName["name"] == FieldType.String);

        // Verify values too
        var outRec = outRc.Records[0];
        AssertTrue("id value preserved", outRec.GetField("id") is long l && l == 12345L);
        AssertTrue("age value preserved", outRec.GetField("age") is int i && i == 30);
        AssertTrue("balance value preserved", outRec.GetField("balance") is double b && Math.Abs(b - 1500.50) < 0.001);
        AssertTrue("active value preserved", outRec.GetField("active") is true);
    }

    static void TestTransformRecordChainedCompute()
    {
        Console.WriteLine("--- TransformRecord: chained compute sees prior writes ---");
        var schema = new Schema("calc", [new Field("x", FieldType.Long)]);
        var rec = new GenericRecord(schema);
        rec.SetField("x", 10L);
        var ff = FlowFile.CreateWithContent(new RecordContent(schema, [rec]), new());

        // Each compute should see the prior step's writes via DictValueResolver.
        var proc = new TransformRecord(
            "compute:doubled:x * 2;" +
            "compute:plus_five:doubled + 5;" +
            "compute:final:plus_five * plus_five");
        var outRec = ((RecordContent)((SingleResult)proc.Process(ff)).FlowFile.Content).Records[0];
        AssertTrue("doubled = 20", outRec.GetField("doubled") is long d && d == 20);
        AssertTrue("plus_five = 25", outRec.GetField("plus_five") is long p && p == 25);
        AssertTrue("final = 625", outRec.GetField("final") is long f && f == 625);
    }

    static void TestSchemaEvolutionPromotion()
    {
        Console.WriteLine("--- Schema evolution: type promotion ---");
        var writer = new Schema("rec", [
            new Field("count", FieldType.Int),
            new Field("amount", FieldType.Long),
            new Field("rate", FieldType.Float),
            new Field("name", FieldType.String)
        ]);
        var reader = new Schema("rec", [
            new Field("count", FieldType.Long),       // int → long
            new Field("amount", FieldType.Double),    // long → double
            new Field("rate", FieldType.Double),      // float → double
            new Field("name", FieldType.Bytes)        // string → bytes
        ]);
        var compat = SchemaResolver.Check(reader, writer);
        AssertTrue("promotion compat", compat.IsCompatible);

        var rec = new GenericRecord(writer);
        rec.SetField("count", 42);
        rec.SetField("amount", 1000L);
        rec.SetField("rate", 1.5f);
        rec.SetField("name", "Alice");

        var projected = SchemaResolver.Project(rec, reader, writer);
        AssertTrue("count promoted to long", projected.GetField("count") is long l && l == 42L);
        AssertTrue("amount promoted to double", projected.GetField("amount") is double d && Math.Abs(d - 1000.0) < 0.001);
        AssertTrue("rate promoted to double", projected.GetField("rate") is double dr && Math.Abs(dr - 1.5) < 0.001);
        AssertTrue("name promoted to bytes", projected.GetField("name") is byte[] b && System.Text.Encoding.UTF8.GetString(b) == "Alice");
    }

    static void TestSchemaEvolutionFieldAddedWithDefault()
    {
        Console.WriteLine("--- Schema evolution: reader adds field with default ---");
        var writer = new Schema("rec", [new Field("name", FieldType.String)]);
        var reader = new Schema("rec", [
            new Field("name", FieldType.String),
            new Field("status", FieldType.String, defaultValue: "active")
        ]);
        var compat = SchemaResolver.Check(reader, writer);
        AssertTrue("add-with-default compat", compat.IsCompatible);
        AssertTrue("warning for missing-from-writer", compat.Warnings.Any(w => w.Contains("status") && w.Contains("default")));

        var rec = new GenericRecord(writer);
        rec.SetField("name", "Bob");
        var projected = SchemaResolver.Project(rec, reader, writer);
        AssertEqual("name preserved", projected.GetField("name")?.ToString() ?? "", "Bob");
        AssertEqual("status defaulted", projected.GetField("status")?.ToString() ?? "", "active");
    }

    static void TestSchemaEvolutionFieldAddedNoDefault()
    {
        Console.WriteLine("--- Schema evolution: reader adds field without default (error) ---");
        var writer = new Schema("rec", [new Field("name", FieldType.String)]);
        var reader = new Schema("rec", [
            new Field("name", FieldType.String),
            new Field("required_field", FieldType.Long)
        ]);
        var compat = SchemaResolver.Check(reader, writer);
        AssertFalse("add-without-default not compat", compat.IsCompatible);
        AssertTrue("error mentions required_field", compat.Errors.Any(e => e.Contains("required_field")));
    }

    static void TestSchemaEvolutionFieldRemoved()
    {
        Console.WriteLine("--- Schema evolution: reader drops a writer field ---");
        var writer = new Schema("rec", [
            new Field("name", FieldType.String),
            new Field("dropped", FieldType.Long)
        ]);
        var reader = new Schema("rec", [new Field("name", FieldType.String)]);
        var compat = SchemaResolver.Check(reader, writer);
        AssertTrue("drop is compat", compat.IsCompatible);
        AssertTrue("warning for dropped field", compat.Warnings.Any(w => w.Contains("dropped") && w.Contains("writer-only")));

        var rec = new GenericRecord(writer);
        rec.SetField("name", "Carol");
        rec.SetField("dropped", 999L);
        var projected = SchemaResolver.Project(rec, reader, writer);
        AssertEqual("name preserved", projected.GetField("name")?.ToString() ?? "", "Carol");
        AssertTrue("dropped not in target", projected.GetField("dropped") is null);
    }

    static void TestSchemaEvolutionIncompatible()
    {
        Console.WriteLine("--- Schema evolution: incompatible promotion (error) ---");
        var writer = new Schema("rec", [new Field("amount", FieldType.Double)]);
        var reader = new Schema("rec", [new Field("amount", FieldType.Int)]); // double → int not allowed
        var compat = SchemaResolver.Check(reader, writer);
        AssertFalse("double→int not compat", compat.IsCompatible);
    }

    static void TestOCFRoundtripWithEvolution()
    {
        Console.WriteLine("--- OCF: write with v1, read with v2 (evolved) ---");
        // V1: writer schema with name+age (int)
        var v1 = new Schema("user", [
            new Field("name", FieldType.String),
            new Field("age", FieldType.Int)
        ]);
        var rec = new GenericRecord(v1);
        rec.SetField("name", "Dan");
        rec.SetField("age", 25);
        var bytes = new OCFWriter().Write([rec], v1);

        // V2: drop age, promote nothing, add status with default
        var v2 = new Schema("user", [
            new Field("name", FieldType.String),
            new Field("status", FieldType.String, defaultValue: "unknown")
        ]);
        var (decodedSchema, decoded) = new OCFReader().Read(bytes, readerSchema: v2);
        AssertTrue("returned schema is reader", decodedSchema == v2);
        AssertEqual("name preserved through evolution", decoded[0].GetField("name")?.ToString() ?? "", "Dan");
        AssertEqual("status from default", decoded[0].GetField("status")?.ToString() ?? "", "unknown");
        AssertTrue("age dropped", decoded[0].GetField("age") is null);

        // V3: promote int → long
        var v3 = new Schema("user", [
            new Field("name", FieldType.String),
            new Field("age", FieldType.Long)
        ]);
        var (_, decodedV3) = new OCFReader().Read(bytes, readerSchema: v3);
        AssertTrue("age promoted to long", decodedV3[0].GetField("age") is long la && la == 25L);

        // V4: incompatible reader (requires non-default field) → throws
        var v4 = new Schema("user", [
            new Field("name", FieldType.String),
            new Field("must_have", FieldType.Long)
        ]);
        var threw = false;
        try { new OCFReader().Read(bytes, readerSchema: v4); }
        catch (InvalidOperationException) { threw = true; }
        AssertTrue("incompatible reader throws", threw);
    }

    static GenericRecord BuildNestedRecord()
    {
        // Outer: { user: { profile: { name: "Alice", age: 30 }, country: "US" }, score: 99.5 }
        var profileSchema = new Schema("profile", [
            new Field("name", FieldType.String),
            new Field("age", FieldType.Int)
        ]);
        var userSchema = new Schema("user", [
            new Field("profile", FieldType.Record),
            new Field("country", FieldType.String)
        ]);
        var rootSchema = new Schema("event", [
            new Field("user", FieldType.Record),
            new Field("score", FieldType.Double)
        ]);
        var profile = new GenericRecord(profileSchema);
        profile.SetField("name", "Alice");
        profile.SetField("age", 30);
        var user = new GenericRecord(userSchema);
        user.SetField("profile", profile);
        user.SetField("country", "US");
        var root = new GenericRecord(rootSchema);
        root.SetField("user", user);
        root.SetField("score", 99.5);
        return root;
    }

    static void TestNestedFieldPathsRead()
    {
        Console.WriteLine("--- Nested paths: GetByPath ---");
        var rec = BuildNestedRecord();
        AssertEqual("name via path", RecordHelpers.GetByPath(rec, "user.profile.name")?.ToString() ?? "", "Alice");
        AssertTrue("age via path", RecordHelpers.GetByPath(rec, "user.profile.age") is int a && a == 30);
        AssertEqual("country via path", RecordHelpers.GetByPath(rec, "user.country")?.ToString() ?? "", "US");
        AssertTrue("top-level score", RecordHelpers.GetByPath(rec, "score") is double s && Math.Abs(s - 99.5) < 0.001);
        AssertTrue("missing path returns null", RecordHelpers.GetByPath(rec, "user.profile.missing") is null);
        AssertTrue("missing intermediate returns null", RecordHelpers.GetByPath(rec, "nope.foo.bar") is null);
        AssertTrue("dotted leaf into primitive returns null", RecordHelpers.GetByPath(rec, "score.foo") is null);
    }

    static void TestNestedPathsInQueryRecord()
    {
        Console.WriteLine("--- QueryRecord: nested-path predicate ---");
        var schema = new Schema("event", [new Field("user", FieldType.Record), new Field("score", FieldType.Double)]);
        var records = new List<GenericRecord>();
        for (int i = 0; i < 3; i++)
        {
            var rec = BuildNestedRecord();
            // Override country and score for each
            ((GenericRecord)rec.GetField("user")!).SetField("country", i == 1 ? "CA" : "US");
            rec.SetField("score", 50.0 + i * 25);
            records.Add(rec);
        }
        var ff = FlowFile.CreateWithContent(new RecordContent(schema, records), new());

        var q = new QueryRecord("user.country = US");
        var result = q.Process(ff);
        AssertTrue("query returned single result", result is SingleResult);
        var filtered = ((RecordContent)((SingleResult)result).FlowFile.Content).Records;
        AssertIntEqual("filtered count = 2 (US records)", filtered.Count, 2);
    }

    static void TestNestedPathsInExtractRecordField()
    {
        Console.WriteLine("--- ExtractRecordField: nested paths to attributes ---");
        var schema = new Schema("event", [new Field("user", FieldType.Record), new Field("score", FieldType.Double)]);
        var ff = FlowFile.CreateWithContent(new RecordContent(schema, [BuildNestedRecord()]), new());

        var ex = new ExtractRecordField("user.profile.name:customer_name;user.country:country_code;score:score");
        var outFf = ((SingleResult)ex.Process(ff)).FlowFile;
        outFf.Attributes.TryGetValue("customer_name", out var name);
        outFf.Attributes.TryGetValue("country_code", out var cc);
        outFf.Attributes.TryGetValue("score", out var sc);
        AssertEqual("nested name extracted", name ?? "", "Alice");
        AssertEqual("nested country extracted", cc ?? "", "US");
        AssertEqual("top-level score extracted", sc ?? "", "99.5");
    }

    static void TestNestedPathsInComputeExpression()
    {
        Console.WriteLine("--- TransformRecord compute: nested-path reads ---");
        var schema = new Schema("event", [new Field("user", FieldType.Record), new Field("score", FieldType.Double)]);
        var ff = FlowFile.CreateWithContent(new RecordContent(schema, [BuildNestedRecord()]), new());

        var proc = new TransformRecord(
            "compute:greeting:concat(\"Hi \", user.profile.name);" +
            "compute:adjusted_score:score * if(user.country == \"US\", 1.0, 0.9);" +
            "compute:summary:user.profile.name + \"-\" + user.country");
        var outRec = ((RecordContent)((SingleResult)proc.Process(ff)).FlowFile.Content).Records[0];

        AssertEqual("greeting from nested", outRec.GetField("greeting")?.ToString() ?? "", "Hi Alice");
        AssertTrue("adjusted_score US bonus", outRec.GetField("adjusted_score") is double s && Math.Abs(s - 99.5) < 0.001);
        AssertEqual("summary string", outRec.GetField("summary")?.ToString() ?? "", "Alice-US");
    }

    static void TestSetByPathCreatesIntermediates()
    {
        Console.WriteLine("--- RecordHelpers: SetByPath creates missing intermediates ---");
        var schema = new Schema("event", [new Field("score", FieldType.Double)]);
        var rec = new GenericRecord(schema);
        rec.SetField("score", 10.0);

        var ok = RecordHelpers.SetByPath(rec, "user.profile.name", "Bob");
        AssertTrue("set ok", ok);
        AssertEqual("readback nested set", RecordHelpers.GetByPath(rec, "user.profile.name")?.ToString() ?? "", "Bob");

        // Top-level set still works
        RecordHelpers.SetByPath(rec, "score", 42.0);
        AssertTrue("top-level set", rec.GetField("score") is double d && Math.Abs(d - 42.0) < 0.001);
    }

    static void TestOCFProcessorRoundtrip()
    {
        Console.WriteLine("--- OCF processors: JSON -> Record -> OCF -> Record -> JSON ---");
        var store = new MemoryContentStore();
        var json = "[{\"city\":\"Boston\",\"pop\":675000},{\"city\":\"Portland\",\"pop\":650000}]";
        var ff = FlowFile.Create(Encoding.UTF8.GetBytes(json), new());

        var r1 = (SingleResult)new ConvertJSONToRecord("cities", store).Process(ff);
        var r2 = (SingleResult)new ConvertRecordToOCF(AvroOCF.CodecNull).Process(r1.FlowFile);
        AssertTrue("ocf raw content", r2.FlowFile.Content is Raw);
        var (ocfBytes, _) = ContentHelpers.Resolve(store, r2.FlowFile.Content);
        AssertTrue("ocf has magic", ocfBytes.Length > 4 && ocfBytes[0] == 0x4F);

        var r3 = (SingleResult)new ConvertOCFToRecord(store).Process(r2.FlowFile);
        var rc = (RecordContent)r3.FlowFile.Content;
        AssertIntEqual("round-trip record count", rc.Records.Count, 2);
        AssertEqual("record 0 city", rc.Records[0].GetField("city")?.ToString() ?? "", "Boston");

        var r4 = (SingleResult)new ConvertRecordToJSON().Process(r3.FlowFile);
        var (jsonBytes, _) = ContentHelpers.Resolve(store, r4.FlowFile.Content);
        var outJson = Encoding.UTF8.GetString(jsonBytes);
        AssertTrue("final json has Boston", outJson.Contains("Boston"));
        AssertTrue("final json has Portland", outJson.Contains("Portland"));
    }
}
