using System.Text;
using ZincFlow.Core;
using ZincFlow.Fabric;
using ZincFlow.StdLib;
using static ZincFlow.Tests.TestRunner;

namespace ZincFlow.Tests;

/// <summary>
/// Tests covering the FlowFile V3 boundary surface: file-source V3 sniff,
/// file/stdout V3 egress, in-pipeline Package/Unpackage processors, and CSV
/// typed reads. These are the pieces that turn "ingest a NiFi FlowFile, do
/// stuff, emit a NiFi FlowFile" into a real promise.
/// </summary>
public static class FlowFileV3BoundaryTests
{
    public static void RunAll()
    {
        TestPackageFlowFileV3Roundtrip();
        TestUnpackageFlowFileV3MultipleFrames();
        TestUnpackageFlowFileV3RejectsNonV3();
        TestPutFileV3Mode();
        TestPutFileRawMode();
        TestPutStdoutV3DoesNotThrow();
        TestGetFileSniffsV3();
        TestGetFileLeavesNonV3RawAlone();
        TestCsvFieldsConfigCoercesTypes();
        TestCsvNoFieldsStaysString();
        TestEndToEndV3FilePipelineRoundtrip();
    }

    private static IContentStore TempStore() => new MemoryContentStore();

    static void TestPackageFlowFileV3Roundtrip()
    {
        Console.WriteLine("--- V3 boundary: PackageFlowFileV3 → UnpackageFlowFileV3 round-trip ---");
        var store = TempStore();
        var attrs = new Dictionary<string, string> { ["filename"] = "data.json", ["env"] = "prod" };
        var ff = FlowFile.Create("hello world"u8.ToArray(), attrs);

        var pkgResult = (SingleResult)new PackageFlowFileV3(store).Process(ff);
        AssertTrue("packaged content is Raw", pkgResult.FlowFile.Content is Raw);
        AssertTrue("v3.packaged attribute set", pkgResult.FlowFile.Attributes.TryGetValue("v3.packaged", out var p) && p == "true");
        AssertTrue("http.content.type set to flowfile-v3",
            pkgResult.FlowFile.Attributes.TryGetValue("http.content.type", out var ct) && ct == "application/flowfile-v3");

        var unpkgResult = new UnpackageFlowFileV3(store).Process(pkgResult.FlowFile);
        AssertTrue("unpackage produced single result", unpkgResult is SingleResult);
        var restored = ((SingleResult)unpkgResult).FlowFile;
        AssertTrue("filename attribute restored",
            restored.Attributes.TryGetValue("filename", out var fn) && fn == "data.json");
        AssertTrue("env attribute restored",
            restored.Attributes.TryGetValue("env", out var env) && env == "prod");
        var (data, _) = ContentHelpers.Resolve(store, restored.Content);
        AssertEqual("content restored", Encoding.UTF8.GetString(data), "hello world");
    }

    static void TestUnpackageFlowFileV3MultipleFrames()
    {
        Console.WriteLine("--- V3 boundary: Unpackage emits MultipleResult for multi-frame streams ---");
        // Build two FlowFiles and pack as a single V3 stream
        var ff1 = FlowFile.Create("first"u8.ToArray(), new() { ["seq"] = "1" });
        var ff2 = FlowFile.Create("second"u8.ToArray(), new() { ["seq"] = "2" });
        var packed = FlowFileV3.PackMultiple(
            new List<FlowFile> { ff1, ff2 },
            new List<byte[]> { "first"u8.ToArray(), "second"u8.ToArray() });

        var store = TempStore();
        var carrier = FlowFile.Create(packed, new());
        var result = new UnpackageFlowFileV3(store).Process(carrier);
        AssertTrue("multi-frame unpack returns MultipleResult", result is MultipleResult);
        var multi = (MultipleResult)result;
        AssertIntEqual("two FlowFiles emitted", multi.FlowFiles.Count, 2);
        AssertTrue("first seq", multi.FlowFiles[0].Attributes.TryGetValue("seq", out var s1) && s1 == "1");
        AssertTrue("second seq", multi.FlowFiles[1].Attributes.TryGetValue("seq", out var s2) && s2 == "2");
    }

    static void TestUnpackageFlowFileV3RejectsNonV3()
    {
        Console.WriteLine("--- V3 boundary: Unpackage on non-V3 content emits Failure ---");
        var ff = FlowFile.Create("just some plain bytes"u8.ToArray(), new());
        var result = new UnpackageFlowFileV3(TempStore()).Process(ff);
        AssertTrue("non-V3 input → FailureResult", result is FailureResult);
    }

    static void TestPutFileV3Mode()
    {
        Console.WriteLine("--- V3 boundary: PutFile format=v3 writes V3-framed files ---");
        var tmp = Path.Combine(Path.GetTempPath(), $"zinc-v3-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tmp);
        try
        {
            var ff = FlowFile.Create("payload"u8.ToArray(),
                new() { ["filename"] = "out.bin", ["custom"] = "value" });
            var put = new PutFile(tmp, "filename", "", ".bin", TempStore(), format: "v3");
            put.Process(ff);

            var path = Path.Combine(tmp, "out.bin");
            AssertTrue("file written", File.Exists(path));
            var bytes = File.ReadAllBytes(path);
            // Verify magic header
            AssertTrue("file starts with V3 magic",
                bytes.Length >= 7 && Encoding.UTF8.GetString(bytes, 0, 7) == "NiFiFF3");

            // Read it back: GetFile should auto-unpack
            var input = Path.Combine(tmp, "input");
            Directory.CreateDirectory(input);
            File.Copy(path, Path.Combine(input, "out.bin"));
            var get = new GetFile("test", input, "*", 1000, TempStore(), unpackV3: true);
            var polled = get.PollOnce();
            AssertTrue("poll returned a flowfile", polled is { Count: 1 });
            AssertTrue("custom attribute round-tripped through file",
                polled![0].Attributes.TryGetValue("custom", out var c) && c == "value");
        }
        finally
        {
            Directory.Delete(tmp, recursive: true);
        }
    }

    static void TestPutFileRawMode()
    {
        Console.WriteLine("--- V3 boundary: PutFile default mode still writes raw bytes (back-compat) ---");
        var tmp = Path.Combine(Path.GetTempPath(), $"zinc-raw-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tmp);
        try
        {
            var ff = FlowFile.Create("just a payload"u8.ToArray(), new() { ["filename"] = "out.txt" });
            var put = new PutFile(tmp, "filename", "", ".txt", TempStore());
            put.Process(ff);
            var bytes = File.ReadAllBytes(Path.Combine(tmp, "out.txt"));
            AssertEqual("raw mode wrote content as-is", Encoding.UTF8.GetString(bytes), "just a payload");
            AssertFalse("no V3 magic in raw output",
                bytes.Length >= 7 && Encoding.UTF8.GetString(bytes, 0, 7) == "NiFiFF3");
        }
        finally
        {
            Directory.Delete(tmp, recursive: true);
        }
    }

    static void TestPutStdoutV3DoesNotThrow()
    {
        Console.WriteLine("--- V3 boundary: PutStdout format=v3 succeeds ---");
        // Smoke test only — we redirect stdout to /dev/null-equivalent to avoid noise
        var sw = new StringWriter();
        var orig = Console.Out;
        Console.SetOut(sw);
        try
        {
            var ff = FlowFile.Create("hi"u8.ToArray(), new() { ["k"] = "v" });
            var result = new PutStdout("v3", TempStore()).Process(ff);
            AssertTrue("v3 stdout returns SingleResult", result is SingleResult);
            var captured = sw.ToString();
            AssertTrue("output mentions v3", captured.Contains("v3"));
        }
        finally
        {
            Console.SetOut(orig);
        }
    }

    static void TestGetFileSniffsV3()
    {
        Console.WriteLine("--- V3 boundary: GetFile auto-detects V3 magic and unwraps ---");
        var tmp = Path.Combine(Path.GetTempPath(), $"zinc-getfile-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tmp);
        try
        {
            // Drop a V3-framed file in the directory
            var ff = FlowFile.Create("inner"u8.ToArray(), new() { ["origin"] = "upstream" });
            var packed = FlowFileV3.Pack(ff, "inner"u8.ToArray());
            File.WriteAllBytes(Path.Combine(tmp, "wrapped.v3"), packed);

            var get = new GetFile("test", tmp, "*", 1000, TempStore(), unpackV3: true);
            var polled = get.PollOnce();
            AssertIntEqual("one flowfile unwrapped", polled.Count, 1);
            AssertTrue("origin attribute preserved",
                polled[0].Attributes.TryGetValue("origin", out var o) && o == "upstream");
            AssertTrue("filename layered on top",
                polled[0].Attributes.TryGetValue("filename", out var f) && f == "wrapped.v3");
            AssertTrue("v3.frame.index set",
                polled[0].Attributes.TryGetValue("v3.frame.index", out var idx) && idx == "0");
        }
        finally
        {
            Directory.Delete(tmp, recursive: true);
        }
    }

    static void TestGetFileLeavesNonV3RawAlone()
    {
        Console.WriteLine("--- V3 boundary: GetFile passes non-V3 files through as raw content ---");
        var tmp = Path.Combine(Path.GetTempPath(), $"zinc-raw-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tmp);
        try
        {
            File.WriteAllText(Path.Combine(tmp, "plain.json"), "{\"a\":1}");
            var get = new GetFile("test", tmp, "*", 1000, TempStore(), unpackV3: true);
            var polled = get.PollOnce();
            AssertIntEqual("one flowfile", polled.Count, 1);
            AssertTrue("no v3.frame attributes when raw",
                !polled[0].Attributes.TryGetValue("v3.frame.index", out _));
        }
        finally
        {
            Directory.Delete(tmp, recursive: true);
        }
    }

    static void TestCsvFieldsConfigCoercesTypes()
    {
        Console.WriteLine("--- V3 boundary: CSV `fields` config coerces cell types ---");
        var csv = "id,price,name\n1,9.99,Widget\n2,19.50,Gadget\n";
        var ff = FlowFile.Create(Encoding.UTF8.GetBytes(csv), new());
        var proc = new ConvertCSVToRecord("orders", ',', hasHeader: true, TempStore(),
            fieldDefs: "id:long,price:double,name:string");
        var result = (SingleResult)proc.Process(ff);
        var rc = (RecordContent)result.FlowFile.Content;
        AssertIntEqual("two rows parsed", rc.Records.Count, 2);

        // Schema reflects declared types
        var byName = rc.Schema.Fields.ToDictionary(f => f.Name, f => f.FieldType);
        AssertTrue("id is Long", byName["id"] == FieldType.Long);
        AssertTrue("price is Double", byName["price"] == FieldType.Double);
        AssertTrue("name is String", byName["name"] == FieldType.String);

        // Values are typed CLR values, not strings
        AssertTrue("id value is long", rc.Records[0].GetField("id") is long l && l == 1L);
        AssertTrue("price value is double", rc.Records[0].GetField("price") is double d && Math.Abs(d - 9.99) < 0.001);
        AssertEqual("name value", rc.Records[0].GetField("name")?.ToString() ?? "", "Widget");
    }

    static void TestCsvNoFieldsStaysString()
    {
        Console.WriteLine("--- V3 boundary: CSV without `fields` keeps all-string back-compat ---");
        var csv = "id,price\n42,9.99\n";
        var ff = FlowFile.Create(Encoding.UTF8.GetBytes(csv), new());
        var proc = new ConvertCSVToRecord("orders", ',', hasHeader: true, TempStore());
        var rc = (RecordContent)((SingleResult)proc.Process(ff)).FlowFile.Content;
        AssertTrue("id stays string when fields not declared", rc.Records[0].GetField("id") is string);
    }

    static void TestEndToEndV3FilePipelineRoundtrip()
    {
        Console.WriteLine("--- V3 boundary: end-to-end PutFile(v3) → GetFile(unpack) roundtrip ---");
        var tmp = Path.Combine(Path.GetTempPath(), $"zinc-e2e-{Guid.NewGuid():N}");
        var outDir = Path.Combine(tmp, "out");
        var inDir = Path.Combine(tmp, "in");
        Directory.CreateDirectory(outDir);
        Directory.CreateDirectory(inDir);
        try
        {
            // Stage 1: a FlowFile with rich attributes lands as a V3-framed file on disk.
            var attrs = new Dictionary<string, string>
            {
                ["filename"] = "evt.v3",
                ["batch.id"] = "abc-123",
                ["produced.at"] = "2026-04-13T18:00:00Z"
            };
            var content = "{\"event\":\"login\"}"u8.ToArray();
            var ff = FlowFile.Create(content, attrs);
            new PutFile(outDir, "filename", "", ".v3", TempStore(), format: "v3").Process(ff);

            // Stage 2: copy the file to a polled input dir and let GetFile auto-unpack.
            File.Copy(Path.Combine(outDir, "evt.v3"), Path.Combine(inDir, "evt.v3"));
            var get = new GetFile("inbox", inDir, "*", 1000, TempStore(), unpackV3: true);
            var polled = get.PollOnce();
            AssertIntEqual("end-to-end one flowfile", polled.Count, 1);

            // All original attributes survived a disk roundtrip.
            AssertTrue("batch.id preserved",
                polled[0].Attributes.TryGetValue("batch.id", out var b) && b == "abc-123");
            AssertTrue("produced.at preserved",
                polled[0].Attributes.TryGetValue("produced.at", out var t) && t == "2026-04-13T18:00:00Z");
            var (decoded, _) = ContentHelpers.Resolve(TempStore(), polled[0].Content);
            AssertEqual("content survived", Encoding.UTF8.GetString(decoded), "{\"event\":\"login\"}");
        }
        finally
        {
            Directory.Delete(tmp, recursive: true);
        }
    }
}
