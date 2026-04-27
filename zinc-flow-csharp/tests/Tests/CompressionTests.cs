using System.Text;
using ZincFlow.Core;
using ZincFlow.StdLib;
using static ZincFlow.Tests.TestRunner;
using static ZincFlow.Tests.Helpers;

namespace ZincFlow.Tests;

public static class CompressionTests
{
    public static void RunAll()
    {
        TestGzipRoundtrip();
        TestZstdRoundtrip();
        TestAutoDecompressUsesAttribute();
        TestDecompressRejectsMissingAttribute();
        TestCompressionAttributesStamped();
        TestGzipMagicBytes();
        TestCompressedSmallerOnCompressibleInput();
    }

    private static IContentStore Store() => new MemoryContentStore();

    private static byte[] Payload() =>
        // Repetitive text compresses well — lets us assert "smaller than input".
        Encoding.UTF8.GetBytes(string.Concat(Enumerable.Repeat("zinc-flow rocks on the edge. ", 64)));

    static void TestGzipRoundtrip()
    {
        Console.WriteLine("--- Compression: gzip roundtrip ---");
        var original = Payload();
        var compress = new CompressContent("gzip", "balanced", Store());
        var result = compress.Process(FlowFile.Create(original, new()));
        AssertTrue("compress returns Single", result is SingleResult);
        var compressedFf = ((SingleResult)result).FlowFile;
        var compressed = ((Raw)compressedFf.Content).Data.ToArray();

        var decompress = new DecompressContent("gzip", Store());
        var rtResult = decompress.Process(FlowFile.Create(compressed, new()));
        var restored = ((Raw)((SingleResult)rtResult).FlowFile.Content).Data.ToArray();
        AssertTrue("roundtrip bytes identical", restored.SequenceEqual(original));
    }

    static void TestZstdRoundtrip()
    {
        Console.WriteLine("--- Compression: zstd roundtrip ---");
        var original = Payload();
        var compress = new CompressContent("zstd", "balanced", Store());
        var result = compress.Process(FlowFile.Create(original, new()));
        var compressedFf = ((SingleResult)result).FlowFile;
        var compressed = ((Raw)compressedFf.Content).Data.ToArray();

        var decompress = new DecompressContent("zstd", Store());
        var rt = decompress.Process(FlowFile.Create(compressed, new()));
        var restored = ((Raw)((SingleResult)rt).FlowFile.Content).Data.ToArray();
        AssertTrue("zstd roundtrip identical", restored.SequenceEqual(original));
    }

    static void TestAutoDecompressUsesAttribute()
    {
        Console.WriteLine("--- Compression: DecompressContent(auto) reads compression.algorithm ---");
        var original = Payload();
        var compress = new CompressContent("zstd", "balanced", Store());
        var compressedFf = ((SingleResult)compress.Process(FlowFile.Create(original, new()))).FlowFile;

        // compressedFf already carries compression.algorithm=zstd — pass to auto-decoder directly.
        var decompress = new DecompressContent("auto", Store());
        var result = decompress.Process(compressedFf);
        AssertTrue("auto returns Single", result is SingleResult);
        var restored = ((Raw)((SingleResult)result).FlowFile.Content).Data.ToArray();
        AssertTrue("auto-decoded bytes identical", restored.SequenceEqual(original));
    }

    static void TestDecompressRejectsMissingAttribute()
    {
        Console.WriteLine("--- Compression: auto without attribute → FailureResult ---");
        var ff = FlowFile.Create(new byte[] { 1, 2, 3 }, new());
        var decompress = new DecompressContent("auto", Store());
        var result = decompress.Process(ff);
        AssertTrue("auto without attr → Failure", result is FailureResult);
        var reason = ((FailureResult)result).Reason;
        AssertTrue("failure mentions compression.algorithm", reason.Contains("compression.algorithm"));
    }

    static void TestCompressionAttributesStamped()
    {
        Console.WriteLine("--- Compression: outgoing FlowFile carries compression.* attributes ---");
        var original = Payload();
        var compress = new CompressContent("gzip", "fastest", Store());
        var ff = ((SingleResult)compress.Process(FlowFile.Create(original, new()))).FlowFile;
        AssertTrue("algorithm attribute set",
            ff.Attributes.TryGetValue("compression.algorithm", out var alg) && alg == "gzip");
        AssertTrue("originalSize attribute set",
            ff.Attributes.TryGetValue("compression.originalSize", out var sz) && sz == original.Length.ToString());

        // Decompressed FlowFile should have the attributes stripped so a
        // downstream auto-decoder doesn't try to decode again.
        var dec = new DecompressContent("auto", Store());
        var restored = ((SingleResult)dec.Process(ff)).FlowFile;
        AssertFalse("algorithm attribute stripped after decompress",
            restored.Attributes.TryGetValue("compression.algorithm", out _));
    }

    static void TestGzipMagicBytes()
    {
        Console.WriteLine("--- Compression: gzip output starts with 1f 8b magic ---");
        var compress = new CompressContent("gzip", "balanced", Store());
        var ff = ((SingleResult)compress.Process(FlowFile.Create("hello"u8.ToArray(), new()))).FlowFile;
        var bytes = ((Raw)ff.Content).Data.ToArray();
        AssertTrue("gzip magic byte 0x1f", bytes.Length >= 2 && bytes[0] == 0x1f);
        AssertTrue("gzip magic byte 0x8b", bytes.Length >= 2 && bytes[1] == 0x8b);
    }

    static void TestCompressedSmallerOnCompressibleInput()
    {
        Console.WriteLine("--- Compression: repetitive input compresses smaller ---");
        var original = Payload();
        foreach (var alg in new[] { "gzip", "zstd" })
        {
            var compress = new CompressContent(alg, "smallest", Store());
            var ff = ((SingleResult)compress.Process(FlowFile.Create(original, new()))).FlowFile;
            var compressed = ((Raw)ff.Content).Data.ToArray();
            AssertTrue($"{alg} produces smaller output (~2048 repeats)", compressed.Length < original.Length);
        }
    }
}
