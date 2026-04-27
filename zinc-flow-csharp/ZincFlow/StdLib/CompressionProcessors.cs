using System.IO.Compression;
using ZincFlow.Core;

namespace ZincFlow.StdLib;

/// <summary>
/// CompressContent: gzip or zstd compression of FlowFile content.
/// Stamps <c>compression.algorithm</c> + <c>compression.originalSize</c>
/// attributes so DecompressContent can reverse without config.
/// Bandwidth reduction matters a lot at the edge — this is why it's in
/// the "core runtime" catalog despite being so small.
/// </summary>
public sealed class CompressContent : IProcessor
{
    private readonly string _algorithm;
    private readonly string _level;
    private readonly IContentStore _store;

    public CompressContent(string algorithm, string level, IContentStore store)
    {
        _algorithm = algorithm;
        _level = level;
        _store = store;
    }

    public ProcessorResult Process(FlowFile ff)
    {
        var (data, error) = ContentHelpers.Resolve(_store, ff.Content);
        if (error != "") return FailureResult.Rent(error, ff);

        byte[] compressed;
        try
        {
            compressed = _algorithm switch
            {
                "gzip" => CompressionCodec.GzipCompress(data, _level),
                "zstd" => CompressionCodec.ZstdCompress(data, _level),
                _ => throw new ConfigException($"CompressContent: unknown algorithm '{_algorithm}' — valid: gzip, zstd"),
            };
        }
        catch (ConfigException) { throw; }
        catch (Exception ex)
        {
            return FailureResult.Rent($"compress failed: {ex.Message}", ff);
        }

        var withContent = FlowFile.WithContent(ff, Raw.Rent(compressed));
        var withAlg = FlowFile.WithAttribute(withContent, "compression.algorithm", _algorithm);
        var stamped = FlowFile.WithAttribute(withAlg, "compression.originalSize", data.Length.ToString());
        return SingleResult.Rent(stamped);
    }
}

/// <summary>
/// DecompressContent: inverse of CompressContent. When
/// <c>algorithm = "auto"</c>, reads the <c>compression.algorithm</c>
/// attribute to decide — which is what the rest of the flow stamps when
/// CompressContent runs, so an auto-decoder sits naturally in front of
/// any processor that can't see compressed bytes.
/// </summary>
public sealed class DecompressContent : IProcessor
{
    private readonly string _algorithm;
    private readonly IContentStore _store;

    public DecompressContent(string algorithm, IContentStore store)
    {
        _algorithm = algorithm;
        _store = store;
    }

    public ProcessorResult Process(FlowFile ff)
    {
        var (data, error) = ContentHelpers.Resolve(_store, ff.Content);
        if (error != "") return FailureResult.Rent(error, ff);

        var alg = _algorithm;
        if (alg == "auto")
        {
            if (!ff.Attributes.TryGetValue("compression.algorithm", out var attrAlg) || string.IsNullOrEmpty(attrAlg))
                return FailureResult.Rent("DecompressContent: algorithm=auto but compression.algorithm attribute missing", ff);
            alg = attrAlg;
        }

        byte[] decompressed;
        try
        {
            decompressed = alg switch
            {
                "gzip" => CompressionCodec.GzipDecompress(data),
                "zstd" => CompressionCodec.ZstdDecompress(data),
                _ => throw new ConfigException($"DecompressContent: unknown algorithm '{alg}' — valid: gzip, zstd"),
            };
        }
        catch (ConfigException) { throw; }
        catch (Exception ex)
        {
            return FailureResult.Rent($"decompress failed: {ex.Message}", ff);
        }

        // Strip compression attributes so downstream processors that
        // look at compression.algorithm don't try to decode twice.
        var withContent = FlowFile.WithContent(ff, Raw.Rent(decompressed));
        var filtered = new AttributeFilter("remove", "compression.algorithm;compression.originalSize");
        var filteredResult = filtered.Process(withContent);
        return filteredResult is SingleResult sr ? sr : SingleResult.Rent(withContent);
    }
}

internal static class CompressionCodec
{
    public static byte[] GzipCompress(byte[] data, string level)
    {
        var cl = level switch
        {
            "fastest" => CompressionLevel.Fastest,
            "balanced" => CompressionLevel.Optimal,
            "smallest" => CompressionLevel.SmallestSize,
            _ => CompressionLevel.Optimal,
        };
        using var ms = new MemoryStream();
        using (var gz = new GZipStream(ms, cl, leaveOpen: true))
            gz.Write(data, 0, data.Length);
        return ms.ToArray();
    }

    public static byte[] GzipDecompress(byte[] data)
    {
        using var input = new MemoryStream(data);
        using var gz = new GZipStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        gz.CopyTo(output);
        return output.ToArray();
    }

    public static byte[] ZstdCompress(byte[] data, string level)
    {
        var numeric = level switch
        {
            "fastest" => 1,
            "balanced" => 3,
            "smallest" => 19,
            _ => 3,
        };
        using var c = new ZstdSharp.Compressor(numeric);
        return c.Wrap(data).ToArray();
    }

    public static byte[] ZstdDecompress(byte[] data)
    {
        using var d = new ZstdSharp.Decompressor();
        return d.Unwrap(data).ToArray();
    }
}

/// <summary>
/// Tiny internal helper used by DecompressContent to strip the
/// compression.* attributes from the outgoing FlowFile. Reuses the
/// FilterAttribute processor directly rather than re-implementing
/// attribute removal logic here.
/// </summary>
file sealed class AttributeFilter : IProcessor
{
    private readonly FilterAttribute _inner;
    public AttributeFilter(string mode, string attributes) => _inner = new FilterAttribute(mode, attributes);
    public ProcessorResult Process(FlowFile ff) => _inner.Process(ff);
}
