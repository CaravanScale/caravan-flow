using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;
using ZincFlow.Core;

namespace ZincFlow.StdLib;

/// <summary>
/// Avro Object Container File (OCF) reader and writer — spec 1.11.
///
/// Format:
///   magic "Obj\x01"
///   metadata: Avro-encoded map&lt;string, bytes&gt; (must include avro.schema; avro.codec optional)
///   sync: 16 random bytes
///   blocks: { count: long, size: long, data (codec-encoded), sync: 16 bytes }*
///
/// Supported codecs: null, deflate. Snappy/zstd deferred.
/// Writes single-block files; reads arbitrary block counts.
/// </summary>
public static class AvroOCF
{
    public static readonly byte[] Magic = [0x4F, 0x62, 0x6A, 0x01]; // "Obj\x01"
    public const string CodecNull = "null";
    public const string CodecDeflate = "deflate";
    public const string CodecZstandard = "zstandard";

    internal static bool IsSupportedCodec(string codec)
        => codec == CodecNull || codec == CodecDeflate || codec == CodecZstandard;
}

public sealed class OCFReader
{
    /// <summary>
    /// Reads an OCF file. If <paramref name="readerSchema"/> is provided, decoded
    /// records are projected onto it (Avro schema-evolution semantics: type
    /// promotion, field add with default, field removal). Throws if the writer
    /// schema embedded in the file isn't compatible with the reader schema.
    /// When omitted, returns records under the writer schema as decoded.
    /// </summary>
    public (Schema Schema, List<GenericRecord> Records) Read(byte[] data, Schema? readerSchema = null)
    {
        if (data.Length < AvroOCF.Magic.Length + 16)
            throw new InvalidOperationException("OCF truncated: missing magic or sync");

        var span = data.AsSpan();
        int offset = 0;

        // Magic
        for (int i = 0; i < AvroOCF.Magic.Length; i++)
            if (span[offset + i] != AvroOCF.Magic[i])
                throw new InvalidOperationException("invalid Avro OCF magic");
        offset += AvroOCF.Magic.Length;

        // Metadata map
        var (metadata, metaBytes) = ReadMetadataMap(span[offset..]);
        offset += metaBytes;

        if (!metadata.TryGetValue("avro.schema", out var schemaBytes))
            throw new InvalidOperationException("OCF metadata missing avro.schema");
        var schemaJson = Encoding.UTF8.GetString(schemaBytes);
        var schema = AvroSchemaJson.Parse(schemaJson);

        string codec = AvroOCF.CodecNull;
        if (metadata.TryGetValue("avro.codec", out var codecBytes))
            codec = Encoding.UTF8.GetString(codecBytes);
        if (!AvroOCF.IsSupportedCodec(codec))
            throw new InvalidOperationException($"unsupported OCF codec: {codec}");

        // Sync marker
        if (offset + 16 > span.Length)
            throw new InvalidOperationException("OCF truncated before sync marker");
        var sync = span.Slice(offset, 16).ToArray();
        offset += 16;

        // Blocks
        var reader = new AvroBinaryReader();
        var records = new List<GenericRecord>();
        while (offset < span.Length)
        {
            var (count, nCount) = AvroEncoding.ReadVarint(span[offset..]);
            offset += nCount;
            var (size, nSize) = AvroEncoding.ReadVarint(span[offset..]);
            offset += nSize;

            if (size < 0 || offset + (int)size + 16 > span.Length)
                throw new InvalidOperationException("OCF block truncated");

            var blockData = span.Slice(offset, (int)size).ToArray();
            offset += (int)size;

            // Verify block sync
            for (int i = 0; i < 16; i++)
                if (span[offset + i] != sync[i])
                    throw new InvalidOperationException("OCF block sync mismatch");
            offset += 16;

            if (codec == AvroOCF.CodecDeflate)
                blockData = Inflate(blockData);
            else if (codec == AvroOCF.CodecZstandard)
                blockData = ZstdInflate(blockData);

            int blockOffset = 0;
            for (long i = 0; i < count; i++)
            {
                var (record, bytesRead) = reader.ReadRecord(blockData.AsSpan(blockOffset), schema);
                records.Add(record);
                blockOffset += bytesRead;
            }
        }

        if (readerSchema is not null)
        {
            var compat = SchemaResolver.Check(readerSchema, schema);
            if (!compat.IsCompatible)
                throw new InvalidOperationException("OCF reader schema incompatible: " + string.Join("; ", compat.Errors));
            var projected = new List<GenericRecord>(records.Count);
            foreach (var r in records)
                projected.Add(SchemaResolver.Project(r, readerSchema, schema));
            return (readerSchema, projected);
        }

        return (schema, records);
    }

    private static (Dictionary<string, byte[]> Map, int BytesRead) ReadMetadataMap(ReadOnlySpan<byte> data)
    {
        var map = new Dictionary<string, byte[]>();
        int offset = 0;
        while (true)
        {
            var (count, nCount) = AvroEncoding.ReadVarint(data[offset..]);
            offset += nCount;
            if (count == 0) break;

            // Negative count means a size prefix follows — we can skip reading it and just iterate |count| entries.
            if (count < 0)
            {
                var (_, nSize) = AvroEncoding.ReadVarint(data[offset..]);
                offset += nSize;
                count = -count;
            }

            for (long i = 0; i < count; i++)
            {
                var (key, nKey) = AvroEncoding.ReadString(data[offset..]);
                offset += nKey;
                var (val, nVal) = AvroEncoding.ReadBytes(data[offset..]);
                offset += nVal;
                map[key] = val;
            }
        }
        return (map, offset);
    }

    private static byte[] Inflate(byte[] compressed)
    {
        using var input = new MemoryStream(compressed);
        using var deflate = new DeflateStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        deflate.CopyTo(output);
        return output.ToArray();
    }

    private static byte[] ZstdInflate(byte[] compressed)
    {
        using var decompressor = new ZstdSharp.Decompressor();
        // Decompress in one shot — Decompressor.Unwrap allocates the right size.
        return decompressor.Unwrap(compressed).ToArray();
    }
}

public sealed class OCFWriter
{
    private readonly string _codec;

    public OCFWriter(string codec = AvroOCF.CodecNull)
    {
        if (!AvroOCF.IsSupportedCodec(codec))
            throw new ArgumentException($"unsupported OCF codec: {codec}");
        _codec = codec;
    }

    public byte[] Write(List<GenericRecord> records, Schema schema)
    {
        // Serialize all records to raw Avro binary
        var recordWriter = new AvroBinaryWriter();
        using var block = new MemoryStream();
        foreach (var r in records)
            recordWriter.WriteRecord(block, r, schema);
        var blockBytes = block.ToArray();

        if (_codec == AvroOCF.CodecDeflate)
            blockBytes = Deflate(blockBytes);
        else if (_codec == AvroOCF.CodecZstandard)
            blockBytes = ZstdDeflate(blockBytes);

        // Sync marker — 16 random bytes
        var sync = new byte[16];
        RandomNumberGenerator.Fill(sync);

        using var output = new MemoryStream();

        // Magic
        output.Write(AvroOCF.Magic);

        // Metadata map: { avro.schema, avro.codec }
        WriteMetadataMap(output, new Dictionary<string, byte[]>
        {
            ["avro.schema"] = Encoding.UTF8.GetBytes(AvroSchemaJson.Emit(schema)),
            ["avro.codec"] = Encoding.UTF8.GetBytes(_codec)
        });

        // Sync marker
        output.Write(sync);

        // Single block: count, size, data, sync
        AvroEncoding.WriteVarint(output, records.Count);
        AvroEncoding.WriteVarint(output, blockBytes.Length);
        output.Write(blockBytes);
        output.Write(sync);

        return output.ToArray();
    }

    private static void WriteMetadataMap(MemoryStream output, Dictionary<string, byte[]> map)
    {
        AvroEncoding.WriteVarint(output, map.Count);
        foreach (var (key, val) in map)
        {
            AvroEncoding.WriteString(output, key);
            AvroEncoding.WriteVarint(output, val.Length);
            output.Write(val);
        }
        AvroEncoding.WriteVarint(output, 0); // Map terminator
    }

    private static byte[] Deflate(byte[] raw)
    {
        using var output = new MemoryStream();
        using (var deflate = new DeflateStream(output, CompressionLevel.Fastest, leaveOpen: true))
            deflate.Write(raw, 0, raw.Length);
        return output.ToArray();
    }

    private static byte[] ZstdDeflate(byte[] raw)
    {
        // Default compression level (3) — Avro OCF spec doesn't pin a level; this matches
        // most reference implementations.
        using var compressor = new ZstdSharp.Compressor();
        return compressor.Wrap(raw).ToArray();
    }
}
