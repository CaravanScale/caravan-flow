using System.Runtime.CompilerServices;
using System.Text;
using CaravanFlow.Core;

namespace CaravanFlow.StdLib;

/// <summary>
/// Avro binary encoding helpers: zigzag varint, IEEE 754 floats.
/// Follows the Apache Avro 1.11 binary encoding specification.
/// Hand-rolled, zero reflection, fully AOT-safe.
/// </summary>
public static class AvroEncoding
{
    // --- Zigzag encode/decode (maps signed ints to unsigned for efficient varint) ---

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long ZigzagEncode(long n) => (n << 1) ^ (n >> 63);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long ZigzagDecode(long n) => (long)((ulong)n >> 1) ^ -(n & 1);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ZigzagEncode(int n) => (n << 1) ^ (n >> 31);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ZigzagDecode(int n) => (int)((uint)n >> 1) ^ -(n & 1);

    // --- Varint write ---

    public static void WriteVarint(MemoryStream ms, long value)
    {
        var encoded = (ulong)ZigzagEncode(value);
        while ((encoded & ~0x7FUL) != 0)
        {
            ms.WriteByte((byte)((encoded & 0x7F) | 0x80));
            encoded >>= 7;
        }
        ms.WriteByte((byte)encoded);
    }

    // --- Varint read ---

    public static (long Value, int BytesRead) ReadVarint(ReadOnlySpan<byte> data)
    {
        long encoded = 0;
        int shift = 0;
        int bytesRead = 0;
        byte b;
        do
        {
            if (bytesRead >= data.Length)
                throw new InvalidOperationException("varint overflows input buffer");
            b = data[bytesRead];
            encoded |= (long)(b & 0x7F) << shift;
            shift += 7;
            bytesRead++;
        } while ((b & 0x80) != 0 && shift < 64);

        return (ZigzagDecode(encoded), bytesRead);
    }

    // --- Primitive writers ---

    public static void WriteNull(MemoryStream ms) { /* 0 bytes */ }

    public static void WriteBoolean(MemoryStream ms, bool value)
        => ms.WriteByte(value ? (byte)1 : (byte)0);

    public static void WriteInt(MemoryStream ms, int value)
        => WriteVarint(ms, value);

    public static void WriteLong(MemoryStream ms, long value)
        => WriteVarint(ms, value);

    public static void WriteFloat(MemoryStream ms, float value)
    {
        Span<byte> buf = stackalloc byte[4];
        BitConverter.TryWriteBytes(buf, value);
        ms.Write(buf);
    }

    public static void WriteDouble(MemoryStream ms, double value)
    {
        Span<byte> buf = stackalloc byte[8];
        BitConverter.TryWriteBytes(buf, value);
        ms.Write(buf);
    }

    public static void WriteString(MemoryStream ms, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        WriteVarint(ms, bytes.Length);
        ms.Write(bytes);
    }

    public static void WriteBytes(MemoryStream ms, byte[] value)
    {
        WriteVarint(ms, value.Length);
        ms.Write(value);
    }

    // --- Primitive readers ---

    public static (bool Value, int BytesRead) ReadBoolean(ReadOnlySpan<byte> data)
        => (data[0] != 0, 1);

    public static (int Value, int BytesRead) ReadInt(ReadOnlySpan<byte> data)
    {
        var (v, n) = ReadVarint(data);
        return ((int)v, n);
    }

    public static (long Value, int BytesRead) ReadLong(ReadOnlySpan<byte> data)
        => ReadVarint(data);

    public static (float Value, int BytesRead) ReadFloat(ReadOnlySpan<byte> data)
        => (BitConverter.ToSingle(data), 4);

    public static (double Value, int BytesRead) ReadDouble(ReadOnlySpan<byte> data)
        => (BitConverter.ToDouble(data), 8);

    public static (string Value, int BytesRead) ReadString(ReadOnlySpan<byte> data)
    {
        var (len, n) = ReadVarint(data);
        var str = Encoding.UTF8.GetString(data.Slice(n, (int)len));
        return (str, n + (int)len);
    }

    public static (byte[] Value, int BytesRead) ReadBytes(ReadOnlySpan<byte> data)
    {
        var (len, n) = ReadVarint(data);
        var bytes = data.Slice(n, (int)len).ToArray();
        return (bytes, n + (int)len);
    }
}

/// <summary>
/// Encodes GenericRecords to Avro binary format.
/// Writes records with no container framing (raw Avro binary, not Object Container File).
/// </summary>
public sealed class AvroBinaryWriter
{
    public void WriteRecord(MemoryStream ms, GenericRecord record, Schema schema)
    {
        foreach (var field in schema.Fields)
        {
            var value = record.GetField(field.Name);
            WriteField(ms, value, field.FieldType);
        }
    }

    private void WriteField(MemoryStream ms, object? value, FieldType type)
    {
        switch (type)
        {
            case FieldType.Null:
                break;
            case FieldType.Boolean:
                AvroEncoding.WriteBoolean(ms, value is true || (value is string s && s == "true"));
                break;
            case FieldType.Int:
                AvroEncoding.WriteInt(ms, Convert.ToInt32(value ?? 0));
                break;
            case FieldType.Long:
                AvroEncoding.WriteLong(ms, Convert.ToInt64(value ?? 0L));
                break;
            case FieldType.Float:
                AvroEncoding.WriteFloat(ms, Convert.ToSingle(value ?? 0f));
                break;
            case FieldType.Double:
                AvroEncoding.WriteDouble(ms, Convert.ToDouble(value ?? 0.0));
                break;
            case FieldType.String:
                AvroEncoding.WriteString(ms, value?.ToString() ?? "");
                break;
            case FieldType.Bytes:
                AvroEncoding.WriteBytes(ms, value is byte[] bytes ? bytes : []);
                break;
            case FieldType.Enum:
                AvroEncoding.WriteInt(ms, Convert.ToInt32(value ?? 0));
                break;
            case FieldType.Union:
                if (value is null)
                {
                    AvroEncoding.WriteVarint(ms, 0);
                }
                else
                {
                    AvroEncoding.WriteVarint(ms, 1);
                    AvroEncoding.WriteString(ms, value.ToString() ?? "");
                }
                break;
            case FieldType.Array:
                if (value is List<object?> list && list.Count > 0)
                {
                    AvroEncoding.WriteVarint(ms, list.Count);
                    foreach (var item in list)
                        AvroEncoding.WriteString(ms, item?.ToString() ?? "");
                }
                AvroEncoding.WriteVarint(ms, 0);
                break;
            case FieldType.Map:
                if (value is Dictionary<string, object?> map && map.Count > 0)
                {
                    AvroEncoding.WriteVarint(ms, map.Count);
                    foreach (var (k, v) in map)
                    {
                        AvroEncoding.WriteString(ms, k);
                        AvroEncoding.WriteString(ms, v?.ToString() ?? "");
                    }
                }
                AvroEncoding.WriteVarint(ms, 0);
                break;
            case FieldType.Record:
                if (value is GenericRecord nested)
                    WriteRecord(ms, nested, nested.RecordSchema);
                break;
        }
    }
}

/// <summary>
/// Decodes Avro binary data into GenericRecords using a provided schema.
/// Reads raw Avro binary (not Object Container File).
/// </summary>
public sealed class AvroBinaryReader
{
    public (GenericRecord Record, int BytesRead) ReadRecord(ReadOnlySpan<byte> data, Schema schema)
    {
        var record = new GenericRecord(schema);
        int offset = 0;
        foreach (var field in schema.Fields)
        {
            var (value, bytesRead) = ReadField(data[offset..], field.FieldType);
            record.SetField(field.Name, value);
            offset += bytesRead;
        }
        return (record, offset);
    }

    private (object? Value, int BytesRead) ReadField(ReadOnlySpan<byte> data, FieldType type)
    {
        switch (type)
        {
            case FieldType.Null:
                return (null, 0);
            case FieldType.Boolean:
            {
                var (v, n) = AvroEncoding.ReadBoolean(data);
                return (v, n);
            }
            case FieldType.Int:
            {
                var (v, n) = AvroEncoding.ReadInt(data);
                return (v, n);
            }
            case FieldType.Long:
            {
                var (v, n) = AvroEncoding.ReadLong(data);
                return (v, n);
            }
            case FieldType.Float:
            {
                var (v, n) = AvroEncoding.ReadFloat(data);
                return (v, n);
            }
            case FieldType.Double:
            {
                var (v, n) = AvroEncoding.ReadDouble(data);
                return (v, n);
            }
            case FieldType.String:
            {
                var (v, n) = AvroEncoding.ReadString(data);
                return (v, n);
            }
            case FieldType.Bytes:
            {
                var (v, n) = AvroEncoding.ReadBytes(data);
                return (v, n);
            }
            case FieldType.Enum:
            {
                var (v, n) = AvroEncoding.ReadInt(data);
                return (v, n);
            }
            case FieldType.Union:
            {
                var (idx, n) = AvroEncoding.ReadLong(data);
                if (idx == 0)
                    return (null, n);
                var (v, n2) = AvroEncoding.ReadString(data[n..]);
                return (v, n + n2);
            }
            case FieldType.Array:
            {
                var items = new List<object?>();
                int offset = 0;
                while (true)
                {
                    var (count, cn) = AvroEncoding.ReadLong(data[offset..]);
                    offset += cn;
                    if (count == 0) break;
                    if (count < 0)
                    {
                        var (_, sn) = AvroEncoding.ReadLong(data[offset..]);
                        offset += sn;
                        count = -count;
                    }
                    for (long i = 0; i < count; i++)
                    {
                        var (v, vn) = AvroEncoding.ReadString(data[offset..]);
                        items.Add(v);
                        offset += vn;
                    }
                }
                return (items, offset);
            }
            case FieldType.Map:
            {
                var map = new Dictionary<string, object?>();
                int offset = 0;
                while (true)
                {
                    var (count, cn) = AvroEncoding.ReadLong(data[offset..]);
                    offset += cn;
                    if (count == 0) break;
                    if (count < 0)
                    {
                        var (_, sn) = AvroEncoding.ReadLong(data[offset..]);
                        offset += sn;
                        count = -count;
                    }
                    for (long i = 0; i < count; i++)
                    {
                        var (k, kn) = AvroEncoding.ReadString(data[offset..]);
                        offset += kn;
                        var (v, vn) = AvroEncoding.ReadString(data[offset..]);
                        offset += vn;
                        map[k] = v;
                    }
                }
                return (map, offset);
            }
            case FieldType.Record:
                return (null, 0);
            default:
                return (null, 0);
        }
    }
}

/// <summary>
/// IRecordReader for Avro binary: reads contiguous Avro-encoded records.
/// </summary>
public sealed class AvroRecordReader : IRecordReader
{
    public List<GenericRecord> Read(byte[] data, Schema schema)
    {
        if (data.Length == 0 || schema.Fields.Count == 0)
            return [];

        var reader = new AvroBinaryReader();
        var records = new List<GenericRecord>();
        int offset = 0;
        var span = data.AsSpan();

        while (offset < span.Length)
        {
            try
            {
                var (record, bytesRead) = reader.ReadRecord(span[offset..], schema);
                if (bytesRead == 0) break;
                records.Add(record);
                offset += bytesRead;
            }
            catch
            {
                break;
            }
        }

        return records;
    }
}

/// <summary>
/// IRecordWriter for Avro binary: writes contiguous Avro-encoded records (no container framing).
/// </summary>
public sealed class AvroRecordWriter : IRecordWriter
{
    public byte[] Write(List<GenericRecord> records, Schema schema)
    {
        if (records.Count == 0)
            return [];

        var writer = new AvroBinaryWriter();
        using var ms = new MemoryStream();
        foreach (var record in records)
            writer.WriteRecord(ms, record, schema);
        return ms.ToArray();
    }
}
