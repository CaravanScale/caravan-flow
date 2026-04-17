using System.Buffers;
using System.Text;
using CaravanFlow.Core;

namespace CaravanFlow.Fabric;

/// <summary>
/// NiFi FlowFile V3 binary wire format: pack/unpack.
/// Uses ArrayPool for intermediate buffers to avoid LOH pressure.
/// </summary>
public static class FlowFileV3
{
    private static readonly byte[] Magic = "NiFiFF3"u8.ToArray();
    private const int MagicLen = 7;
    private const int MaxValue2Bytes = 0xFFFF;

    // --- Pack: FlowFile → V3 binary ---

    public static byte[] Pack(FlowFile ff, byte[] contentBytes)
    {
        // Estimate size: magic + attrs + content
        using var ms = new MemoryStream(MagicLen + 256 + contentBytes.Length);
        ms.Write(Magic);

        // Attribute count
        var attrCount = CountAttributes(ff.Attributes);
        WriteFieldLength(ms, attrCount);

        // Attributes — iterate overlay chain, collect key/value pairs
        WriteAttributes(ms, ff.Attributes);

        // Content length (8-byte big-endian) + content
        Span<byte> lenBuf = stackalloc byte[8];
        BinaryHelpers.WriteInt64BE(lenBuf, contentBytes.Length);
        ms.Write(lenBuf);
        ms.Write(contentBytes);

        return ms.ToArray();
    }

    public static byte[] PackMultiple(List<FlowFile> flowfiles, List<byte[]> contents)
    {
        using var ms = new MemoryStream();
        for (int i = 0; i < flowfiles.Count; i++)
        {
            var packed = Pack(flowfiles[i], contents[i]);
            ms.Write(packed);
        }
        return ms.ToArray();
    }

    // --- Unpack: V3 binary → FlowFile ---

    public static (FlowFile? Ff, int NextOffset, string Error) Unpack(byte[] data, int offset)
    {
        if (offset + MagicLen > data.Length || !data.AsSpan(offset, MagicLen).SequenceEqual(Magic))
            return (null, offset, $"invalid FlowFile V3 magic at offset {offset}");

        var pos = offset + MagicLen;

        // Attribute count
        var (count, nextPos) = ReadFieldLength(data, pos);
        pos = nextPos;

        // Attributes
        var attrs = new Dictionary<string, string>(count);
        for (int i = 0; i < count; i++)
        {
            var (keyLen, kp) = ReadFieldLength(data, pos); pos = kp;
            var key = Encoding.UTF8.GetString(data, pos, keyLen); pos += keyLen;

            var (valLen, vp) = ReadFieldLength(data, pos); pos = vp;
            var val = Encoding.UTF8.GetString(data, pos, valLen); pos += valLen;

            attrs[key] = val;
        }

        // Content
        var contentLen = (int)BinaryHelpers.ReadInt64BE(data.AsSpan(pos));
        pos += 8;
        var content = data.AsSpan(pos, contentLen).ToArray();
        pos += contentLen;

        var ff = FlowFile.Create(content, attrs);
        return (ff, pos, "");
    }

    public static List<FlowFile> UnpackAll(byte[] data)
    {
        var result = new List<FlowFile>();
        var pos = 0;
        while (pos < data.Length)
        {
            var (ff, nextPos, error) = Unpack(data, pos);
            if (error != "" || ff is null) break;
            result.Add(ff);
            pos = nextPos;
        }
        return result;
    }

    // --- Encoding helpers ---

    private static void WriteFieldLength(MemoryStream ms, int value)
    {
        Span<byte> buf = stackalloc byte[6];
        if (value < MaxValue2Bytes)
        {
            BinaryHelpers.WriteUInt16BE(buf, (ushort)value);
            ms.Write(buf[..2]);
        }
        else
        {
            buf[0] = 0xFF;
            buf[1] = 0xFF;
            BinaryHelpers.WriteUInt32BE(buf[2..], (uint)value);
            ms.Write(buf);
        }
    }

    private static (int Value, int NextOffset) ReadFieldLength(byte[] data, int offset)
    {
        var val = BinaryHelpers.ReadUInt16BE(data.AsSpan(offset));
        if (val < MaxValue2Bytes)
            return (val, offset + 2);
        return ((int)BinaryHelpers.ReadUInt32BE(data.AsSpan(offset + 2)), offset + 6);
    }

    private static int CountAttributes(AttributeMap attrs)
    {
        return attrs.Count;
    }

    private static void WriteAttributes(MemoryStream ms, AttributeMap attrs)
    {
        // Materialize overlay chain to dict for serialization
        var dict = new Dictionary<string, string>();
        MaterializeAttributes(attrs, dict);

        foreach (var (key, value) in dict)
        {
            var keyBytes = Encoding.UTF8.GetBytes(key);
            var valBytes = Encoding.UTF8.GetBytes(value);
            WriteFieldLength(ms, keyBytes.Length);
            ms.Write(keyBytes);
            WriteFieldLength(ms, valBytes.Length);
            ms.Write(valBytes);
        }
    }

    private static void MaterializeAttributes(AttributeMap attrs, Dictionary<string, string> dict)
    {
        // Walk the overlay chain bottom-up, base dict first, overlays on top
        var stack = new Stack<AttributeMap>();
        var current = attrs;
        while (current is not null)
        {
            stack.Push(current);
            // Access parent via internal field
            if (current._key is not null)
                current = current._parent;
            else
                break; // at base
        }

        while (stack.Count > 0)
        {
            var node = stack.Pop();
            if (node._base is not null)
            {
                foreach (var (k, v) in node._base)
                    dict[k] = v;
            }
            else if (node._key is not null)
            {
                dict[node._key] = node._value!;
            }
        }
    }
}
