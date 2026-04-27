package zincflow.fabric;

import zincflow.core.FlowFile;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// NiFi FlowFile V3 binary wire format — pack / unpack pairs for the
/// {@code NiFiFF3} magic, 2-byte (extended to 6-byte) length prefixes,
/// and 8-byte big-endian content length. Mirrors zinc-flow-csharp's
/// FlowFileV3 so Java and C# backends interchange FlowFiles byte-exact.
///
/// Format:
/// <pre>
///   magic: "NiFiFF3" (7 bytes)
///   attrCount: field-length (2 or 6 bytes)
///   for each attribute:
///     keyLen: field-length
///     key:    UTF-8
///     valLen: field-length
///     value:  UTF-8
///   contentLen: int64 big-endian (8 bytes)
///   content:    raw bytes
/// </pre>
/// Field-length: 2 bytes unsigned when value &lt; 0xFFFF, otherwise
/// {@code 0xFFFF} sentinel + 4 bytes unsigned int.
public final class FlowFileV3 {

    public static final byte[] MAGIC = "NiFiFF3".getBytes(StandardCharsets.US_ASCII);
    public static final int MAGIC_LEN = MAGIC.length;

    private static final int MAX_VALUE_2_BYTES = 0xFFFF;

    private FlowFileV3() {}

    public record UnpackResult(FlowFile flowFile, int nextOffset, String error) {
        public boolean ok() { return error == null || error.isEmpty(); }
    }

    // --- Pack ---

    public static byte[] pack(FlowFile ff, byte[] contentBytes) {
        if (contentBytes == null) contentBytes = new byte[0];
        ByteArrayOutputStream out = new ByteArrayOutputStream(MAGIC_LEN + 256 + contentBytes.length);
        out.writeBytes(MAGIC);

        Map<String, String> attrs = ff.attributes();
        writeFieldLength(out, attrs.size());
        for (var entry : attrs.entrySet()) {
            byte[] k = entry.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] v = entry.getValue() == null
                    ? new byte[0]
                    : entry.getValue().getBytes(StandardCharsets.UTF_8);
            writeFieldLength(out, k.length);
            out.writeBytes(k);
            writeFieldLength(out, v.length);
            out.writeBytes(v);
        }

        byte[] lenBuf = new byte[8];
        writeInt64BE(lenBuf, 0, contentBytes.length);
        out.writeBytes(lenBuf);
        out.writeBytes(contentBytes);
        return out.toByteArray();
    }

    public static byte[] packMultiple(List<FlowFile> flowFiles, List<byte[]> contents) {
        if (flowFiles.size() != contents.size()) {
            throw new IllegalArgumentException(
                    "flowFiles.size() (" + flowFiles.size() + ") must equal contents.size() ("
                            + contents.size() + ")");
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0; i < flowFiles.size(); i++) {
            out.writeBytes(pack(flowFiles.get(i), contents.get(i)));
        }
        return out.toByteArray();
    }

    // --- Unpack ---

    public static UnpackResult unpack(byte[] data, int offset) {
        if (offset < 0 || offset + MAGIC_LEN > data.length) {
            return new UnpackResult(null, offset, "buffer too small for FlowFile V3 magic at offset " + offset);
        }
        for (int i = 0; i < MAGIC_LEN; i++) {
            if (data[offset + i] != MAGIC[i]) {
                return new UnpackResult(null, offset, "invalid FlowFile V3 magic at offset " + offset);
            }
        }
        int pos = offset + MAGIC_LEN;

        int[] ref = new int[1];
        int count = readFieldLength(data, pos, ref);
        pos = ref[0];

        Map<String, String> attrs = new LinkedHashMap<>(count);
        for (int i = 0; i < count; i++) {
            int keyLen = readFieldLength(data, pos, ref); pos = ref[0];
            if (pos + keyLen > data.length) {
                return new UnpackResult(null, offset, "truncated key at attribute " + i);
            }
            String key = new String(data, pos, keyLen, StandardCharsets.UTF_8);
            pos += keyLen;

            int valLen = readFieldLength(data, pos, ref); pos = ref[0];
            if (pos + valLen > data.length) {
                return new UnpackResult(null, offset, "truncated value at attribute '" + key + "'");
            }
            String val = new String(data, pos, valLen, StandardCharsets.UTF_8);
            pos += valLen;

            attrs.put(key, val);
        }

        if (pos + 8 > data.length) {
            return new UnpackResult(null, offset, "truncated content-length header");
        }
        long contentLen = readInt64BE(data, pos);
        pos += 8;
        if (contentLen < 0 || pos + contentLen > data.length) {
            return new UnpackResult(null, offset, "content length " + contentLen + " overruns buffer");
        }
        byte[] content = new byte[(int) contentLen];
        System.arraycopy(data, pos, content, 0, content.length);
        pos += content.length;

        FlowFile ff = FlowFile.create(content, attrs);
        return new UnpackResult(ff, pos, "");
    }

    /// Decode every FlowFile in the buffer; stops on the first unpack
    /// error without throwing (mirrors the C# behavior — partial streams
    /// surface whatever was well-formed). Returns empty if the magic is
    /// missing at offset 0.
    public static List<FlowFile> unpackAll(byte[] data) {
        List<FlowFile> out = new java.util.ArrayList<>();
        int pos = 0;
        while (pos < data.length) {
            UnpackResult r = unpack(data, pos);
            if (!r.ok() || r.flowFile() == null) break;
            out.add(r.flowFile());
            pos = r.nextOffset();
        }
        return out;
    }

    // --- Length + int helpers ---

    private static void writeFieldLength(ByteArrayOutputStream out, int value) {
        if (value < MAX_VALUE_2_BYTES) {
            out.write((value >>> 8) & 0xFF);
            out.write(value & 0xFF);
        } else {
            out.write(0xFF);
            out.write(0xFF);
            out.write((value >>> 24) & 0xFF);
            out.write((value >>> 16) & 0xFF);
            out.write((value >>> 8) & 0xFF);
            out.write(value & 0xFF);
        }
    }

    private static int readFieldLength(byte[] data, int offset, int[] nextOut) {
        int high = data[offset] & 0xFF;
        int low  = data[offset + 1] & 0xFF;
        int val = (high << 8) | low;
        if (val < MAX_VALUE_2_BYTES) {
            nextOut[0] = offset + 2;
            return val;
        }
        int ext = ((data[offset + 2] & 0xFF) << 24)
                | ((data[offset + 3] & 0xFF) << 16)
                | ((data[offset + 4] & 0xFF) << 8)
                | (data[offset + 5] & 0xFF);
        nextOut[0] = offset + 6;
        return ext;
    }

    private static void writeInt64BE(byte[] buf, int offset, long value) {
        ByteBuffer.wrap(buf, offset, 8).putLong(value);
    }

    private static long readInt64BE(byte[] buf, int offset) {
        return ByteBuffer.wrap(buf, offset, 8).getLong();
    }
}
