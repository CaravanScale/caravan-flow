package zincflow.processors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Bidirectional helpers between Avro's {@link GenericRecord} and the
/// {@code Map<String,Object>} shape that {@link zincflow.core.RecordContent}
/// uses.  Avro's native types don't map 1-to-1 to Java collections
/// (strings come back as {@link Utf8}, byte fields as {@link ByteBuffer},
/// unions need branch selection), so this centralises the translation.
///
/// Not a full-featured codec — we cover the primitive + nested record +
/// array + map + nullable-union shapes that real pipelines use. Fixed,
/// enum, decimal, and other logical types can land in follow-up work.
final class AvroConversion {

    private AvroConversion() { }

    // --- Record ↔ Map -----------------------------------------------------

    static Map<String, Object> toMap(GenericRecord record) {
        if (record == null) return null;
        Schema schema = record.getSchema();
        Map<String, Object> out = new LinkedHashMap<>();
        for (Schema.Field f : schema.getFields()) {
            out.put(f.name(), unwrap(record.get(f.name()), f.schema()));
        }
        return out;
    }

    static GenericRecord toGenericRecord(Map<String, Object> map, Schema schema) {
        if (map == null) return null;
        GenericData.Record record = new GenericData.Record(schema);
        for (Schema.Field f : schema.getFields()) {
            Object raw = map.get(f.name());
            record.put(f.name(), wrap(raw, f.schema()));
        }
        return record;
    }

    // --- Value unwrap (Avro → Java idiomatic) ----------------------------

    @SuppressWarnings("unchecked")
    private static Object unwrap(Object value, Schema schema) {
        if (value == null) return null;
        Schema effective = resolveUnion(schema, value);
        return switch (effective.getType()) {
            case STRING   -> value instanceof Utf8 u ? u.toString() : value.toString();
            case BYTES    -> value instanceof ByteBuffer bb ? toByteArray(bb) : value;
            case RECORD   -> toMap((GenericRecord) value);
            case ARRAY    -> {
                List<Object> src = (List<Object>) value;
                List<Object> out = new ArrayList<>(src.size());
                for (Object item : src) out.add(unwrap(item, effective.getElementType()));
                yield out;
            }
            case MAP      -> {
                Map<CharSequence, Object> src = (Map<CharSequence, Object>) value;
                Map<String, Object> out = new LinkedHashMap<>();
                for (var entry : src.entrySet()) {
                    out.put(entry.getKey().toString(), unwrap(entry.getValue(), effective.getValueType()));
                }
                yield out;
            }
            default       -> value; // INT/LONG/FLOAT/DOUBLE/BOOLEAN/NULL pass through
        };
    }

    // --- Value wrap (Java idiomatic → Avro) ------------------------------

    @SuppressWarnings("unchecked")
    private static Object wrap(Object value, Schema schema) {
        if (value == null) return null;
        Schema effective = resolveUnion(schema, value);
        return switch (effective.getType()) {
            case STRING   -> value instanceof CharSequence cs ? cs.toString() : value.toString();
            case BYTES    -> value instanceof byte[] bytes ? ByteBuffer.wrap(bytes) : value;
            case RECORD   -> value instanceof Map<?, ?> m
                                ? toGenericRecord((Map<String, Object>) m, effective)
                                : value;
            case ARRAY    -> {
                List<Object> src = value instanceof List<?> list ? (List<Object>) list : List.of(value);
                List<Object> out = new ArrayList<>(src.size());
                for (Object item : src) out.add(wrap(item, effective.getElementType()));
                yield out;
            }
            case MAP      -> {
                Map<String, Object> src = (Map<String, Object>) value;
                Map<String, Object> out = new LinkedHashMap<>();
                for (var entry : src.entrySet()) {
                    out.put(entry.getKey(), wrap(entry.getValue(), effective.getValueType()));
                }
                yield out;
            }
            case INT      -> value instanceof Number n ? n.intValue() : value;
            case LONG     -> value instanceof Number n ? n.longValue() : value;
            case FLOAT    -> value instanceof Number n ? n.floatValue() : value;
            case DOUBLE   -> value instanceof Number n ? n.doubleValue() : value;
            default       -> value;
        };
    }

    // --- Union handling ---------------------------------------------------

    /// For unions like {@code ["null", "string"]}, pick the branch that
    /// matches the actual value type. Falls back to the first non-null
    /// branch when value is null and the union includes null.
    private static Schema resolveUnion(Schema schema, Object value) {
        if (schema.getType() != Schema.Type.UNION) return schema;
        for (Schema branch : schema.getTypes()) {
            if (branchMatches(branch, value)) return branch;
        }
        // Fall back to the first non-null branch — common for nullable
        // fields where we're writing a non-null value.
        for (Schema branch : schema.getTypes()) {
            if (branch.getType() != Schema.Type.NULL) return branch;
        }
        return schema.getTypes().getFirst();
    }

    private static boolean branchMatches(Schema branch, Object value) {
        return switch (branch.getType()) {
            case NULL    -> value == null;
            case STRING  -> value instanceof CharSequence;
            case INT, LONG, FLOAT, DOUBLE -> value instanceof Number;
            case BOOLEAN -> value instanceof Boolean;
            case BYTES   -> value instanceof byte[] || value instanceof ByteBuffer;
            case ARRAY   -> value instanceof List<?>;
            case MAP     -> value instanceof Map<?, ?>;
            case RECORD  -> value instanceof GenericRecord || value instanceof Map<?, ?>;
            default      -> false;
        };
    }

    private static byte[] toByteArray(ByteBuffer bb) {
        bb = bb.duplicate();
        byte[] out = new byte[bb.remaining()];
        bb.get(out);
        return out;
    }
}
