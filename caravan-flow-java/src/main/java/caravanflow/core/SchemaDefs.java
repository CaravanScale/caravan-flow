package caravanflow.core;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;

/// Parser for the compact field-defs config string
/// ({@code "name:type,name:type"}) into an Apache Avro {@link Schema}.
///
/// Mirrors caravan-flow-csharp's {@code ConvertAvroToRecord.ParseFieldDefs}
/// (StdLib/RecordProcessors.cs:54-83). Keeps the same type aliases and
/// the same error wording so config failures are recognizable across
/// tracks. Delimiter is comma (C# canonical for this field).
///
/// Supported types: boolean/bool, int/int32, long/int64, float/float32,
/// double/float64, bytes, string. Unknown types throw — matches C#.
public final class SchemaDefs {

    private SchemaDefs() {}

    public static Schema parse(String recordName, String fieldDefs) {
        if (fieldDefs == null || fieldDefs.isBlank()) return null;

        String name = recordName == null || recordName.isBlank() ? "Record" : recordName;
        FieldAssembler<Schema> assembler = SchemaBuilder.record(name).namespace("caravanflow").fields();

        for (String part : fieldDefs.split(",")) {
            String trimmed = part.trim();
            if (trimmed.isEmpty()) continue;
            int colon = trimmed.indexOf(':');
            if (colon <= 0 || colon == trimmed.length() - 1) {
                throw new IllegalArgumentException(
                        "ConvertAvroToRecord: malformed field def '" + trimmed + "' — expected 'name:type'");
            }
            String fname = trimmed.substring(0, colon).trim();
            String ftype = trimmed.substring(colon + 1).trim().toLowerCase();

            assembler = appendField(assembler, fname, ftype);
        }
        return assembler.endRecord();
    }

    private static FieldAssembler<Schema> appendField(FieldAssembler<Schema> a, String fname, String ftype) {
        return switch (ftype) {
            case "boolean", "bool"       -> a.name(fname).type().booleanType().noDefault();
            case "int", "int32"          -> a.name(fname).type().intType().noDefault();
            case "long", "int64"         -> a.name(fname).type().longType().noDefault();
            case "float", "float32"      -> a.name(fname).type().floatType().noDefault();
            case "double", "float64"     -> a.name(fname).type().doubleType().noDefault();
            case "bytes"                 -> a.name(fname).type().bytesType().noDefault();
            case "string"                -> a.name(fname).type().stringType().noDefault();
            default -> throw new IllegalArgumentException(
                    "ConvertAvroToRecord: unknown field type '" + ftype + "' in '" + fname + ":" + ftype
                    + "' — valid: boolean, int, long, float, double, bytes, string");
        };
    }
}
