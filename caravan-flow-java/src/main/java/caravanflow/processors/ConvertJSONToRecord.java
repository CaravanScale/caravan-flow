package caravanflow.processors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import caravanflow.core.FlowFile;
import caravanflow.core.Processor;
import caravanflow.core.ProcessorResult;
import caravanflow.core.RawContent;
import caravanflow.core.RecordContent;
import caravanflow.core.SchemaDefs;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/// Parses the FlowFile's RawContent payload as JSON and upgrades it to
/// {@link RecordContent}. Mirrors caravan-flow-csharp's
/// {@code ConvertJSONToRecord} (StdLib/Processors.cs:148-187).
///
/// Config:
///   schemaName — record name label applied to the inferred schema
///
/// Accepts either a single JSON object (wrapped into a 1-element record
/// list) or a JSON array of objects. The schema is inferred from the
/// first record's keys + value types so downstream Avro/OCF writers can
/// encode without extra config.
public final class ConvertJSONToRecord implements Processor {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> OBJ_TYPE = new TypeReference<>() {};
    private static final TypeReference<List<Map<String, Object>>> ARR_TYPE = new TypeReference<>() {};

    private final String schemaName;

    public ConvertJSONToRecord() { this(""); }

    public ConvertJSONToRecord(String schemaName) {
        this.schemaName = schemaName == null || schemaName.isBlank() ? "JsonRecord" : schemaName;
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        if (!(ff.content() instanceof RawContent raw)) {
            return ProcessorResult.failure(
                    "ConvertJSONToRecord: expected RawContent, got " + ff.content().getClass().getSimpleName(), ff);
        }
        String text = new String(raw.bytes(), StandardCharsets.UTF_8).trim();
        if (text.isEmpty()) {
            return ProcessorResult.failure("ConvertJSONToRecord: empty payload", ff);
        }
        try {
            List<Map<String, Object>> records;
            if (text.startsWith("[")) {
                records = MAPPER.readValue(text, ARR_TYPE);
            } else {
                records = List.of(MAPPER.readValue(text, OBJ_TYPE));
            }
            Schema schema = inferSchema(records);
            return ProcessorResult.single(
                    ff.withContent(new RecordContent(records, schema))
                      .withAttribute("record.count", String.valueOf(records.size())));
        } catch (Exception ex) {
            return ProcessorResult.failure("ConvertJSONToRecord: parse failed — " + ex.getMessage(), ff);
        }
    }

    /// Infer a flat Avro schema from the first record's keys + value
    /// types. String / int / long / double / boolean / bytes are
    /// detected directly; everything else (nested objects, arrays) falls
    /// back to string — not strictly correct but keeps JSON→Record
    /// usable as input to Avro writers for the common flat-object case.
    private Schema inferSchema(List<Map<String, Object>> records) {
        if (records.isEmpty()) return null;
        Map<String, Object> first = records.getFirst();
        StringJoiner defs = new StringJoiner(",");
        for (var entry : first.entrySet()) {
            defs.add(entry.getKey() + ":" + inferType(entry.getValue()));
        }
        return SchemaDefs.parse(schemaName, defs.toString());
    }

    private static String inferType(Object v) {
        if (v == null) return "string";
        if (v instanceof Boolean) return "boolean";
        if (v instanceof Integer) return "int";
        if (v instanceof Long) return "long";
        if (v instanceof Float) return "float";
        if (v instanceof Double) return "double";
        if (v instanceof byte[]) return "bytes";
        return "string";
    }
}
