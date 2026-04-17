package caravanflow.processors;

import caravanflow.core.FlowFile;
import caravanflow.core.Processor;
import caravanflow.core.ProcessorResult;
import caravanflow.core.RecordContent;

import java.util.Map;

/// Extracts one field from the first record in a {@link RecordContent}
/// payload and stores it as a FlowFile attribute. Useful for hoisting
/// routing keys ("tenant", "priority", etc.) out of records into
/// attributes so downstream RouteOnAttribute can dispatch on them.
public final class ExtractRecordField implements Processor {

    private final String fieldPath;
    private final String attributeName;

    public ExtractRecordField(String fieldPath, String attributeName) {
        if (fieldPath == null || fieldPath.isEmpty()) {
            throw new IllegalArgumentException("ExtractRecordField: fieldPath must not be blank");
        }
        if (attributeName == null || attributeName.isEmpty()) {
            throw new IllegalArgumentException("ExtractRecordField: attributeName must not be blank");
        }
        this.fieldPath = fieldPath;
        this.attributeName = attributeName;
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        if (!(ff.content() instanceof RecordContent rc)) {
            return ProcessorResult.failure(
                    "ExtractRecordField: expected RecordContent, got " + ff.content().getClass().getSimpleName(), ff);
        }
        if (rc.records().isEmpty()) {
            return ProcessorResult.failure("ExtractRecordField: record list is empty", ff);
        }
        Object value = resolve(rc.records().getFirst(), fieldPath);
        if (value == null) {
            return ProcessorResult.failure(
                    "ExtractRecordField: field '" + fieldPath + "' not present in first record", ff);
        }
        return ProcessorResult.single(ff.withAttribute(attributeName, String.valueOf(value)));
    }

    /// Resolves a dotted field path against a nested map. {@code "a.b.c"}
    /// traverses record["a"]["b"]["c"]. Returns null on any missing hop
    /// or non-map intermediate value.
    private static Object resolve(Map<String, Object> record, String path) {
        String[] parts = path.split("\\.");
        Object current = record;
        for (String part : parts) {
            if (!(current instanceof Map<?, ?> m)) return null;
            current = m.get(part);
            if (current == null) return null;
        }
        return current;
    }
}
