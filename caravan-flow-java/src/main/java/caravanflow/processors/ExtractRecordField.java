package caravanflow.processors;

import caravanflow.core.FlowFile;
import caravanflow.core.Processor;
import caravanflow.core.ProcessorResult;
import caravanflow.core.RecordContent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/// Extracts one or more field values from a {@link RecordContent} record
/// and stores them as FlowFile attributes. Mirrors caravan-flow-csharp's
/// {@code ExtractRecordField} (StdLib/RecordProcessors.cs:370-407).
///
/// Config:
///   fields      — semicolon-delimited {@code "fieldName:attrName"} pairs.
///                 Dotted paths supported on the field side
///                 ({@code "address.city:city"}).
///   recordIndex — which record to read (default 0 — the first).
///
/// Missing fields and out-of-range indices are silently skipped (the
/// FlowFile passes through) — matches C# semantics. Empty records list
/// also passes through.
public final class ExtractRecordField implements Processor {

    private record Pair(String field, String attr) {}

    private final List<Pair> pairs;
    private final int recordIndex;

    public ExtractRecordField(String fields, int recordIndex) {
        this.recordIndex = Math.max(0, recordIndex);
        List<Pair> parsed = new ArrayList<>();
        if (fields != null && !fields.isBlank()) {
            for (String entry : fields.split(";")) {
                String trimmed = entry.trim();
                if (trimmed.isEmpty()) continue;
                int colon = trimmed.indexOf(':');
                if (colon <= 0 || colon == trimmed.length() - 1) {
                    throw new IllegalArgumentException(
                            "ExtractRecordField: malformed entry '" + trimmed + "' — expected 'fieldName:attrName'");
                }
                parsed.add(new Pair(
                        trimmed.substring(0, colon).trim(),
                        trimmed.substring(colon + 1).trim()));
            }
        }
        this.pairs = List.copyOf(parsed);
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        if (!(ff.content() instanceof RecordContent rc)) {
            return ProcessorResult.failure(
                    "ExtractRecordField: expected RecordContent, got " + ff.content().getClass().getSimpleName(), ff);
        }
        if (rc.records().isEmpty() || recordIndex >= rc.records().size()) {
            return ProcessorResult.single(ff);
        }
        Map<String, Object> record = rc.records().get(recordIndex);
        FlowFile result = ff;
        for (Pair p : pairs) {
            Object val = resolve(record, p.field());
            if (val != null) {
                result = result.withAttribute(p.attr(), String.valueOf(val));
            }
        }
        return ProcessorResult.single(result);
    }

    /// Dotted path lookup: {@code "a.b.c"} traverses
    /// record["a"]["b"]["c"]. Returns null on any missing hop or
    /// non-map intermediate value.
    private static Object resolve(Map<String, Object> record, String path) {
        if (!path.contains(".")) return record.get(path);
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
