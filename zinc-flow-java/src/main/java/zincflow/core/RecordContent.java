package zincflow.core;

import java.util.List;
import java.util.Map;

/// Structured records payload — a list of flat {@code Map<String,Object>}
/// entries, suitable for JSON, CSV, or other row-oriented formats.
/// Schema-aware Avro records use this same shape (field name → value);
/// a dedicated {@code AvroContent} variant with an explicit
/// {@code org.apache.avro.Schema} can land as a follow-up when strict
/// schema validation matters.
public record RecordContent(List<Map<String, Object>> records) implements Content {

    public RecordContent {
        if (records == null) {
            throw new IllegalArgumentException("RecordContent records must not be null — use List.of() for empty");
        }
        records = List.copyOf(records);
    }

    @Override
    public int size() {
        return records.size();
    }
}
