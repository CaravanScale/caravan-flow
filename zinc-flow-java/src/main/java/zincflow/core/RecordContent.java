package zincflow.core;

import org.apache.avro.Schema;

import java.util.List;
import java.util.Map;

/// Structured records payload — a list of flat {@code Map<String,Object>}
/// entries carrying an optional Avro {@link Schema}. The schema travels
/// alongside the records so downstream format-converting processors
/// (CSV, Avro, OCF writers) can preserve field order and types.
///
/// Mirrors zinc-flow-csharp's {@code RecordContent(Schema, List<GenericRecord>)}.
/// The schema is nullable: JSON-read records and other ad-hoc sources
/// arrive without one, and processors that only need field values
/// (QueryRecord, TransformRecord, ExtractRecordField) ignore it. Format
/// writers fall back to inferring a schema from the first record when
/// the field is absent.
public record RecordContent(List<Map<String, Object>> records, Schema schema) implements Content {

    public RecordContent {
        if (records == null) {
            throw new IllegalArgumentException("RecordContent records must not be null — use List.of() for empty");
        }
        records = List.copyOf(records);
        // schema may be null — see class javadoc.
    }

    /// Backwards-compatible constructor for call sites that don't yet
    /// carry a schema (JSON reads, in-memory transforms, test setup).
    public RecordContent(List<Map<String, Object>> records) {
        this(records, null);
    }

    @Override
    public int size() {
        return records.size();
    }
}
