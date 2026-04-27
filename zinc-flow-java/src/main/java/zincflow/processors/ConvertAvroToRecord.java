package zincflow.processors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import zincflow.core.FlowFile;
import zincflow.core.Processor;
import zincflow.core.ProcessorResult;
import zincflow.core.RawContent;
import zincflow.core.RecordContent;
import zincflow.core.SchemaDefs;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/// Avro binary → {@link RecordContent}. Mirrors zinc-flow-csharp's
/// {@code ConvertAvroToRecord} (StdLib/RecordProcessors.cs:11-84).
///
/// Config:
///   schemaName — record name label for the parsed schema (optional)
///   fields     — compact Avro field defs: {@code "name:type,name:type"}
///                (see {@link SchemaDefs} for supported types)
///
/// When {@code fields} is omitted, the processor falls back to the
/// {@code avro.schema} FlowFile attribute (same compact format). When
/// neither produces a schema, ingestion fails with a clear message —
/// Avro binary is self-describing only when paired with a schema.
public final class ConvertAvroToRecord implements Processor {

    private final String schemaName;
    private final Schema configSchema;

    public ConvertAvroToRecord(String schemaName, String fieldDefs) {
        this.schemaName = schemaName == null ? "" : schemaName;
        this.configSchema = SchemaDefs.parse(this.schemaName, fieldDefs);
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        if (!(ff.content() instanceof RawContent raw)) {
            return ProcessorResult.failure(
                    "ConvertAvroToRecord: expected RawContent, got " + ff.content().getClass().getSimpleName(), ff);
        }

        Schema schema = configSchema;
        if (schema == null) {
            String attrFields = ff.attributes().get("avro.schema");
            if (attrFields != null && !attrFields.isBlank()) {
                schema = SchemaDefs.parse(schemaName, attrFields);
            }
        }
        if (schema == null || schema.getFields().isEmpty()) {
            return ProcessorResult.failure(
                    "no schema: set 'fields' config or 'avro.schema' attribute", ff);
        }

        try {
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(raw.bytes()), null);
            List<Map<String, Object>> records = new ArrayList<>();
            while (!decoder.isEnd()) {
                GenericRecord r = reader.read(null, decoder);
                records.add(AvroConversion.toMap(r));
            }
            if (records.isEmpty()) {
                return ProcessorResult.failure("no records decoded from Avro binary", ff);
            }
            return ProcessorResult.single(
                    ff.withContent(new RecordContent(records, schema))
                      .withAttribute("record.count", String.valueOf(records.size())));
        } catch (EOFException eof) {
            return ProcessorResult.failure("ConvertAvroToRecord: unexpected EOF mid-record", ff);
        } catch (IOException ex) {
            return ProcessorResult.failure("ConvertAvroToRecord: decode failed — " + ex.getMessage(), ff);
        }
    }
}
