package caravanflow.processors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import caravanflow.core.FlowFile;
import caravanflow.core.Processor;
import caravanflow.core.ProcessorResult;
import caravanflow.core.RawContent;
import caravanflow.core.RecordContent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.StringJoiner;

/// {@link RecordContent} → Avro binary bytes. Mirrors caravan-flow-csharp's
/// {@code ConvertRecordToAvro} (StdLib/RecordProcessors.cs:90-105) — no
/// config, schema is read directly from the incoming RecordContent.
///
/// Writes an {@code avro.schema} attribute on the output FlowFile with the
/// compact field-defs representation so a downstream
/// {@link ConvertAvroToRecord} can decode without repeating the schema in
/// its config.
public final class ConvertRecordToAvro implements Processor {

    @Override
    public ProcessorResult process(FlowFile ff) {
        if (!(ff.content() instanceof RecordContent rc)) {
            return ProcessorResult.failure(
                    "ConvertRecordToAvro: expected RecordContent, got " + ff.content().getClass().getSimpleName(), ff);
        }
        if (rc.records().isEmpty()) {
            return ProcessorResult.single(ff);
        }
        Schema schema = rc.schema();
        if (schema == null) {
            return ProcessorResult.failure(
                    "ConvertRecordToAvro: RecordContent has no schema — upstream must declare one", ff);
        }

        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(buf, null);
        try {
            for (Map<String, Object> record : rc.records()) {
                writer.write(AvroConversion.toGenericRecord(record, schema), encoder);
            }
            encoder.flush();
            return ProcessorResult.single(
                    ff.withContent(new RawContent(buf.toByteArray()))
                      .withAttribute("avro.schema", compactFieldDefs(schema)));
        } catch (IOException ex) {
            return ProcessorResult.failure("ConvertRecordToAvro: encode failed — " + ex.getMessage(), ff);
        }
    }

    /// Serialize a Schema as the compact {@code "name:type,name:type"}
    /// form so downstream processors can re-parse it with
    /// {@link caravanflow.core.SchemaDefs#parse}.
    private static String compactFieldDefs(Schema schema) {
        StringJoiner sj = new StringJoiner(",");
        for (Schema.Field f : schema.getFields()) {
            Schema.Type t = f.schema().getType() == Schema.Type.UNION
                    ? firstNonNullBranch(f.schema())
                    : f.schema().getType();
            sj.add(f.name() + ":" + t.getName());
        }
        return sj.toString();
    }

    private static Schema.Type firstNonNullBranch(Schema union) {
        for (Schema branch : union.getTypes()) {
            if (branch.getType() != Schema.Type.NULL) return branch.getType();
        }
        return Schema.Type.NULL;
    }
}
