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

/// {@link RecordContent} → Avro binary bytes. Each record is encoded
/// sequentially with no container framing — the counterpart of
/// {@link ConvertAvroToRecord}. For file-style payloads with schema
/// embedded, use {@link ConvertRecordToOCF}.
public final class ConvertRecordToAvro implements Processor {

    private final Schema schema;

    public ConvertRecordToAvro(String schemaJson) {
        if (schemaJson == null || schemaJson.isEmpty()) {
            throw new IllegalArgumentException("ConvertRecordToAvro: schema must not be blank");
        }
        this.schema = new Schema.Parser().parse(schemaJson);
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        if (!(ff.content() instanceof RecordContent rc)) {
            return ProcessorResult.failure(
                    "ConvertRecordToAvro: expected RecordContent, got " + ff.content().getClass().getSimpleName(), ff);
        }
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(buf, null);
        try {
            for (Map<String, Object> record : rc.records()) {
                writer.write(AvroConversion.toGenericRecord(record, schema), encoder);
            }
            encoder.flush();
            return ProcessorResult.single(ff.withContent(new RawContent(buf.toByteArray())));
        } catch (IOException ex) {
            return ProcessorResult.failure("ConvertRecordToAvro: encode failed — " + ex.getMessage(), ff);
        }
    }
}
