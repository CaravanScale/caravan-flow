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

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/// Avro binary → {@link RecordContent}. Reads records until the decoder
/// hits EOF, so payloads with concatenated records work — useful for
/// pipelines that fan-in from multiple producers.
public final class ConvertAvroToRecord implements Processor {

    private final Schema schema;

    public ConvertAvroToRecord(String schemaJson) {
        if (schemaJson == null || schemaJson.isEmpty()) {
            throw new IllegalArgumentException("ConvertAvroToRecord: schema must not be blank");
        }
        this.schema = new Schema.Parser().parse(schemaJson);
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        if (!(ff.content() instanceof RawContent raw)) {
            return ProcessorResult.failure(
                    "ConvertAvroToRecord: expected RawContent, got " + ff.content().getClass().getSimpleName(), ff);
        }
        try {
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(raw.bytes()), null);
            List<Map<String, Object>> records = new ArrayList<>();
            while (!decoder.isEnd()) {
                GenericRecord r = reader.read(null, decoder);
                records.add(AvroConversion.toMap(r));
            }
            return ProcessorResult.single(
                    ff.withContent(new RecordContent(records))
                      .withAttribute("record.count", String.valueOf(records.size())));
        } catch (EOFException eof) {
            // Rare — should be caught by isEnd() — treat as an incomplete payload.
            return ProcessorResult.failure("ConvertAvroToRecord: unexpected EOF mid-record", ff);
        } catch (IOException ex) {
            return ProcessorResult.failure("ConvertAvroToRecord: decode failed — " + ex.getMessage(), ff);
        }
    }
}
