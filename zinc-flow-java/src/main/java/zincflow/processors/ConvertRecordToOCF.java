package zincflow.processors;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import zincflow.core.FlowFile;
import zincflow.core.Processor;
import zincflow.core.ProcessorResult;
import zincflow.core.RawContent;
import zincflow.core.RecordContent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/// RecordContent → OCF bytes (Object Container File). Schema is
/// required — OCF's header embeds it so downstream readers need no
/// config. Codec defaults to {@code null} (no compression) to match
/// the zinc-flow-csharp default; {@code deflate}, {@code snappy},
/// {@code bzip2}, {@code xz}, and {@code zstandard} are passed through
/// to {@link CodecFactory#fromString(String)}.
public final class ConvertRecordToOCF implements Processor {

    private final Schema schema;
    private final CodecFactory codec;

    public ConvertRecordToOCF(String schemaJson) { this(schemaJson, "null"); }

    public ConvertRecordToOCF(String schemaJson, String codecName) {
        if (schemaJson == null || schemaJson.isEmpty()) {
            throw new IllegalArgumentException("ConvertRecordToOCF: schema must not be blank");
        }
        this.schema = new Schema.Parser().parse(schemaJson);
        this.codec = CodecFactory.fromString(codecName == null || codecName.isEmpty() ? "null" : codecName);
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        if (!(ff.content() instanceof RecordContent rc)) {
            return ProcessorResult.failure(
                    "ConvertRecordToOCF: expected RecordContent, got " + ff.content().getClass().getSimpleName(), ff);
        }
        GenericDatumWriter<GenericRecord> datum = new GenericDatumWriter<>(schema);
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(datum)) {
            writer.setCodec(codec);
            writer.create(schema, buf);
            for (Map<String, Object> record : rc.records()) {
                writer.append(AvroConversion.toGenericRecord(record, schema));
            }
            writer.flush();
            return ProcessorResult.single(ff.withContent(new RawContent(buf.toByteArray())));
        } catch (IOException ex) {
            return ProcessorResult.failure("ConvertRecordToOCF: write failed — " + ex.getMessage(), ff);
        }
    }
}
