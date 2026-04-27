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

/// {@link RecordContent} → OCF bytes (Object Container File). Mirrors
/// zinc-flow-csharp's {@code ConvertRecordToOCF} (StdLib/RecordProcessors.cs:259-283):
/// no schema config — the schema already on the RecordContent is what
/// gets embedded in the OCF header.
///
/// Config:
///   codec — compression codec; {@code null} (default, no compression),
///           {@code deflate}, {@code snappy}, {@code bzip2}, {@code xz},
///           {@code zstandard}. Passed through to
///           {@link CodecFactory#fromString(String)}.
public final class ConvertRecordToOCF implements Processor {

    private final CodecFactory codec;

    public ConvertRecordToOCF() { this("null"); }

    public ConvertRecordToOCF(String codecName) {
        this.codec = CodecFactory.fromString(codecName == null || codecName.isEmpty() ? "null" : codecName);
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        if (!(ff.content() instanceof RecordContent rc)) {
            return ProcessorResult.failure(
                    "ConvertRecordToOCF: expected RecordContent, got " + ff.content().getClass().getSimpleName(), ff);
        }
        if (rc.records().isEmpty()) {
            return ProcessorResult.single(ff);
        }
        Schema schema = rc.schema();
        if (schema == null) {
            return ProcessorResult.failure(
                    "ConvertRecordToOCF: RecordContent has no schema — upstream must declare one", ff);
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
