package zincflow.processors;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import zincflow.core.FlowFile;
import zincflow.core.Processor;
import zincflow.core.ProcessorResult;
import zincflow.core.RawContent;
import zincflow.core.RecordContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/// Object Container File (OCF, the on-disk Avro format) → RecordContent.
/// The schema is embedded in the file header, so no config is required.
/// Surfaces the schema JSON as the {@code avro.schema} attribute so a
/// downstream ConvertRecordToOCF / ConvertRecordToAvro can round-trip
/// without duplicating schema config.
public final class ConvertOCFToRecord implements Processor {

    @Override
    public ProcessorResult process(FlowFile ff) {
        if (!(ff.content() instanceof RawContent raw)) {
            return ProcessorResult.failure(
                    "ConvertOCFToRecord: expected RawContent, got " + ff.content().getClass().getSimpleName(), ff);
        }
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>();
        try (DataFileReader<GenericRecord> file =
                     new DataFileReader<>(new SeekableByteArrayInput(raw.bytes()), reader)) {
            List<Map<String, Object>> records = new ArrayList<>();
            GenericRecord record = null;
            while (file.hasNext()) {
                record = file.next(record); // reuse instance for perf
                records.add(AvroConversion.toMap(record));
            }
            String schemaJson = file.getSchema().toString();
            return ProcessorResult.single(
                    ff.withContent(new RecordContent(records))
                      .withAttribute("record.count", String.valueOf(records.size()))
                      .withAttribute("avro.schema", schemaJson));
        } catch (IOException ex) {
            return ProcessorResult.failure("ConvertOCFToRecord: read failed — " + ex.getMessage(), ff);
        }
    }
}
