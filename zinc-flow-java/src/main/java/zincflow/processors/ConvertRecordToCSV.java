package zincflow.processors;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import zincflow.core.FlowFile;
import zincflow.core.Processor;
import zincflow.core.ProcessorResult;
import zincflow.core.RawContent;
import zincflow.core.RecordContent;

import java.util.List;
import java.util.Map;

/// Serializes {@link RecordContent} to CSV text using Jackson's
/// {@code jackson-dataformat-csv}. Column order is taken from the first
/// record's key insertion order (LinkedHashMap preserves this from
/// JSON/Avro reads) or from {@code explicitColumns} config. A header
/// row is written unless {@code writeHeader=false}.
public final class ConvertRecordToCSV implements Processor {

    private static final CsvMapper MAPPER = new CsvMapper();

    private final char delimiter;
    private final boolean writeHeader;
    private final List<String> explicitColumns;

    public ConvertRecordToCSV() { this(',', true, List.of()); }

    public ConvertRecordToCSV(char delimiter, boolean writeHeader, List<String> explicitColumns) {
        this.delimiter = delimiter;
        this.writeHeader = writeHeader;
        this.explicitColumns = explicitColumns == null ? List.of() : List.copyOf(explicitColumns);
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        if (!(ff.content() instanceof RecordContent rc)) {
            return ProcessorResult.failure(
                    "ConvertRecordToCSV: expected RecordContent, got " + ff.content().getClass().getSimpleName(), ff);
        }
        List<Map<String, Object>> records = rc.records();

        List<String> columns = explicitColumns;
        if (columns.isEmpty()) {
            if (records.isEmpty()) {
                // Empty record list + no explicit schema → emit an empty payload.
                return ProcessorResult.single(ff.withContent(new RawContent(new byte[0])));
            }
            columns = List.copyOf(records.getFirst().keySet());
        }

        CsvSchema.Builder sb = CsvSchema.builder().setColumnSeparator(delimiter);
        for (String c : columns) sb.addColumn(c);
        CsvSchema schema = sb.build().withUseHeader(writeHeader);

        try {
            byte[] bytes = MAPPER.writer(schema).writeValueAsBytes(records);
            return ProcessorResult.single(ff.withContent(new RawContent(bytes)));
        } catch (Exception ex) {
            return ProcessorResult.failure("ConvertRecordToCSV: serialize failed — " + ex.getMessage(), ff);
        }
    }
}
