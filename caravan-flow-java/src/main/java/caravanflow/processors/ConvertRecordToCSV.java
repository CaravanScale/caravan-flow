package caravanflow.processors;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import caravanflow.core.FlowFile;
import caravanflow.core.Processor;
import caravanflow.core.ProcessorResult;
import caravanflow.core.RawContent;
import caravanflow.core.RecordContent;

import java.util.List;
import java.util.Map;

/// Serializes {@link RecordContent} to CSV text. Mirrors caravan-flow-csharp's
/// {@code ConvertRecordToCSV} (StdLib/RecordProcessors.cs:346-364).
///
/// Config:
///   delimiter     — single char (default ',')
///   includeHeader — write the column header row (default true)
///
/// Column order is driven by the first record's key insertion order
/// (LinkedHashMap preserves this from JSON/Avro reads). No explicit
/// column-override config — matches C#, which relies on the
/// RecordContent schema.
public final class ConvertRecordToCSV implements Processor {

    private static final CsvMapper MAPPER = new CsvMapper();

    private final char delimiter;
    private final boolean includeHeader;

    public ConvertRecordToCSV() { this(',', true); }

    public ConvertRecordToCSV(char delimiter, boolean includeHeader) {
        this.delimiter = delimiter;
        this.includeHeader = includeHeader;
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        if (!(ff.content() instanceof RecordContent rc)) {
            return ProcessorResult.failure(
                    "ConvertRecordToCSV: expected RecordContent, got " + ff.content().getClass().getSimpleName(), ff);
        }
        List<Map<String, Object>> records = rc.records();

        if (records.isEmpty()) {
            // Empty record list → empty payload. Can't emit a header
            // without a first record to source keys from.
            return ProcessorResult.single(ff.withContent(new RawContent(new byte[0])));
        }

        List<String> columns = List.copyOf(records.getFirst().keySet());
        CsvSchema.Builder sb = CsvSchema.builder().setColumnSeparator(delimiter);
        for (String c : columns) sb.addColumn(c);
        CsvSchema schema = sb.build().withUseHeader(includeHeader);

        try {
            byte[] bytes = MAPPER.writer(schema).writeValueAsBytes(records);
            return ProcessorResult.single(ff.withContent(new RawContent(bytes)));
        } catch (Exception ex) {
            return ProcessorResult.failure("ConvertRecordToCSV: serialize failed — " + ex.getMessage(), ff);
        }
    }
}
