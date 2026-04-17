package caravanflow.processors;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import caravanflow.core.FlowFile;
import caravanflow.core.Processor;
import caravanflow.core.ProcessorResult;
import caravanflow.core.RawContent;
import caravanflow.core.RecordContent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/// Parses CSV text into {@link RecordContent}. By default the first row
/// is the header and column names become map keys. Configurable
/// delimiter (default comma) and optional header override if your CSV
/// doesn't include one.
///
/// Backed by Jackson's {@code jackson-dataformat-csv} — standard Java
/// CSV library, no hand-rolling, handles quoted values and escapes.
public final class ConvertCSVToRecord implements Processor {

    private static final CsvMapper MAPPER = new CsvMapper();

    private final char delimiter;
    private final boolean firstRowHeader;
    private final List<String> explicitColumns;

    public ConvertCSVToRecord() { this(',', true, List.of()); }

    public ConvertCSVToRecord(char delimiter, boolean firstRowHeader, List<String> explicitColumns) {
        this.delimiter = delimiter;
        this.firstRowHeader = firstRowHeader;
        this.explicitColumns = explicitColumns == null ? List.of() : List.copyOf(explicitColumns);
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        if (!(ff.content() instanceof RawContent raw)) {
            return ProcessorResult.failure(
                    "ConvertCSVToRecord: expected RawContent, got " + ff.content().getClass().getSimpleName(), ff);
        }
        CsvSchema.Builder sb = CsvSchema.builder().setColumnSeparator(delimiter);
        if (firstRowHeader && explicitColumns.isEmpty()) {
            sb = sb.setUseHeader(true);
        } else {
            for (String c : explicitColumns) sb.addColumn(c);
            sb.setUseHeader(!explicitColumns.isEmpty() && firstRowHeader);
        }
        CsvSchema schema = sb.build();
        try {
            String text = new String(raw.bytes(), StandardCharsets.UTF_8);
            MappingIterator<Map<String, Object>> it = MAPPER
                    .readerFor(Map.class)
                    .with(schema)
                    .readValues(text);
            List<Map<String, Object>> records = new ArrayList<>();
            while (it.hasNext()) records.add(it.next());
            return ProcessorResult.single(
                    ff.withContent(new RecordContent(records))
                      .withAttribute("record.count", String.valueOf(records.size())));
        } catch (IOException ex) {
            return ProcessorResult.failure("ConvertCSVToRecord: parse failed — " + ex.getMessage(), ff);
        }
    }
}
