package caravanflow.processors;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.avro.Schema;
import caravanflow.core.FlowFile;
import caravanflow.core.Processor;
import caravanflow.core.ProcessorResult;
import caravanflow.core.RawContent;
import caravanflow.core.RecordContent;
import caravanflow.core.SchemaDefs;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/// Parses CSV text into {@link RecordContent}. Mirrors caravan-flow-csharp's
/// {@code ConvertCSVToRecord} (StdLib/RecordProcessors.cs:301-340).
///
/// Config:
///   schemaName — record name label (optional)
///   delimiter  — single char (default ',')
///   hasHeader  — first row is a header (default true)
///   fields     — compact Avro field defs ({@code "name:type,name:type"});
///                when set, drives the output schema and column order.
///                When absent, a string-typed schema is inferred from the
///                header row or the `hasHeader=false` raw shape.
public final class ConvertCSVToRecord implements Processor {

    private static final CsvMapper MAPPER = new CsvMapper();

    private final String schemaName;
    private final char delimiter;
    private final boolean hasHeader;
    private final Schema explicitSchema;

    public ConvertCSVToRecord() { this("", ',', true, ""); }

    public ConvertCSVToRecord(String schemaName, char delimiter, boolean hasHeader, String fieldDefs) {
        this.schemaName = schemaName == null ? "" : schemaName;
        this.delimiter = delimiter;
        this.hasHeader = hasHeader;
        this.explicitSchema = SchemaDefs.parse(this.schemaName, fieldDefs);
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        if (!(ff.content() instanceof RawContent raw)) {
            return ProcessorResult.failure(
                    "ConvertCSVToRecord: expected RawContent, got " + ff.content().getClass().getSimpleName(), ff);
        }
        CsvSchema.Builder sb = CsvSchema.builder().setColumnSeparator(delimiter);
        if (explicitSchema != null) {
            for (Schema.Field f : explicitSchema.getFields()) sb.addColumn(f.name());
            sb.setUseHeader(hasHeader); // header row consumed but schema-field order wins
        } else if (hasHeader) {
            sb = sb.setUseHeader(true);
        }
        CsvSchema csvSchema = sb.build();

        try {
            String text = new String(raw.bytes(), StandardCharsets.UTF_8);
            MappingIterator<Map<String, Object>> it = MAPPER
                    .readerFor(Map.class)
                    .with(csvSchema)
                    .readValues(text);
            List<Map<String, Object>> records = new ArrayList<>();
            while (it.hasNext()) records.add(it.next());
            Schema effective = explicitSchema != null ? explicitSchema : inferStringSchema(records);
            return ProcessorResult.single(
                    ff.withContent(new RecordContent(records, effective))
                      .withAttribute("record.count", String.valueOf(records.size())));
        } catch (IOException ex) {
            return ProcessorResult.failure("ConvertCSVToRecord: parse failed — " + ex.getMessage(), ff);
        }
    }

    /// Build a string-typed schema from the first record's keys when no
    /// explicit field defs were provided. CSV values land as strings
    /// (no type inference without a schema hint) so this is a faithful
    /// representation of what's in the RecordContent.
    private Schema inferStringSchema(List<Map<String, Object>> records) {
        if (records.isEmpty()) return null;
        StringBuilder defs = new StringBuilder();
        boolean first = true;
        for (String key : records.getFirst().keySet()) {
            if (!first) defs.append(',');
            defs.append(key).append(":string");
            first = false;
        }
        String name = schemaName.isEmpty() ? "CsvRecord" : schemaName;
        return SchemaDefs.parse(name, defs.toString());
    }
}
