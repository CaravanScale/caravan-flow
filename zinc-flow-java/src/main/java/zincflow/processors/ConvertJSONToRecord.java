package zincflow.processors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import zincflow.core.FlowFile;
import zincflow.core.Processor;
import zincflow.core.ProcessorResult;
import zincflow.core.RawContent;
import zincflow.core.RecordContent;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/// Parses the FlowFile's RawContent payload as JSON and upgrades it to
/// {@link RecordContent}. Accepts either a single JSON object (wrapped
/// into a 1-element record list) or a JSON array of objects. Uses
/// Jackson for parsing so every JSON edge case (numbers, nulls, nested
/// objects) works identically to the rest of the Java ecosystem.
public final class ConvertJSONToRecord implements Processor {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> OBJ_TYPE = new TypeReference<>() {};
    private static final TypeReference<List<Map<String, Object>>> ARR_TYPE = new TypeReference<>() {};

    @Override
    public ProcessorResult process(FlowFile ff) {
        if (!(ff.content() instanceof RawContent raw)) {
            return ProcessorResult.failure(
                    "ConvertJSONToRecord: expected RawContent, got " + ff.content().getClass().getSimpleName(), ff);
        }
        String text = new String(raw.bytes(), StandardCharsets.UTF_8).trim();
        if (text.isEmpty()) {
            return ProcessorResult.failure("ConvertJSONToRecord: empty payload", ff);
        }
        try {
            List<Map<String, Object>> records;
            if (text.startsWith("[")) {
                records = MAPPER.readValue(text, ARR_TYPE);
            } else {
                records = List.of(MAPPER.readValue(text, OBJ_TYPE));
            }
            return ProcessorResult.single(
                    ff.withContent(new RecordContent(records))
                      .withAttribute("record.count", String.valueOf(records.size())));
        } catch (Exception ex) {
            return ProcessorResult.failure("ConvertJSONToRecord: parse failed — " + ex.getMessage(), ff);
        }
    }
}
