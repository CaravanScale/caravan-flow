package zincflow.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import zincflow.core.FlowFile;
import zincflow.core.Processor;
import zincflow.core.ProcessorResult;
import zincflow.core.RawContent;
import zincflow.core.RecordContent;

import java.nio.charset.StandardCharsets;

/// Serializes a {@link RecordContent} payload back to JSON bytes in a
/// {@link RawContent}. The wrapping shape is controlled by
/// {@code singleObject}: when false (default), emits a JSON array; when
/// true, emits the first record as a bare object (useful when a pipeline
/// fans back to HTTP APIs that don't accept arrays).
public final class ConvertRecordToJSON implements Processor {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final boolean singleObject;

    public ConvertRecordToJSON() { this(false); }

    public ConvertRecordToJSON(boolean singleObject) {
        this.singleObject = singleObject;
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        if (!(ff.content() instanceof RecordContent rc)) {
            return ProcessorResult.failure(
                    "ConvertRecordToJSON: expected RecordContent, got " + ff.content().getClass().getSimpleName(), ff);
        }
        try {
            byte[] bytes;
            if (singleObject) {
                if (rc.records().isEmpty()) {
                    bytes = "{}".getBytes(StandardCharsets.UTF_8);
                } else {
                    bytes = MAPPER.writeValueAsBytes(rc.records().getFirst());
                }
            } else {
                bytes = MAPPER.writeValueAsBytes(rc.records());
            }
            return ProcessorResult.single(ff.withContent(new RawContent(bytes)));
        } catch (JsonProcessingException ex) {
            return ProcessorResult.failure("ConvertRecordToJSON: serialize failed — " + ex.getMessage(), ff);
        }
    }
}
