package zincflow.processors;

import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.MapContext;
import zincflow.core.FlowFile;
import zincflow.core.Processor;
import zincflow.core.ProcessorResult;
import zincflow.core.RecordContent;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Applies a map of {@code fieldName → JEXL expression} to every record
/// in a RecordContent payload. The current record is bound as
/// {@code record} in the JEXL context; access other fields with
/// {@code record.fieldName} (dotted) or {@code record['field name']}
/// (bracket).
///
/// The resulting record is the original record with each expression-key
/// overwritten/added with the evaluation result. Fields not in the
/// transforms map pass through unchanged — use an empty-string
/// expression to drop a field (sets the value to empty).
public final class TransformRecord implements Processor {

    private static final JexlEngine ENGINE = new JexlBuilder()
            .strict(false)
            .safe(true)
            .silent(false)
            .create();

    private final Map<String, JexlExpression> transforms;

    public TransformRecord(Map<String, String> transformsSource) {
        if (transformsSource == null || transformsSource.isEmpty()) {
            throw new IllegalArgumentException(
                    "TransformRecord: transforms map must have at least one field mapping");
        }
        Map<String, JexlExpression> compiled = new LinkedHashMap<>();
        for (var entry : transformsSource.entrySet()) {
            try {
                compiled.put(entry.getKey(), ENGINE.createExpression(entry.getValue()));
            } catch (JexlException ex) {
                throw new IllegalArgumentException(
                        "TransformRecord: invalid JEXL for field '" + entry.getKey() + "' — " + ex.getMessage(), ex);
            }
        }
        this.transforms = Map.copyOf(compiled);
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        if (!(ff.content() instanceof RecordContent rc)) {
            return ProcessorResult.failure(
                    "TransformRecord: expected RecordContent, got " + ff.content().getClass().getSimpleName(), ff);
        }
        List<Map<String, Object>> out = new ArrayList<>(rc.records().size());
        int idx = 0;
        for (Map<String, Object> record : rc.records()) {
            Map<String, Object> next = new LinkedHashMap<>(record);
            MapContext ctx = new MapContext();
            ctx.set("record", record);
            ctx.set("attributes", ff.attributes());
            ctx.set("index", idx++);
            for (var t : transforms.entrySet()) {
                try {
                    next.put(t.getKey(), t.getValue().evaluate(ctx));
                } catch (JexlException ex) {
                    return ProcessorResult.failure(
                            "TransformRecord: field '" + t.getKey() + "' failed on record " + (idx - 1)
                                    + " — " + ex.getMessage(), ff);
                }
            }
            out.add(next);
        }
        return ProcessorResult.single(ff.withContent(new RecordContent(out)));
    }
}
