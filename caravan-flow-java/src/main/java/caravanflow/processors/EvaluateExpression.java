package caravanflow.processors;

import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.MapContext;
import caravanflow.core.FlowFile;
import caravanflow.core.Processor;
import caravanflow.core.ProcessorResult;
import caravanflow.core.RecordContent;

import java.util.List;
import java.util.Map;

/// Evaluates an Apache Commons JEXL 3 expression against the FlowFile's
/// attributes (and the first record, when the payload is RecordContent).
/// Stores the string form of the result in the {@code targetAttribute}.
///
/// Variables visible in the expression:
/// <ul>
///   <li>{@code attributes} — {@code Map<String,String>} of FlowFile attributes
///   <li>{@code record}     — first record as a {@code Map<String,Object>}
///                             (null when payload is not RecordContent)
///   <li>{@code records}    — full list of records (same caveat)
///   <li>{@code id}         — FlowFile id as a long
///   <li>{@code contentSize} — Content size (bytes or record count). Named
///                             {@code contentSize} not {@code size} to avoid
///                             JEXL's reserved {@code size} operator.
/// </ul>
///
/// JEXL gives us arithmetic, string ops, booleans, ternary, null-safe
/// access, and lambdas — a production-grade expression surface without
/// a hand-rolled parser.
public final class EvaluateExpression implements Processor {

    private static final JexlEngine ENGINE = new JexlBuilder()
            .strict(false) // Undefined vars evaluate to null rather than throwing.
            .safe(true)
            .silent(false) // Still surface syntax errors.
            .create();

    private final JexlExpression expression;
    private final String targetAttribute;

    public EvaluateExpression(String expressionSource, String targetAttribute) {
        if (expressionSource == null || expressionSource.isEmpty()) {
            throw new IllegalArgumentException("EvaluateExpression: expression must not be blank");
        }
        if (targetAttribute == null || targetAttribute.isEmpty()) {
            throw new IllegalArgumentException("EvaluateExpression: targetAttribute must not be blank");
        }
        try {
            this.expression = ENGINE.createExpression(expressionSource);
        } catch (JexlException ex) {
            throw new IllegalArgumentException(
                    "EvaluateExpression: invalid JEXL — " + ex.getMessage(), ex);
        }
        this.targetAttribute = targetAttribute;
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        MapContext ctx = new MapContext();
        ctx.set("attributes", ff.attributes());
        ctx.set("id", ff.id());
        ctx.set("contentSize", ff.content().size());
        if (ff.content() instanceof RecordContent rc) {
            List<Map<String, Object>> records = rc.records();
            ctx.set("records", records);
            ctx.set("record", records.isEmpty() ? null : records.getFirst());
        } else {
            ctx.set("records", List.of());
            ctx.set("record", null);
        }
        try {
            Object value = expression.evaluate(ctx);
            String asString = value == null ? "" : String.valueOf(value);
            return ProcessorResult.single(ff.withAttribute(targetAttribute, asString));
        } catch (JexlException ex) {
            return ProcessorResult.failure(
                    "EvaluateExpression: evaluation failed — " + ex.getMessage(), ff);
        }
    }
}
