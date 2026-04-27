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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Evaluates one or more Apache Commons JEXL 3 expressions against the
/// FlowFile's attributes (and the first record, when the payload is
/// RecordContent). Each expression produces a FlowFile attribute named
/// by the entry's target.
///
/// Mirrors zinc-flow-csharp's {@code EvaluateExpression} shape
/// (StdLib/ExpressionProcessors.cs:23) — multi-output, target→expression.
/// The expression ENGINE differs: Java uses JEXL (full arithmetic,
/// booleans, ternary, lambdas); C# uses a string-template DSL with a
/// fixed function set. Configs are therefore not yet interoperable;
/// C# is slated to gain arithmetic + booleans in the post-Java cohort.
///
/// Variables visible in each expression:
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
public final class EvaluateExpression implements Processor {

    private static final JexlEngine ENGINE = new JexlBuilder()
            .strict(false) // Undefined vars evaluate to null rather than throwing.
            .safe(true)
            .silent(false) // Still surface syntax errors.
            .create();

    private final Map<String, JexlExpression> expressions;

    public EvaluateExpression(Map<String, String> expressionsByTarget) {
        if (expressionsByTarget == null || expressionsByTarget.isEmpty()) {
            throw new IllegalArgumentException(
                    "EvaluateExpression: expressions map must have at least one target=expression entry");
        }
        Map<String, JexlExpression> compiled = new LinkedHashMap<>();
        for (var entry : expressionsByTarget.entrySet()) {
            String target = entry.getKey();
            String source = entry.getValue();
            if (target == null || target.isBlank()) {
                throw new IllegalArgumentException("EvaluateExpression: target attribute must not be blank");
            }
            if (source == null || source.isBlank()) {
                throw new IllegalArgumentException(
                        "EvaluateExpression: expression for '" + target + "' must not be blank");
            }
            try {
                compiled.put(target, ENGINE.createExpression(source));
            } catch (JexlException ex) {
                throw new IllegalArgumentException(
                        "EvaluateExpression: invalid JEXL for '" + target + "' — " + ex.getMessage(), ex);
            }
        }
        this.expressions = Map.copyOf(compiled);
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
        FlowFile result = ff;
        for (var entry : expressions.entrySet()) {
            try {
                Object value = entry.getValue().evaluate(ctx);
                String asString = value == null ? "" : String.valueOf(value);
                result = result.withAttribute(entry.getKey(), asString);
            } catch (JexlException ex) {
                return ProcessorResult.failure(
                        "EvaluateExpression: evaluation failed for '" + entry.getKey() + "' — " + ex.getMessage(), ff);
            }
        }
        return ProcessorResult.single(result);
    }
}
