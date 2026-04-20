package caravanflow.processors;

import caravanflow.core.FlowFile;
import caravanflow.core.Processor;
import caravanflow.core.ProcessorResult;
import caravanflow.core.RecordContent;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.MapContext;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Set or derive record fields via expressions. Record-level counterpart
/// to {@code EvaluateExpression} (which targets FlowFile attributes).
/// Config key {@code updates} is a semicolon-delimited list of
/// {@code fieldName = expression} pairs evaluated left-to-right against
/// a mutable working copy of each record — later expressions see earlier
/// writes.
///
/// Mirrors caravan-flow-csharp's {@code UpdateRecord}. Uses Apache
/// Commons JEXL on both track sides; syntax (arithmetic, string ops,
/// ternary, coalesce) is portable between Java and C#.
public final class UpdateRecord implements Processor {

    private static final JexlEngine JEXL = new JexlBuilder()
            .strict(false).safe(true).silent(false).create();

    private record Update(String field, JexlExpression compiled) {}

    private final List<Update> updates;

    public UpdateRecord(String spec) {
        List<Update> parsed = new ArrayList<>();
        if (spec != null) {
            for (String pair : spec.split(";")) {
                String trimmed = pair.trim();
                if (trimmed.isEmpty()) continue;
                int eq = trimmed.indexOf('=');
                if (eq <= 0) {
                    throw new IllegalArgumentException(
                            "UpdateRecord: malformed pair '" + trimmed + "' — expected 'field = expression'");
                }
                String field = trimmed.substring(0, eq).trim();
                String exprStr = trimmed.substring(eq + 1).trim();
                if (field.isEmpty()) {
                    throw new IllegalArgumentException(
                            "UpdateRecord: empty field name in '" + trimmed + "'");
                }
                if (exprStr.isEmpty()) {
                    throw new IllegalArgumentException(
                            "UpdateRecord: empty expression for field '" + field + "'");
                }
                try {
                    parsed.add(new Update(field, JEXL.createExpression(exprStr)));
                } catch (JexlException ex) {
                    throw new IllegalArgumentException(
                            "UpdateRecord: field '" + field + "' has invalid expression '" + exprStr + "': "
                                    + ex.getMessage(), ex);
                }
            }
        }
        if (parsed.isEmpty()) {
            throw new IllegalArgumentException("UpdateRecord: updates spec produced no entries");
        }
        this.updates = List.copyOf(parsed);
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        if (!(ff.content() instanceof RecordContent rc)) {
            return ProcessorResult.single(ff);
        }
        if (rc.records().isEmpty()) {
            return ProcessorResult.single(ff);
        }

        List<Map<String, Object>> out = new ArrayList<>(rc.records().size());
        for (Map<String, Object> record : rc.records()) {
            Map<String, Object> dict = new LinkedHashMap<>(record);
            for (Update u : updates) {
                MapContext ctx = new MapContext();
                ctx.set("record", dict);
                for (var e : dict.entrySet()) ctx.set(e.getKey(), e.getValue());
                Object val;
                try { val = u.compiled().evaluate(ctx); }
                catch (JexlException ex) { val = null; }
                dict.put(u.field(), val);
            }
            out.add(dict);
        }

        String schemaName = rc.schema() == null ? "UpdatedRecord" : rc.schema().getName();
        Schema outSchema = inferSchema(schemaName, out.getFirst());
        return ProcessorResult.single(ff.withContent(new RecordContent(out, outSchema)));
    }

    private static Schema inferSchema(String name, Map<String, Object> record) {
        if (record.isEmpty()) return null;
        SchemaBuilder.FieldAssembler<Schema> fa = SchemaBuilder.record(name).fields();
        for (var e : record.entrySet()) {
            Object v = e.getValue();
            if (v == null) fa = fa.nullableString(e.getKey(), "");
            else if (v instanceof Boolean) fa = fa.optionalBoolean(e.getKey());
            else if (v instanceof Integer) fa = fa.optionalInt(e.getKey());
            else if (v instanceof Long) fa = fa.optionalLong(e.getKey());
            else if (v instanceof Float) fa = fa.optionalFloat(e.getKey());
            else if (v instanceof Double) fa = fa.optionalDouble(e.getKey());
            else fa = fa.optionalString(e.getKey());
        }
        return fa.endRecord();
    }
}
