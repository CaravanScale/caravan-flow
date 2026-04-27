package zincflow.processors;

import org.apache.avro.Schema;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.MapContext;
import zincflow.core.FlowFile;
import zincflow.core.Processor;
import zincflow.core.ProcessorResult;
import zincflow.core.RecordContent;
import zincflow.core.SchemaDefs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

/// Applies a semicolon-delimited operations palette to every record in a
/// RecordContent payload. Mirrors zinc-flow-csharp's
/// {@code TransformRecord} (StdLib/ExpressionProcessors.cs:177-333).
///
/// Operations:
/// <ul>
///   <li>{@code rename:oldName:newName}     — rename a field
///   <li>{@code remove:fieldName}           — remove a field
///   <li>{@code add:fieldName:value}        — add a string-typed field with literal value
///   <li>{@code copy:source:target}         — copy field value (preserves type)
///   <li>{@code toUpper:fieldName}          — uppercase a string field
///   <li>{@code toLower:fieldName}          — lowercase a string field
///   <li>{@code default:fieldName:value}    — set field to string value if missing/null
///   <li>{@code compute:targetField:expr}   — evaluate JEXL expression, assign result
/// </ul>
///
/// {@code compute} uses Apache Commons JEXL (arithmetic, booleans, ternary,
/// string ops); C#'s track currently uses a lighter template DSL and is
/// tracked to catch up in the post-Java cohort. Every other op is bit-for-bit
/// portable across tracks.
public final class TransformRecord implements Processor {

    private static final JexlEngine JEXL = new JexlBuilder()
            .strict(false).safe(true).silent(false).create();

    private static final Set<String> KNOWN_OPS = Set.copyOf(Arrays.asList(
            "rename", "remove", "add", "copy", "toUpper", "toLower", "default", "compute"));

    private record Directive(String op, String arg1, String arg2, JexlExpression compiled) {}

    private final List<Directive> directives;

    public TransformRecord(String operationsSpec) {
        if (operationsSpec == null || operationsSpec.isBlank()) {
            throw new IllegalArgumentException(
                    "TransformRecord: operations must have at least one directive");
        }
        List<Directive> parsed = new ArrayList<>();
        for (String raw : operationsSpec.split(";")) {
            String directive = raw.trim();
            if (directive.isEmpty()) continue;
            // Split into up to 3 parts so arg2 can legally contain colons
            // (e.g. compute expressions with dotted paths).
            String[] parts = directive.split(":", 3);
            if (parts.length < 2) {
                throw new IllegalArgumentException(
                        "TransformRecord: malformed directive '" + directive
                                + "' — expected 'op:arg1' or 'op:arg1:arg2'");
            }
            String op = parts[0];
            if (!KNOWN_OPS.contains(op)) {
                throw new IllegalArgumentException(
                        "TransformRecord: unknown op '" + op + "' in directive '" + directive
                                + "' — valid: " + String.join(", ", KNOWN_OPS));
            }
            String arg1 = parts[1];
            String arg2 = parts.length > 2 ? parts[2] : "";
            JexlExpression compiled = null;
            if ("compute".equals(op)) {
                if (arg2.isBlank()) {
                    throw new IllegalArgumentException(
                            "TransformRecord: compute requires an expression — 'compute:target:expr'");
                }
                try {
                    compiled = JEXL.createExpression(arg2);
                } catch (JexlException ex) {
                    throw new IllegalArgumentException(
                            "TransformRecord: invalid JEXL in compute '" + arg2 + "' — " + ex.getMessage(), ex);
                }
            }
            parsed.add(new Directive(op, arg1, arg2, compiled));
        }
        if (parsed.isEmpty()) {
            throw new IllegalArgumentException("TransformRecord: operations spec produced no directives");
        }
        this.directives = List.copyOf(parsed);
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        if (!(ff.content() instanceof RecordContent rc)) {
            return ProcessorResult.failure(
                    "TransformRecord: expected RecordContent, got " + ff.content().getClass().getSimpleName(), ff);
        }
        if (rc.records().isEmpty()) {
            return ProcessorResult.single(ff);
        }

        List<Map<String, Object>> transformed = new ArrayList<>(rc.records().size());
        for (Map<String, Object> record : rc.records()) {
            Map<String, Object> dict = new LinkedHashMap<>(record);
            for (Directive d : directives) {
                applyOp(dict, d);
            }
            transformed.add(dict);
        }

        // Re-infer schema from first record post-transform. Unlike C# we
        // don't track per-field original Avro types — the safest choice
        // is a fresh inferred schema, which covers the common cases
        // (add/compute adds typed fields; rename/copy preserves types
        // through the map; remove drops fields cleanly).
        String schemaName = rc.schema() == null ? "TransformedRecord" : rc.schema().getName();
        Schema outSchema = inferSchema(schemaName, transformed.getFirst());

        return ProcessorResult.single(ff.withContent(new RecordContent(transformed, outSchema)));
    }

    private void applyOp(Map<String, Object> dict, Directive d) {
        switch (d.op()) {
            case "rename" -> {
                if (dict.containsKey(d.arg1())) dict.put(d.arg2(), dict.remove(d.arg1()));
            }
            case "remove" -> dict.remove(d.arg1());
            case "add"    -> dict.put(d.arg1(), d.arg2());
            case "copy"   -> {
                if (dict.containsKey(d.arg1())) dict.put(d.arg2(), dict.get(d.arg1()));
            }
            case "toUpper" -> {
                if (dict.get(d.arg1()) instanceof String s) dict.put(d.arg1(), s.toUpperCase());
            }
            case "toLower" -> {
                if (dict.get(d.arg1()) instanceof String s) dict.put(d.arg1(), s.toLowerCase());
            }
            case "default" -> {
                if (!dict.containsKey(d.arg1()) || dict.get(d.arg1()) == null) dict.put(d.arg1(), d.arg2());
            }
            case "compute" -> {
                MapContext ctx = new MapContext();
                ctx.set("record", dict);
                for (var e : dict.entrySet()) ctx.set(e.getKey(), e.getValue());
                try {
                    dict.put(d.arg1(), d.compiled().evaluate(ctx));
                } catch (JexlException ex) {
                    // C# silently skips failed computes — match that so
                    // one bad record doesn't crash the whole batch.
                }
            }
            default -> { /* unreachable — parse-time validation rejects unknown ops */ }
        }
    }

    /// Build a flat Avro Schema from a record's keys + value types.
    /// Matches the inference logic in ConvertJSONToRecord / ConvertCSVToRecord.
    private static Schema inferSchema(String name, Map<String, Object> record) {
        if (record.isEmpty()) return null;
        StringJoiner defs = new StringJoiner(",");
        Set<String> seen = new HashSet<>();
        for (var entry : record.entrySet()) {
            if (!seen.add(entry.getKey())) continue;
            defs.add(entry.getKey() + ":" + inferType(entry.getValue()));
        }
        return SchemaDefs.parse(name, defs.toString());
    }

    private static String inferType(Object v) {
        if (v == null) return "string";
        if (v instanceof Boolean) return "boolean";
        if (v instanceof Integer) return "int";
        if (v instanceof Long) return "long";
        if (v instanceof Float) return "float";
        if (v instanceof Double) return "double";
        if (v instanceof byte[]) return "bytes";
        return "string";
    }
}
