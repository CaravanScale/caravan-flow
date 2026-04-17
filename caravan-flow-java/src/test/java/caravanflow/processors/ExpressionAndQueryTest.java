package caravanflow.processors;

import org.junit.jupiter.api.Test;
import caravanflow.core.FlowFile;
import caravanflow.core.ProcessorResult;
import caravanflow.core.RecordContent;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class ExpressionAndQueryTest {

    // --- EvaluateExpression ---

    @Test
    void evaluateExpressionReadsAttributes() {
        var proc = new EvaluateExpression("attributes['priority'] + '/' + attributes['tenant']", "routing.key");
        var ff = FlowFile.create(new byte[0], Map.of("priority", "high", "tenant", "acme"));
        var out = (ProcessorResult.Single) proc.process(ff);
        assertEquals("high/acme", out.flowFile().attributes().get("routing.key"));
    }

    @Test
    void evaluateExpressionDoesArithmetic() {
        var proc = new EvaluateExpression("contentSize * 2 + 1", "doubled");
        var ff = FlowFile.create(new byte[]{1, 2, 3, 4, 5}, Map.of());
        var out = (ProcessorResult.Single) proc.process(ff);
        assertEquals("11", out.flowFile().attributes().get("doubled"));
    }

    @Test
    void evaluateExpressionReadsFirstRecord() {
        var records = List.<Map<String, Object>>of(Map.of("amount", 42));
        var ff = FlowFile.create(new RecordContent(records), Map.of());
        var proc = new EvaluateExpression("record['amount'] > 10 ? 'big' : 'small'", "classification");
        var out = (ProcessorResult.Single) proc.process(ff);
        assertEquals("big", out.flowFile().attributes().get("classification"));
    }

    @Test
    void evaluateExpressionNullSafeOnMissingAttribute() {
        var proc = new EvaluateExpression("attributes['nope']", "out");
        var ff = FlowFile.create(new byte[0], Map.of());
        var out = (ProcessorResult.Single) proc.process(ff);
        assertEquals("", out.flowFile().attributes().get("out"));
    }

    @Test
    void evaluateExpressionInvalidSyntaxRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new EvaluateExpression("not ))) valid (((", "x"));
    }

    @Test
    void evaluateExpressionBlankArgsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new EvaluateExpression("", "x"));
        assertThrows(IllegalArgumentException.class, () -> new EvaluateExpression("1", ""));
    }

    // --- TransformRecord ---

    @Test
    void transformRecordRewritesFields() {
        var records = List.<Map<String, Object>>of(
                mutable("name", "alice", "score", 10),
                mutable("name", "bob", "score", 20));
        var ff = FlowFile.create(new RecordContent(records), Map.of());
        var proc = new TransformRecord(Map.of(
                "score", "record['score'] * 2",
                "label", "record['name'] + '!'"));
        var out = (ProcessorResult.Single) proc.process(ff);
        var rc = (RecordContent) out.flowFile().content();

        assertEquals(20, ((Number) rc.records().get(0).get("score")).intValue());
        assertEquals("alice!", rc.records().get(0).get("label"));
        assertEquals(40, ((Number) rc.records().get(1).get("score")).intValue());
        // Untouched fields pass through.
        assertEquals("alice", rc.records().get(0).get("name"));
    }

    @Test
    void transformRecordUsesAttributesContext() {
        var records = List.<Map<String, Object>>of(mutable("v", 1));
        var ff = FlowFile.create(new RecordContent(records), Map.of("multiplier", "5"));
        // attributes['multiplier'] is a String; JEXL coerces in arithmetic.
        var proc = new TransformRecord(Map.of("v", "record['v'] * attributes['multiplier']"));
        var out = (ProcessorResult.Single) proc.process(ff);
        var rc = (RecordContent) out.flowFile().content();
        assertEquals(5, ((Number) rc.records().getFirst().get("v")).intValue());
    }

    @Test
    void transformRecordOnNonRecordContentFails() {
        var ff = FlowFile.create(new byte[]{1, 2}, Map.of());
        var proc = new TransformRecord(Map.of("x", "1"));
        assertInstanceOf(ProcessorResult.Failure.class, proc.process(ff));
    }

    @Test
    void transformRecordEmptyMapRejected() {
        assertThrows(IllegalArgumentException.class, () -> new TransformRecord(Map.of()));
        assertThrows(IllegalArgumentException.class, () -> new TransformRecord(null));
    }

    @Test
    void transformRecordInvalidJexlRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new TransformRecord(Map.of("bad", "this is ))) not valid (((")));
    }

    private static Map<String, Object> mutable(Object... keyValues) {
        var m = new LinkedHashMap<String, Object>();
        for (int i = 0; i < keyValues.length; i += 2) {
            m.put((String) keyValues[i], keyValues[i + 1]);
        }
        return m;
    }

    // --- QueryRecord ---

    @Test
    void queryRecordMatchesHighPriority() {
        var records = List.<Map<String, Object>>of(
                Map.of("id", 1, "priority", "high"),
                Map.of("id", 2, "priority", "low"),
                Map.of("id", 3, "priority", "high"));
        var ff = FlowFile.create(new RecordContent(records), Map.of());
        var proc = new QueryRecord("$[?(@.priority == 'high')]");
        var out = (ProcessorResult.Routed) proc.process(ff);
        assertEquals("matched", out.route());
        var rc = (RecordContent) out.flowFile().content();
        assertEquals(2, rc.records().size());
    }

    @Test
    void queryRecordNoMatchRoutesUnmatched() {
        var records = List.<Map<String, Object>>of(Map.of("priority", "low"));
        var ff = FlowFile.create(new RecordContent(records), Map.of());
        var proc = new QueryRecord("$[?(@.priority == 'urgent')]");
        var out = (ProcessorResult.Routed) proc.process(ff);
        assertEquals("unmatched", out.route());
    }

    @Test
    void queryRecordOnNonRecordContentFails() {
        var ff = FlowFile.create(new byte[]{1, 2}, Map.of());
        assertInstanceOf(ProcessorResult.Failure.class, new QueryRecord("$").process(ff));
    }

    @Test
    void queryRecordInvalidQueryRejected() {
        assertThrows(IllegalArgumentException.class, () -> new QueryRecord(""));
        // Note: JsonPath's compile is lenient — truly invalid syntax like "][["
        // surfaces at compile time.
        assertThrows(IllegalArgumentException.class, () -> new QueryRecord("][?("));
    }
}
