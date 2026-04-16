package zincflow.processors;

import org.junit.jupiter.api.Test;
import zincflow.core.FlowFile;
import zincflow.core.ProcessorResult;
import zincflow.core.RawContent;
import zincflow.core.RecordContent;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class RecordProcessorsTest {

    // --- ConvertJSONToRecord ---

    @Test
    void convertJsonArrayToRecords() {
        var ff = FlowFile.create("[{\"id\":1,\"name\":\"a\"},{\"id\":2,\"name\":\"b\"}]".getBytes(), Map.of());
        var out = (ProcessorResult.Single) new ConvertJSONToRecord().process(ff);
        var rc = (RecordContent) out.flowFile().content();
        assertEquals(2, rc.records().size());
        assertEquals(1, ((Number) rc.records().get(0).get("id")).intValue());
        assertEquals("b", rc.records().get(1).get("name"));
        assertEquals("2", out.flowFile().attributes().get("record.count"));
    }

    @Test
    void convertJsonSingleObjectWrappedInList() {
        var ff = FlowFile.create("{\"id\":42}".getBytes(), Map.of());
        var out = (ProcessorResult.Single) new ConvertJSONToRecord().process(ff);
        var rc = (RecordContent) out.flowFile().content();
        assertEquals(1, rc.records().size());
    }

    @Test
    void convertJsonMalformedRoutesToFailure() {
        var ff = FlowFile.create("{bad".getBytes(), Map.of());
        var out = new ConvertJSONToRecord().process(ff);
        assertInstanceOf(ProcessorResult.Failure.class, out);
    }

    @Test
    void convertJsonEmptyRoutesToFailure() {
        var ff = FlowFile.create(new byte[0], Map.of());
        assertInstanceOf(ProcessorResult.Failure.class, new ConvertJSONToRecord().process(ff));
    }

    // --- ConvertRecordToJSON ---

    @Test
    void convertRecordsToJsonArray() throws Exception {
        var records = List.<Map<String,Object>>of(
                Map.of("id", 1, "name", "a"),
                Map.of("id", 2, "name", "b"));
        var ff = FlowFile.create(new RecordContent(records), Map.of());
        var out = (ProcessorResult.Single) new ConvertRecordToJSON().process(ff);
        var raw = (RawContent) out.flowFile().content();
        // Round-trip through ConvertJSONToRecord to avoid map-ordering flakes.
        var roundTrip = new ConvertJSONToRecord().process(
                FlowFile.create(raw.bytes(), Map.of()));
        var decoded = (RecordContent) ((ProcessorResult.Single) roundTrip).flowFile().content();
        assertEquals(2, decoded.records().size());
    }

    @Test
    void convertRecordToJsonSingleObjectEmitsBareObject() {
        var records = List.<Map<String,Object>>of(Map.of("only", "one"));
        var ff = FlowFile.create(new RecordContent(records), Map.of());
        var out = (ProcessorResult.Single) new ConvertRecordToJSON(true).process(ff);
        var raw = (RawContent) out.flowFile().content();
        String json = new String(raw.bytes());
        assertTrue(json.startsWith("{"), "singleObject mode must emit a JSON object, got: " + json);
    }

    @Test
    void convertRecordToJsonNonRecordContentRoutesToFailure() {
        var ff = FlowFile.create(new byte[]{1, 2}, Map.of());
        assertInstanceOf(ProcessorResult.Failure.class, new ConvertRecordToJSON().process(ff));
    }

    // --- ExtractRecordField ---

    @Test
    void extractFieldHoistsValueToAttribute() {
        var records = List.<Map<String,Object>>of(Map.of("tenant", "acme", "other", 1));
        var ff = FlowFile.create(new RecordContent(records), Map.of());
        var out = (ProcessorResult.Single) new ExtractRecordField("tenant", "tenant.id").process(ff);
        assertEquals("acme", out.flowFile().attributes().get("tenant.id"));
    }

    @Test
    void extractFieldSupportsDottedPath() {
        var records = List.<Map<String,Object>>of(Map.of("meta", Map.of("user", Map.of("id", 7))));
        var ff = FlowFile.create(new RecordContent(records), Map.of());
        var out = (ProcessorResult.Single) new ExtractRecordField("meta.user.id", "user.id").process(ff);
        assertEquals("7", out.flowFile().attributes().get("user.id"));
    }

    @Test
    void extractFieldMissingPathRoutesToFailure() {
        var records = List.<Map<String,Object>>of(Map.of("x", "y"));
        var ff = FlowFile.create(new RecordContent(records), Map.of());
        assertInstanceOf(ProcessorResult.Failure.class,
                new ExtractRecordField("missing", "out").process(ff));
    }

    @Test
    void extractFieldEmptyRecordListRoutesToFailure() {
        var ff = FlowFile.create(new RecordContent(List.of()), Map.of());
        assertInstanceOf(ProcessorResult.Failure.class,
                new ExtractRecordField("any", "out").process(ff));
    }

    @Test
    void extractFieldValidatesConstructorArgs() {
        assertThrows(IllegalArgumentException.class, () -> new ExtractRecordField("", "x"));
        assertThrows(IllegalArgumentException.class, () -> new ExtractRecordField("x", ""));
    }
}
