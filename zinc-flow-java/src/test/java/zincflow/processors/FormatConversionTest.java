package zincflow.processors;

import org.junit.jupiter.api.Test;
import zincflow.core.FlowFile;
import zincflow.core.ProcessorResult;
import zincflow.core.RawContent;
import zincflow.core.RecordContent;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

final class FormatConversionTest {

    // --- CSV ---

    @Test
    void csvHeaderRowParse() {
        var ff = FlowFile.create("name,age\nAlice,30\nBob,25".getBytes(StandardCharsets.UTF_8), Map.of());
        var out = (ProcessorResult.Single) new ConvertCSVToRecord().process(ff);
        var rc = (RecordContent) out.flowFile().content();
        assertEquals(2, rc.records().size());
        assertEquals("Alice", rc.records().get(0).get("name"));
        assertEquals("25", rc.records().get(1).get("age"));
        assertEquals("2", out.flowFile().attributes().get("record.count"));
    }

    @Test
    void csvTabDelimitedParse() {
        var body = "name\tage\nAlice\t30".getBytes(StandardCharsets.UTF_8);
        var ff = FlowFile.create(body, Map.of());
        var out = (ProcessorResult.Single) new ConvertCSVToRecord('\t', true, List.of()).process(ff);
        var rc = (RecordContent) out.flowFile().content();
        assertEquals("Alice", rc.records().getFirst().get("name"));
    }

    @Test
    void csvQuotedValuesHandled() {
        var body = "name,note\n\"Smith, Jr.\",\"has, commas\"".getBytes(StandardCharsets.UTF_8);
        var ff = FlowFile.create(body, Map.of());
        var out = (ProcessorResult.Single) new ConvertCSVToRecord().process(ff);
        var rc = (RecordContent) out.flowFile().content();
        assertEquals("Smith, Jr.", rc.records().getFirst().get("name"));
        assertEquals("has, commas", rc.records().getFirst().get("note"));
    }

    @Test
    void csvRoundTripPreservesRecords() {
        // JSON → Record → CSV → Record — values should line up.
        var jsonFf = FlowFile.create("[{\"id\":\"1\",\"name\":\"a\"},{\"id\":\"2\",\"name\":\"b\"}]".getBytes(), Map.of());
        var afterJson = (ProcessorResult.Single) new ConvertJSONToRecord().process(jsonFf);
        var afterCsv  = (ProcessorResult.Single) new ConvertRecordToCSV().process(afterJson.flowFile());
        var afterRead = (ProcessorResult.Single) new ConvertCSVToRecord().process(afterCsv.flowFile());
        var rc = (RecordContent) afterRead.flowFile().content();
        assertEquals(2, rc.records().size());
        assertEquals("1", rc.records().get(0).get("id"));
        assertEquals("b", rc.records().get(1).get("name"));
    }

    @Test
    void csvOnNonRawContentFailsCleanly() {
        var ff = FlowFile.create(new RecordContent(List.of()), Map.of());
        assertInstanceOf(ProcessorResult.Failure.class, new ConvertCSVToRecord().process(ff));
    }

    // --- Avro binary (no container) ---

    private static final String SIMPLE_SCHEMA = """
            {
              "type": "record",
              "name": "User",
              "fields": [
                {"name": "id",   "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "active", "type": "boolean"}
              ]
            }
            """;

    @Test
    void avroRoundTripFlatRecord() {
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("id", 7L);
        record.put("name", "alice");
        record.put("active", true);
        var ff = FlowFile.create(new RecordContent(List.of(record)), Map.of());

        var encoded = (ProcessorResult.Single) new ConvertRecordToAvro(SIMPLE_SCHEMA).process(ff);
        var decoded = (ProcessorResult.Single) new ConvertAvroToRecord(SIMPLE_SCHEMA).process(encoded.flowFile());
        var rc = (RecordContent) decoded.flowFile().content();

        assertEquals(1, rc.records().size());
        assertEquals(7L, rc.records().getFirst().get("id"));
        assertEquals("alice", rc.records().getFirst().get("name"));
        assertEquals(Boolean.TRUE, rc.records().getFirst().get("active"));
    }

    @Test
    void avroRoundTripMultipleRecords() {
        List<Map<String, Object>> records = List.of(
                flat(1L, "a", true),
                flat(2L, "b", false),
                flat(3L, "c", true));
        var ff = FlowFile.create(new RecordContent(records), Map.of());
        var encoded = (ProcessorResult.Single) new ConvertRecordToAvro(SIMPLE_SCHEMA).process(ff);
        var decoded = (ProcessorResult.Single) new ConvertAvroToRecord(SIMPLE_SCHEMA).process(encoded.flowFile());
        var rc = (RecordContent) decoded.flowFile().content();
        assertEquals(3, rc.records().size());
        assertEquals("b", rc.records().get(1).get("name"));
    }

    private static Map<String, Object> flat(long id, String name, boolean active) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("id", id);
        r.put("name", name);
        r.put("active", active);
        return r;
    }

    @Test
    void avroRoundTripNestedRecordAndArray() {
        String nested = """
                {
                  "type": "record",
                  "name": "Order",
                  "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "items", "type": {"type": "array", "items": "string"}},
                    {"name": "customer", "type": {
                      "type": "record", "name": "Customer",
                      "fields": [{"name": "id", "type": "long"}, {"name": "tenant", "type": "string"}]
                    }}
                  ]
                }
                """;
        Map<String, Object> customer = new LinkedHashMap<>();
        customer.put("id", 42L);
        customer.put("tenant", "acme");
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("id", "ord-1");
        record.put("items", List.of("apple", "banana"));
        record.put("customer", customer);
        var ff = FlowFile.create(new RecordContent(List.of(record)), Map.of());

        var encoded = (ProcessorResult.Single) new ConvertRecordToAvro(nested).process(ff);
        var decoded = (ProcessorResult.Single) new ConvertAvroToRecord(nested).process(encoded.flowFile());
        var rc = (RecordContent) decoded.flowFile().content();

        var top = rc.records().getFirst();
        assertEquals("ord-1", top.get("id"));
        @SuppressWarnings("unchecked")
        List<Object> items = (List<Object>) top.get("items");
        assertEquals(List.of("apple", "banana"), items);
        @SuppressWarnings("unchecked")
        Map<String, Object> cust = (Map<String, Object>) top.get("customer");
        assertEquals(42L, cust.get("id"));
        assertEquals("acme", cust.get("tenant"));
    }

    @Test
    void avroMissingSchemaConfigRejected() {
        assertThrows(IllegalArgumentException.class, () -> new ConvertAvroToRecord(""));
        assertThrows(IllegalArgumentException.class, () -> new ConvertRecordToAvro(null));
        assertThrows(IllegalArgumentException.class, () -> new ConvertRecordToOCF(""));
    }

    @Test
    void avroMalformedBinaryFails() {
        var ff = FlowFile.create(new byte[]{1, 2, 3, 4, 5}, Map.of());
        var out = new ConvertAvroToRecord(SIMPLE_SCHEMA).process(ff);
        assertInstanceOf(ProcessorResult.Failure.class, out);
    }

    @Test
    void avroOnNonRecordContentFailsCleanly() {
        var ff = FlowFile.create(new byte[]{1, 2}, Map.of());
        assertInstanceOf(ProcessorResult.Failure.class,
                new ConvertRecordToAvro(SIMPLE_SCHEMA).process(ff));
    }

    // --- OCF ---

    @Test
    void ocfRoundTripEmbedsSchema() {
        Map<String, Object> record = flat(1L, "alice", true);
        var ff = FlowFile.create(new RecordContent(List.of(record)), Map.of());

        var written = (ProcessorResult.Single) new ConvertRecordToOCF(SIMPLE_SCHEMA).process(ff);
        var read    = (ProcessorResult.Single) new ConvertOCFToRecord().process(written.flowFile());
        var rc = (RecordContent) read.flowFile().content();

        assertEquals(1, rc.records().size());
        assertEquals("alice", rc.records().getFirst().get("name"));
        // Schema surfaces as an attribute for downstream use.
        assertTrue(read.flowFile().attributes().get("avro.schema").contains("\"name\":\"User\""));
    }

    @Test
    void ocfDeflateCodecAccepted() {
        Map<String, Object> record = flat(1L, "alice", true);
        var ff = FlowFile.create(new RecordContent(List.of(record)), Map.of());
        // deflate is a built-in Avro codec — should not throw.
        assertDoesNotThrow(() -> new ConvertRecordToOCF(SIMPLE_SCHEMA, "deflate").process(ff));
    }

    @Test
    void ocfReadOnNonRawContentFailsCleanly() {
        var ff = FlowFile.create(new RecordContent(List.of()), Map.of());
        assertInstanceOf(ProcessorResult.Failure.class, new ConvertOCFToRecord().process(ff));
    }

    @Test
    void ocfWriteEmptyRecordList() {
        var ff = FlowFile.create(new RecordContent(List.of()), Map.of());
        var written = (ProcessorResult.Single) new ConvertRecordToOCF(SIMPLE_SCHEMA).process(ff);
        // OCF with zero records should still produce a valid file (header only),
        // which ConvertOCFToRecord reads back as an empty record list.
        var read = (ProcessorResult.Single) new ConvertOCFToRecord().process(written.flowFile());
        assertEquals(0, ((RecordContent) read.flowFile().content()).records().size());
    }

    @Test
    void ocfGarbageInputFailsCleanly() {
        var ff = FlowFile.create(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}, Map.of());
        assertInstanceOf(ProcessorResult.Failure.class, new ConvertOCFToRecord().process(ff));
    }
}
