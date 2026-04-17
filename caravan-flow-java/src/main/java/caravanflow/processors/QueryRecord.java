package caravanflow.processors;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;
import caravanflow.core.FlowFile;
import caravanflow.core.Processor;
import caravanflow.core.ProcessorResult;
import caravanflow.core.RecordContent;
import caravanflow.core.Relationships;

import java.util.List;
import java.util.Map;

/// Filters RecordContent by a JsonPath predicate (Jayway JsonPath). The
/// query is applied to the full record list, and the matching subset
/// becomes a new RecordContent. Routing:
///
/// <ul>
///   <li>non-empty matches  → {@code matched} relationship
///   <li>empty matches      → {@code unmatched} (FlowFile passes through untouched)
/// </ul>
///
/// Example query (records where {@code priority == "high"}):
/// {@code $[?(@.priority == 'high')]}
public final class QueryRecord implements Processor {

    // Default-safe JsonPath config: suppress exceptions on missing paths
    // so a mismatched record just doesn't match rather than blowing up.
    private static final Configuration CONFIG = Configuration.defaultConfiguration()
            .addOptions(Option.SUPPRESS_EXCEPTIONS);

    private final com.jayway.jsonpath.JsonPath compiled;

    public QueryRecord(String jsonPathQuery) {
        if (jsonPathQuery == null || jsonPathQuery.isEmpty()) {
            throw new IllegalArgumentException("QueryRecord: jsonPath query must not be blank");
        }
        try {
            this.compiled = JsonPath.compile(jsonPathQuery);
        } catch (RuntimeException ex) {
            throw new IllegalArgumentException(
                    "QueryRecord: invalid JsonPath — " + ex.getMessage(), ex);
        }
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        if (!(ff.content() instanceof RecordContent rc)) {
            return ProcessorResult.failure(
                    "QueryRecord: expected RecordContent, got " + ff.content().getClass().getSimpleName(), ff);
        }
        try {
            Object result = compiled.read(rc.records(), CONFIG);
            List<Map<String, Object>> matches = normalise(result);
            if (matches.isEmpty()) {
                return ProcessorResult.routed(Relationships.UNMATCHED, ff);
            }
            return ProcessorResult.routed(Relationships.MATCHED,
                    ff.withContent(new RecordContent(matches, rc.schema())));
        } catch (PathNotFoundException ex) {
            // With SUPPRESS_EXCEPTIONS this usually becomes an empty result,
            // but some malformed paths still throw — treat as no match.
            return ProcessorResult.routed(Relationships.UNMATCHED, ff);
        } catch (RuntimeException ex) {
            return ProcessorResult.failure("QueryRecord: query failed — " + ex.getMessage(), ff);
        }
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> normalise(Object jsonPathResult) {
        if (jsonPathResult == null) return List.of();
        if (jsonPathResult instanceof List<?> list) {
            List<Map<String, Object>> out = new java.util.ArrayList<>(list.size());
            for (Object item : list) {
                if (item instanceof Map<?, ?> m) out.add((Map<String, Object>) m);
            }
            return out;
        }
        if (jsonPathResult instanceof Map<?, ?> m) {
            return List.of((Map<String, Object>) m);
        }
        return List.of();
    }
}
