package zincflow.sources;

import zincflow.core.FlowFile;
import zincflow.core.FlowFileAttributes;
import zincflow.core.PollingSource;
import zincflow.core.Source;
import zincflow.core.SourcePlugin;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/// Timer-driven generator. Emits {@code batchSize} identical FlowFiles
/// every {@code pollIntervalMillis}. Handy for load testing, soak
/// tests, heartbeats, and smoke-testing a deployed flow.
///
/// Every emitted FlowFile carries:
/// <ul>
///   <li>{@code source} — this source's name</li>
///   <li>{@code generate.index} — monotonically increasing counter</li>
///   <li>{@code http.content.type} — when {@code contentType} is non-empty</li>
///   <li>plus any custom pairs parsed from the {@code attributes} string</li>
/// </ul>
///
/// Config (under {@code sources.generate}):
/// <pre>
/// sources:
///   generate:
///     content: "ping"
///     content_type: application/json
///     attributes: "env:dev;tenant:acme"
///     batch_size: 1
///     poll_interval_ms: 1000
/// </pre>
///
/// Mirrors zinc-flow-csharp's GenerateFlowFile.
public final class GenerateFlowFile extends PollingSource {

    public static final String NAME = "generate";
    public static final String TYPE = "GenerateFlowFile";

    private final byte[] content;
    private final Map<String, String> baseAttributes;
    private final int batchSize;
    private final AtomicLong index = new AtomicLong();

    public GenerateFlowFile(String name, long pollIntervalMillis,
                            String content, String contentType,
                            String attributes, int batchSize) {
        super(name, pollIntervalMillis);
        this.content = (content == null ? "" : content).getBytes(StandardCharsets.UTF_8);
        this.batchSize = batchSize <= 0 ? 1 : batchSize;
        this.baseAttributes = buildAttributes(name, contentType, attributes);
    }

    @Override public String sourceType() { return TYPE; }

    @Override
    protected List<FlowFile> poll() {
        List<FlowFile> out = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            Map<String, String> attrs = new LinkedHashMap<>(baseAttributes);
            attrs.put(FlowFileAttributes.GENERATE_INDEX, Long.toString(index.incrementAndGet()));
            out.add(FlowFile.create(content, attrs));
        }
        return out;
    }

    /// SPI entry for ServiceLoader discovery. Both the built-in
    /// bootstrap and any plugin jar pick up the source through this
    /// class, so there's one discovery path across all source sources.
    public static final class Plugin implements SourcePlugin {
        @Override public String sourceType() { return TYPE; }
        @Override public String description() { return "Timer-driven FlowFile generator for heartbeats and load tests."; }
        @Override public List<String> configKeys() {
            return List.of("content", "content_type", "attributes", "batch_size", "poll_interval_ms");
        }
        @Override public Source create(String name, Map<String, Object> config) {
            String content = str(config.get("content"));
            if (content.isEmpty()) return null; // disabled when content is absent
            return new GenerateFlowFile(name,
                    longOr(config.get("poll_interval_ms"), 1000),
                    content,
                    str(config.get("content_type")),
                    str(config.get("attributes")),
                    (int) longOr(config.get("batch_size"), 1));
        }

        private static String str(Object o) { return o == null ? "" : String.valueOf(o); }
        private static long longOr(Object o, long fallback) {
            if (o == null) return fallback;
            if (o instanceof Number n) return n.longValue();
            try { return Long.parseLong(o.toString().trim()); }
            catch (NumberFormatException ex) { return fallback; }
        }
    }

    private static Map<String, String> buildAttributes(String name, String contentType, String spec) {
        Map<String, String> out = new LinkedHashMap<>();
        out.put(FlowFileAttributes.SOURCE, name);
        if (contentType != null && !contentType.isEmpty()) {
            out.put(FlowFileAttributes.HTTP_CONTENT_TYPE, contentType);
        }
        if (spec == null || spec.isBlank()) return out;
        // "key:value;key:value" — permissive: ignore entries without a
        // colon instead of throwing, so a minor config typo doesn't
        // crash boot. An empty value is a legal attribute.
        for (String pair : spec.split(";")) {
            String trimmed = pair.trim();
            if (trimmed.isEmpty()) continue;
            int idx = trimmed.indexOf(':');
            if (idx <= 0) continue;
            out.put(trimmed.substring(0, idx).trim(), trimmed.substring(idx + 1).trim());
        }
        return out;
    }
}
