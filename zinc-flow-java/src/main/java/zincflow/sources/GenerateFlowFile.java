package zincflow.sources;

import zincflow.core.FlowFile;
import zincflow.core.PollingSource;

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
            attrs.put("generate.index", Long.toString(index.incrementAndGet()));
            out.add(FlowFile.create(content, attrs));
        }
        return out;
    }

    private static Map<String, String> buildAttributes(String name, String contentType, String spec) {
        Map<String, String> out = new LinkedHashMap<>();
        out.put("source", name);
        if (contentType != null && !contentType.isEmpty()) {
            out.put("http.content.type", contentType);
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
