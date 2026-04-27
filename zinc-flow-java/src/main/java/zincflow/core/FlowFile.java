package zincflow.core;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/// The unit of work flowing through the pipeline. Immutable record — the
/// {@code withAttribute} / {@code withContent} helpers produce a new
/// FlowFile rather than mutating. Attribute map is defensively copied on
/// construction.
public record FlowFile(
        long id,
        Map<String, String> attributes,
        Content content,
        long timestampMillis,
        int hopCount) {

    private static final AtomicLong ID_SEQ = new AtomicLong();

    public FlowFile {
        if (attributes == null) throw new IllegalArgumentException("attributes must not be null");
        if (content == null) throw new IllegalArgumentException("content must not be null");
        attributes = Map.copyOf(attributes);
    }

    /// String form used for logs and debugging only.
    public String stringId() {
        return "ff-" + id;
    }

    /// Create a new FlowFile with a fresh sequence id and current-time
    /// timestamp, from raw bytes + attribute map.
    public static FlowFile create(byte[] bytes, Map<String, String> attributes) {
        return new FlowFile(
                ID_SEQ.incrementAndGet(),
                attributes == null ? Map.of() : attributes,
                new RawContent(bytes),
                System.currentTimeMillis(),
                0);
    }

    /// Create a new FlowFile with explicit Content (not necessarily raw).
    public static FlowFile create(Content content, Map<String, String> attributes) {
        return new FlowFile(
                ID_SEQ.incrementAndGet(),
                attributes == null ? Map.of() : attributes,
                content,
                System.currentTimeMillis(),
                0);
    }

    /// Return a new FlowFile that carries the same id, content, and
    /// timestamp but with one attribute added/replaced.
    public FlowFile withAttribute(String key, String value) {
        Map<String, String> next = new HashMap<>(attributes);
        next.put(key, value);
        return new FlowFile(id, next, content, timestampMillis, hopCount);
    }

    /// Return a new FlowFile with the same metadata but different content.
    public FlowFile withContent(Content newContent) {
        return new FlowFile(id, attributes, newContent, timestampMillis, hopCount);
    }

    /// Return a new FlowFile with hopCount + 1 — called by the executor
    /// after each processor dispatch, to detect pipeline loops.
    public FlowFile bumpHop() {
        return new FlowFile(id, attributes, content, timestampMillis, hopCount + 1);
    }
}
