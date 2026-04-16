package zincflow.processors;

import zincflow.core.FlowFile;
import zincflow.core.Processor;
import zincflow.core.ProcessorResult;

/// Drops a FlowFile when a named attribute matches an expected value;
/// otherwise passes it through on "success". Intended for "kill this
/// record before it reaches the sink" filtering.
public final class FilterAttribute implements Processor {

    private final String key;
    private final String value;
    private final boolean dropOnMatch;

    public FilterAttribute(String key, String value) {
        this(key, value, true);
    }

    public FilterAttribute(String key, String value, boolean dropOnMatch) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("FilterAttribute: key must not be blank");
        }
        this.key = key;
        this.value = value == null ? "" : value;
        this.dropOnMatch = dropOnMatch;
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        String attr = ff.attributes().get(key);
        boolean matches = value.equals(attr);
        return (matches == dropOnMatch) ? ProcessorResult.dropped() : ProcessorResult.single(ff);
    }
}
