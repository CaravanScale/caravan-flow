package zincflow.processors;

import zincflow.core.FlowFile;
import zincflow.core.Processor;
import zincflow.core.ProcessorResult;

/// Sets (or replaces) a single attribute on the FlowFile and forwards
/// on "success". Multi-attribute updates are a Phase 3 follow-up.
public final class UpdateAttribute implements Processor {

    private final String key;
    private final String value;

    public UpdateAttribute(String key, String value) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("UpdateAttribute: key must not be blank");
        }
        this.key = key;
        this.value = value == null ? "" : value;
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        return ProcessorResult.single(ff.withAttribute(key, value));
    }
}
