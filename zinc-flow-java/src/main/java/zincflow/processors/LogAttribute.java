package zincflow.processors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zincflow.core.FlowFile;
import zincflow.core.Processor;
import zincflow.core.ProcessorResult;

/// Logs a FlowFile (id, size, attributes) under the configured prefix,
/// then passes it through on the "success" connection unchanged.
public final class LogAttribute implements Processor {

    private static final Logger log = LoggerFactory.getLogger(LogAttribute.class);

    private final String prefix;

    public LogAttribute(String prefix) {
        this.prefix = prefix == null ? "" : prefix;
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        log.info("{} ff={} size={} attrs={}",
                prefix, ff.stringId(), ff.content().size(), ff.attributes());
        return ProcessorResult.single(ff);
    }
}
