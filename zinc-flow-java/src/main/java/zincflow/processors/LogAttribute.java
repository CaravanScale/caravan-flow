package zincflow.processors;

import zincflow.core.FlowFile;
import zincflow.core.Processor;
import zincflow.core.ProcessorResult;
import zincflow.providers.LoggingProvider;

import java.util.Map;
import java.util.Objects;

/// Logs a FlowFile (id, size, attributes) under the configured prefix,
/// then passes it through on the "success" connection unchanged. Always
/// goes through a {@link LoggingProvider}; when the pipeline wires a
/// shared provider the operator can flip it off at runtime to mute
/// chatty LogAttribute instances.
public final class LogAttribute implements Processor {

    private final String prefix;
    private final LoggingProvider logger;

    public LogAttribute(String prefix) {
        this(prefix, null);
    }

    public LogAttribute(String prefix, LoggingProvider logger) {
        this.prefix = prefix == null ? "" : prefix;
        this.logger = Objects.requireNonNullElseGet(logger, LoggingProvider::enabled);
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        logger.info(
                "LogAttribute",
                prefix,
                Map.of(
                        "ff", ff.stringId(),
                        "size", ff.content().size(),
                        "attrs", ff.attributes()));
        return ProcessorResult.single(ff);
    }
}
