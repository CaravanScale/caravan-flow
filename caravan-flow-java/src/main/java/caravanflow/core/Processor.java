package caravanflow.core;

/// A processor is a pure function over FlowFiles. Implementations can
/// hold config/state (fields) but the {@link #process(FlowFile)} method
/// must be thread-safe — the executor may invoke it concurrently for
/// different flowfiles under the same instance.
public interface Processor {

    /// Stable identifier used in config.yaml and `/api/flow` responses.
    /// Defaults to the simple class name; override for custom naming.
    default String name() {
        return getClass().getSimpleName();
    }

    ProcessorResult process(FlowFile ff);
}
