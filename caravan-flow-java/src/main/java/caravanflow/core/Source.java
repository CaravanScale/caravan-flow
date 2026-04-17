package caravanflow.core;

import java.util.function.Predicate;

/// Pull/push source that feeds FlowFiles into a pipeline. Concrete
/// sources (see {@link caravanflow.core.PollingSource}) hand off to the
/// pipeline through the {@code ingest} callback supplied at start —
/// the callback returns {@code true} on acceptance so the source can
/// mark the underlying item consumed (e.g. {@code GetFile} moves the
/// file to {@code .processed/}).
///
/// Mirrors caravan-flow-csharp's IConnectorSource.
public interface Source {

    String name();

    String sourceType();

    boolean isRunning();

    /// Begin emitting. {@code ingest} is called once per FlowFile; it
    /// returns whether the pipeline accepted the submission. Sources
    /// must be idempotent w.r.t. {@code start} — calling twice with the
    /// source already running is a no-op.
    void start(Predicate<FlowFile> ingest);

    void stop();
}
