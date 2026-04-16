package zincflow.core;

/// Pull/push source that feeds FlowFiles into a pipeline. Sources are a
/// forward-compatible surface — zinc-flow-java doesn't ship built-in
/// sources yet, so the management API reports an empty list out of the
/// box. External code can register concrete sources through
/// {@link zincflow.fabric.Pipeline#addSource(Source)}.
///
/// Mirrors zinc-flow-csharp's IConnectorSource.
public interface Source {

    String name();

    String sourceType();

    boolean isRunning();

    void start();

    void stop();
}
