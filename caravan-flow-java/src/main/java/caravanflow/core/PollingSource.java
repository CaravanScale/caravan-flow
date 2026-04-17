package caravanflow.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/// Abstract base for sources that wake up on a fixed interval, scan
/// an external system, and hand resulting FlowFiles to the pipeline.
/// Subclasses implement {@link #poll()}; the base class owns the
/// lifecycle, the virtual-thread loop, and the ingest-accept/reject
/// dispatch.
///
/// <h2>Threading</h2>
/// The poll loop runs on a single virtual thread. Blocking I/O inside
/// {@link #poll()} (disk scan, HTTP GET, database query) is fine — it
/// doesn't tie up a platform thread. Stop is cooperative: {@link #stop()}
/// interrupts the thread and {@link #poll()} should return promptly
/// when {@code Thread.interrupted()} is observed.
///
/// <h2>Backpressure</h2>
/// The {@code ingest} callback passed to {@link #start(Predicate)} returns
/// {@code true} if the pipeline accepted the FlowFile. Subclasses react
/// via {@link #onIngested(FlowFile)} (e.g. move the file to
/// {@code .processed/}) or {@link #onRejected(FlowFile)}.
///
/// Mirrors caravan-flow-csharp's PollingSource.
public abstract class PollingSource implements Source {

    private static final Logger log = LoggerFactory.getLogger(PollingSource.class);

    private final String name;
    private final long pollIntervalMillis;
    private volatile boolean running;
    private volatile Thread loop;

    protected PollingSource(String name, long pollIntervalMillis) {
        if (name == null || name.isEmpty())
            throw new IllegalArgumentException("source name must not be blank");
        this.name = name;
        // Guard against zero/negative — a tight loop would pin a CPU
        // and surprise any operator who typo'd a config value.
        this.pollIntervalMillis = pollIntervalMillis > 0 ? pollIntervalMillis : 1000;
    }

    @Override public final String name() { return name; }
    @Override public final boolean isRunning() { return running; }
    public final long pollIntervalMillis() { return pollIntervalMillis; }

    /// Scan the external system and return zero or more FlowFiles to
    /// hand to the pipeline. Implementations should return promptly on
    /// interruption so {@link #stop()} isn't blocked by a long scan.
    protected abstract List<FlowFile> poll();

    /// Called after the pipeline accepted a FlowFile. Default is a no-op;
    /// {@code GetFile} overrides this to move the source file to
    /// {@code .processed/}.
    protected void onIngested(FlowFile ff) { /* default: nothing */ }

    /// Called when the pipeline refused a FlowFile (ingest returned
    /// {@code false}). Default logs at debug — subclasses can retry,
    /// quarantine, or drop.
    protected void onRejected(FlowFile ff) {
        log.debug("source {}: pipeline rejected {}", name, ff.stringId());
    }

    @Override
    public final synchronized void start(Predicate<FlowFile> ingest) {
        if (ingest == null) throw new IllegalArgumentException("ingest callback must not be null");
        if (running) return;
        running = true;
        loop = Thread.ofVirtual()
                .name("caravan-flow-source-" + name)
                .start(() -> runLoop(ingest));
        log.info("source {} started ({} type, poll={}ms)", name, sourceType(), pollIntervalMillis);
    }

    @Override
    public final synchronized void stop() {
        if (!running) return;
        running = false;
        Thread t = loop;
        if (t != null) t.interrupt();
        loop = null;
        log.info("source {} stopped", name);
    }

    private void runLoop(Predicate<FlowFile> ingest) {
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                List<FlowFile> batch = poll();
                if (batch != null) {
                    for (FlowFile ff : batch) {
                        if (!running) return;
                        boolean accepted;
                        try { accepted = ingest.test(ff); }
                        catch (RuntimeException ex) {
                            log.warn("source {}: ingest threw for {} — {}", name, ff.stringId(), ex.toString());
                            accepted = false;
                        }
                        if (accepted) onIngested(ff);
                        else onRejected(ff);
                    }
                }
            } catch (RuntimeException ex) {
                log.warn("source {}: poll failed — {}", name, ex.toString());
            }
            try { TimeUnit.MILLISECONDS.sleep(pollIntervalMillis); }
            catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
