package zincflow.core;

import java.util.List;

/// Result of a single {@link Processor#process(FlowFile)} call. Sealed
/// so the executor can pattern-match on the shape:
/// <ul>
///   <li>{@link Single} — one FlowFile flows to the processor's "success" connections
///   <li>{@link Multiple} — fan-out; every FlowFile in the list follows "success"
///   <li>{@link Routed} — named relationship (e.g. "matched", "not-matched")
///   <li>{@link Dropped} — terminate this branch; no downstream dispatch
///   <li>{@link Failure} — follow "failure" connections (or log+drop if none)
/// </ul>
public sealed interface ProcessorResult
        permits ProcessorResult.Single,
                ProcessorResult.Multiple,
                ProcessorResult.Routed,
                ProcessorResult.Dropped,
                ProcessorResult.Failure {

    record Single(FlowFile flowFile) implements ProcessorResult {
        public Single {
            if (flowFile == null) throw new IllegalArgumentException("flowFile must not be null");
        }
    }

    record Multiple(List<FlowFile> flowFiles) implements ProcessorResult {
        public Multiple {
            if (flowFiles == null) throw new IllegalArgumentException("flowFiles must not be null");
            flowFiles = List.copyOf(flowFiles);
        }
    }

    record Routed(String route, FlowFile flowFile) implements ProcessorResult {
        public Routed {
            if (route == null || route.isEmpty()) throw new IllegalArgumentException("route must not be blank");
            if (flowFile == null) throw new IllegalArgumentException("flowFile must not be null");
        }
    }

    /// Singleton — no state, reuse the shared instance.
    enum Dropped implements ProcessorResult {
        INSTANCE
    }

    record Failure(String reason, FlowFile flowFile) implements ProcessorResult {
        public Failure {
            if (reason == null) reason = "";
            if (flowFile == null) throw new IllegalArgumentException("flowFile must not be null");
        }
    }

    // Static factories for ergonomics — match the C# ctor sites' feel.

    static ProcessorResult single(FlowFile ff) { return new Single(ff); }
    static ProcessorResult multiple(List<FlowFile> ffs) { return new Multiple(ffs); }
    static ProcessorResult routed(String route, FlowFile ff) { return new Routed(route, ff); }
    static ProcessorResult dropped() { return Dropped.INSTANCE; }
    static ProcessorResult failure(String reason, FlowFile ff) { return new Failure(reason, ff); }
}
