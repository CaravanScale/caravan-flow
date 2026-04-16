package zincflow.core;

/// Lifecycle state shared by processors, providers, and sources.
/// Matches the zinc-flow-csharp enum 1:1.
public enum ComponentState {
    DISABLED,
    ENABLED,
    DRAINING
}
