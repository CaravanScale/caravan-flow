package caravanflow.core;

/// Lifecycle state shared by processors, providers, and sources.
/// Matches the caravan-flow-csharp enum 1:1.
public enum ComponentState {
    DISABLED,
    ENABLED,
    DRAINING
}
