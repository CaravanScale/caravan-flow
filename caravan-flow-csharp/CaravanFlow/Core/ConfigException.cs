namespace CaravanFlow.Core;

/// <summary>
/// Thrown by processor factories, source factories, and config parsers
/// when a YAML config can't produce a usable component — missing
/// required keys, unparseable values, unknown processor types, dangling
/// connection targets. <c>Fabric.LoadFlow</c> catches these per-
/// processor, aggregates, and throws <see cref="AggregateException"/>
/// at the end of the load pass so the operator sees every config
/// problem at once rather than fixing them one-at-a-time.
///
/// Distinct from <see cref="InvalidOperationException"/> (which marks
/// runtime invariants — schema mismatches, truncated inputs) and from
/// <c>FailureResult</c> (runtime per-FlowFile failure). Rule of thumb:
///
///   • Can the operator fix this by editing config? → ConfigException.
///   • Is this a malformed input the pipeline is asked to process? →
///     FailureResult.
///   • Is this a "should never happen" invariant? → InvalidOperationException
///     (or ArgumentException where it already fits).
/// </summary>
public sealed class ConfigException : Exception
{
    /// <summary>
    /// User-facing component name this error is attached to — e.g.
    /// the processor name from the YAML. Lets the aggregator group
    /// errors by component for a cleaner report.
    /// </summary>
    public string? ComponentName { get; }

    public ConfigException(string message) : base(message) { }

    public ConfigException(string componentName, string message)
        : base($"[{componentName}] {message}")
    {
        ComponentName = componentName;
    }

    public ConfigException(string message, Exception inner) : base(message, inner) { }
}
