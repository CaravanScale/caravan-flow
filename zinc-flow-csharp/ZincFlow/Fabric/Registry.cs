using ZincFlow.Core;

namespace ZincFlow.Fabric;

/// <summary>
/// ProcessorFactory: creates a configured IProcessor from scoped context and config.
/// </summary>
public delegate IProcessor ProcessorFactory(ScopedContext ctx, Dictionary<string, string> config);

/// <summary>
/// Kind of a processor parameter — drives UI form rendering and round-trip
/// string encoding. Values are serialized to JSON as strings (see ApiHandler).
/// </summary>
public enum ParamKind
{
    String,         // single-line text
    Multiline,      // textarea
    Integer,
    Number,         // double
    Boolean,        // "true" / "false"
    Enum,           // choice list (Choices populated)
    Expression,     // EL expression — UI can syntax-highlight
    KeyValueList,   // repeater of (key, value) rows; see ValueKind + delims
    StringList,     // repeater of single-string rows (no key)
    Secret,         // env-var reference like ${MY_SECRET}
}

/// <summary>
/// Typed description of a single processor config parameter. Drives the UI
/// form for visual programming. <see cref="Default"/> distinguishes
/// null ("no default — leave blank") from "" ("default is the empty string").
/// </summary>
public sealed class ParamInfo
{
    public string Name { get; init; } = "";
    public string Label { get; init; } = "";
    public string Description { get; init; } = "";
    public ParamKind Kind { get; init; } = ParamKind.String;
    public bool Required { get; init; }
    public string? Default { get; init; }
    public string? Placeholder { get; init; }
    public IReadOnlyList<string>? Choices { get; init; }
    public ParamKind? ValueKind { get; init; }
    public string EntryDelim { get; init; } = ";";
    public string PairDelim { get; init; } = "=";
}

/// <summary>
/// Metadata about a registered processor type.
/// </summary>
public sealed class ProcessorInfo
{
    public string Name { get; }
    public string Description { get; }
    public string Category { get; }
    public IReadOnlyList<ParamInfo> Parameters { get; }
    public IReadOnlyList<string> ConfigKeys { get; }
    /// Opt-in wizard id — when set, the UI renders the named wizard
    /// component instead of the generic per-kind form. Null means use
    /// the generic form (default).
    public string? WizardComponent { get; init; }

    public ProcessorInfo(string name, string description, string category, IReadOnlyList<ParamInfo> parameters)
    {
        Name = name;
        Description = description;
        Category = category;
        Parameters = parameters;
        ConfigKeys = parameters.Select(p => p.Name).ToList();
    }

    // Legacy constructor — kept permanently so third-party processors using the
    // old (name, description, List<string>) shape still compile. Wraps each key
    // as a String-kind param with Category="Other".
    public ProcessorInfo(string name, string description, List<string> configKeys)
        : this(name, description, "Other",
               configKeys.Select(k => new ParamInfo { Name = k, Label = k, Kind = ParamKind.String }).ToList())
    { }
}

/// <summary>
/// Registry: maps processor type names to factory functions.
/// </summary>
public sealed class Registry
{
    private readonly Dictionary<string, ProcessorFactory> _factories = new();
    private readonly Dictionary<string, ProcessorInfo> _info = new();

    public void Register(ProcessorInfo info, ProcessorFactory factory)
    {
        _factories[info.Name] = factory;
        _info[info.Name] = info;
    }

    public IProcessor Create(string name, ScopedContext ctx, Dictionary<string, string> config)
    {
        return _factories[name](ctx, config);
    }

    public bool Has(string name) => _factories.ContainsKey(name);

    public ProcessorInfo? GetInfo(string name)
        => _info.TryGetValue(name, out var info) ? info : null;

    public List<ProcessorInfo> List() => new(_info.Values);
}
