using ZincFlow.Core;

namespace ZincFlow.Fabric;

/// <summary>
/// ProcessorFactory: creates a configured IProcessor from scoped context and config.
/// </summary>
public delegate IProcessor ProcessorFactory(ScopedContext ctx, Dictionary<string, string> config);

/// <summary>
/// Metadata about a registered processor type.
/// </summary>
public sealed class ProcessorInfo
{
    public string Name { get; }
    public string Description { get; }
    public List<string> ConfigKeys { get; }

    public ProcessorInfo(string name, string description, List<string> configKeys)
    {
        Name = name;
        Description = description;
        ConfigKeys = configKeys;
    }
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

    public List<ProcessorInfo> List() => new(_info.Values);
}
