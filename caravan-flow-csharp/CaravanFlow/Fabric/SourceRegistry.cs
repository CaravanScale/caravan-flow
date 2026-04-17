using CaravanFlow.Core;

namespace CaravanFlow.Fabric;

/// <summary>
/// Creates a configured IConnectorSource from a name (the map key in
/// config.yaml's <c>sources:</c> block), its flattened config dict, and
/// a content store for offload-heavy payloads. Return null to signal
/// "this instance is not configured" (e.g. GetFile with no inputDir)
/// so the loader can skip it without throwing.
/// </summary>
public delegate IConnectorSource? SourceFactory(string name, Dictionary<string, string> config, IContentStore store);

/// <summary>
/// Metadata about a registered source type — type name, human description,
/// and the config keys the factory reads. Parallels ProcessorInfo.
/// </summary>
public sealed class SourceInfo
{
    public string TypeName { get; }
    public string Description { get; }
    public List<string> ConfigKeys { get; }

    public SourceInfo(string typeName, string description, List<string> configKeys)
    {
        TypeName = typeName;
        Description = description;
        ConfigKeys = configKeys;
    }
}

/// <summary>
/// Registry mapping source-type names to factory functions. Mirrors
/// <see cref="Registry"/> (processors) so Program.cs can iterate
/// config.yaml's <c>sources:</c> block generically instead of
/// hardcoding one block per type. Matches caravan-flow-java's
/// SourceRegistry shape — generic map, multiple instances per type,
/// one declarative section per source.
/// </summary>
public sealed class SourceRegistry
{
    private readonly Dictionary<string, SourceFactory> _factories = new();
    private readonly Dictionary<string, SourceInfo> _info = new();

    public void Register(SourceInfo info, SourceFactory factory)
    {
        _factories[info.TypeName] = factory;
        _info[info.TypeName] = info;
    }

    public IConnectorSource? Create(string type, string name, Dictionary<string, string> config, IContentStore store)
    {
        if (!_factories.TryGetValue(type, out var factory)) return null;
        return factory(name, config, store);
    }

    public bool Has(string type) => _factories.ContainsKey(type);

    public SourceInfo? GetInfo(string type)
        => _info.TryGetValue(type, out var info) ? info : null;

    public List<SourceInfo> List() => new(_info.Values);
}
