namespace ZincFlow.Core;

// --- Provider interface ---

public interface IProvider
{
    string Name { get; }
    string ProviderType { get; }
    ComponentState State { get; }
    void Enable();
    void Disable(int drainTimeout);
    void Shutdown();
    bool IsEnabled => State == ComponentState.Enabled;
}

// --- ProcessorContext ---

public sealed class ProcessorContext
{
    private readonly Dictionary<string, IProvider> _providers = new();
    private readonly Dictionary<string, List<string>> _dependents = new();

    public void AddProvider(IProvider provider) => _providers[provider.Name] = provider;
    public IProvider? GetProvider(string name) => _providers.GetValueOrDefault(name);
    public List<string> ListProviders() => new(_providers.Keys);

    public void RegisterDependent(string providerName, string processorName)
    {
        if (!_dependents.ContainsKey(providerName))
            _dependents[providerName] = new List<string>();
        _dependents[providerName].Add(processorName);
    }

    public List<string> GetDependents(string providerName)
        => _dependents.GetValueOrDefault(providerName) ?? new List<string>();

    public void ShutdownAll()
    {
        foreach (var p in _providers.Values)
        {
            if (p.IsEnabled) p.Disable(60);
            p.Shutdown();
        }
    }
}

// --- ScopedContext ---

public sealed class ScopedContext
{
    private readonly Dictionary<string, IProvider> _providers;

    public ScopedContext(Dictionary<string, IProvider> providers) => _providers = providers;

    public IProvider GetProvider(string name)
    {
        if (!_providers.TryGetValue(name, out var p))
            throw new KeyNotFoundException($"provider not in scope: {name}");
        if (!p.IsEnabled)
            throw new InvalidOperationException($"provider not enabled: {name}");
        return p;
    }

    public List<string> ListProviders() => new(_providers.Keys);
}

// --- Built-in providers ---

public sealed class ConfigProvider : IProvider
{
    public string Name => "config";
    public string ProviderType => "config";
    public ComponentState State { get; private set; } = ComponentState.Disabled;
    private readonly Dictionary<string, object> _config;

    public ConfigProvider(Dictionary<string, object> config) => _config = config;

    public void Enable() => State = ComponentState.Enabled;
    public void Disable(int drainTimeout) => State = ComponentState.Disabled;
    public void Shutdown() => State = ComponentState.Disabled;
}

public sealed class LoggingProvider : IProvider
{
    public string Name => "logging";
    public string ProviderType => "logging";
    public ComponentState State { get; private set; } = ComponentState.Disabled;

    public void Enable() => State = ComponentState.Enabled;
    public void Disable(int drainTimeout) => State = ComponentState.Disabled;
    public void Shutdown() => State = ComponentState.Disabled;
}

public sealed class ContentProvider : IProvider
{
    public string Name { get; }
    public string ProviderType => "content";
    public ComponentState State { get; private set; } = ComponentState.Disabled;
    public IContentStore Store { get; }

    public ContentProvider(string name, IContentStore store) { Name = name; Store = store; }

    public void Enable() => State = ComponentState.Enabled;
    public void Disable(int drainTimeout) => State = ComponentState.Disabled;
    public void Shutdown() => State = ComponentState.Disabled;
}
