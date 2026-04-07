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
    private readonly Dictionary<string, object?> _config;

    public ConfigProvider(Dictionary<string, object?> config) => _config = config;

    public void Enable() => State = ComponentState.Enabled;
    public void Disable(int drainTimeout) => State = ComponentState.Disabled;
    public void Shutdown() => State = ComponentState.Disabled;

    // --- Config access: dot-path navigation ---

    public string GetString(string key, string defaultVal = "")
    {
        var val = Navigate(key);
        return val?.ToString() ?? defaultVal;
    }

    public int GetInt(string key, int defaultVal = 0)
    {
        var val = Navigate(key);
        if (val is int i) return i;
        if (val is not null && int.TryParse(val.ToString(), out var parsed)) return parsed;
        return defaultVal;
    }

    public bool GetBool(string key, bool defaultVal = false)
    {
        var val = Navigate(key);
        if (val is bool b) return b;
        if (val is not null && bool.TryParse(val.ToString(), out var parsed)) return parsed;
        return defaultVal;
    }

    public bool Has(string key) => Navigate(key) is not null;

    public Dictionary<string, string> GetStringMap(string key)
    {
        var val = Navigate(key);
        var result = new Dictionary<string, string>();
        if (val is Dictionary<string, object?> sd)
            foreach (var (k, v) in sd) result[k] = v?.ToString() ?? "";
        else if (val is Dictionary<object, object?> od)
            foreach (var (k, v) in od) result[k.ToString()!] = v?.ToString() ?? "";
        return result;
    }

    public List<string> GetSubKeys(string key)
    {
        var val = Navigate(key);
        if (val is Dictionary<string, object?> sd) return new(sd.Keys);
        if (val is Dictionary<object, object?> od) return od.Keys.Select(k => k.ToString()!).ToList();
        return [];
    }

    public List<string> GetStringSlice(string key)
    {
        var val = Navigate(key);
        if (val is List<object?> list)
            return list.Where(x => x is not null).Select(x => x!.ToString()!).ToList();
        return [];
    }

    private object? Navigate(string dotPath)
    {
        var parts = dotPath.Split('.');
        object? current = _config;
        foreach (var part in parts)
        {
            if (current is Dictionary<string, object?> sd && sd.TryGetValue(part, out current)) continue;
            if (current is Dictionary<object, object?> od && od.TryGetValue(part, out current)) continue;
            return null;
        }
        return current;
    }
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
