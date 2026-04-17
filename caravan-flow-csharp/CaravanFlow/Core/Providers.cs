namespace CaravanFlow.Core;

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

    public bool TryGetProvider<T>(string name, out T? provider) where T : class, IProvider
    {
        provider = null;
        if (!_providers.TryGetValue(name, out var p)) return false;
        if (!p.IsEnabled) return false;
        provider = p as T;
        return provider is not null;
    }

    public IContentStore GetContentStoreOrDefault()
    {
        if (TryGetProvider<ContentProvider>("content", out var cp))
            return cp!.Store;
        return new MemoryContentStore();
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
        if (val is null) return defaultVal;
        if (val is int i) return i;
        return ConfigHelpers.ParseIntRaw(val.ToString() ?? "", $"config key '{key}'");
    }

    public bool GetBool(string key, bool defaultVal = false)
    {
        var val = Navigate(key);
        if (val is null) return defaultVal;
        if (val is bool b) return b;
        return ConfigHelpers.ParseBool(val.ToString(), key, defaultVal);
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
    public bool JsonOutput { get; set; }

    public void Enable() => State = ComponentState.Enabled;
    public void Disable(int drainTimeout) => State = ComponentState.Disabled;
    public void Shutdown() => State = ComponentState.Disabled;

    public void Log(string level, string component, string message, Dictionary<string, string>? extra = null)
    {
        if (State != ComponentState.Enabled) return;

        if (JsonOutput)
        {
            var sb = new System.Text.StringBuilder(128);
            sb.Append("{\"ts\":\"").Append(DateTime.UtcNow.ToString("o"))
              .Append("\",\"level\":\"").Append(level)
              .Append("\",\"component\":\"").Append(component)
              .Append("\",\"msg\":\"").Append(EscapeJson(message)).Append('"');
            if (extra is not null)
                foreach (var (k, v) in extra)
                    sb.Append(",\"").Append(k).Append("\":\"").Append(EscapeJson(v)).Append('"');
            sb.Append('}');
            Console.WriteLine(sb.ToString());
        }
        else
        {
            Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] [{level}] [{component}] {message}");
        }
    }

    private static string EscapeJson(string s) => s.Replace("\\", "\\\\").Replace("\"", "\\\"").Replace("\n", "\\n");
}

// --- Provenance provider: records FlowFile lifecycle events ---

public enum ProvenanceEventType { Created, Processed, Routed, Dropped, Failed }

public sealed class ProvenanceEvent
{
    public long FlowFileId { get; }
    public ProvenanceEventType EventType { get; }
    public string Component { get; }
    public string Details { get; }
    public long Timestamp { get; }

    public ProvenanceEvent(long ffId, ProvenanceEventType type, string component, string details = "")
    {
        FlowFileId = ffId;
        EventType = type;
        Component = component;
        Details = details;
        Timestamp = Environment.TickCount64;
    }
}

public sealed class ProvenanceProvider : IProvider
{
    public string Name => "provenance";
    public string ProviderType => "provenance";
    public ComponentState State { get; private set; } = ComponentState.Disabled;

    // Ring buffer — bounded, oldest events evicted
    private readonly ProvenanceEvent[] _events;
    private readonly int _capacity;
    private int _head;
    private int _count;
    private readonly object _lock = new();

    public ProvenanceProvider(int capacity = 100_000)
    {
        _capacity = capacity;
        _events = new ProvenanceEvent[capacity];
    }

    public void Enable() => State = ComponentState.Enabled;
    public void Disable(int drainTimeout) => State = ComponentState.Disabled;
    public void Shutdown() => State = ComponentState.Disabled;

    public void Record(long ffId, ProvenanceEventType type, string component, string details = "")
    {
        if (State != ComponentState.Enabled) return;
        var evt = new ProvenanceEvent(ffId, type, component, details);
        lock (_lock)
        {
            _events[_head] = evt;
            _head = (_head + 1) % _capacity;
            if (_count < _capacity) _count++;
        }
    }

    /// <summary>Get all events for a specific FlowFile ID, oldest first.</summary>
    public List<ProvenanceEvent> GetEvents(long ffId)
    {
        var result = new List<ProvenanceEvent>();
        lock (_lock)
        {
            int start = _count < _capacity ? 0 : _head;
            for (int i = 0; i < _count; i++)
            {
                var evt = _events[(start + i) % _capacity];
                if (evt.FlowFileId == ffId)
                    result.Add(evt);
            }
        }
        return result;
    }

    /// <summary>Get the most recent N events across all FlowFiles.</summary>
    public List<ProvenanceEvent> GetRecent(int n)
    {
        var result = new List<ProvenanceEvent>();
        lock (_lock)
        {
            int take = Math.Min(n, _count);
            int start = (_head - take + _capacity) % _capacity;
            for (int i = 0; i < take; i++)
                result.Add(_events[(start + i) % _capacity]);
        }
        return result;
    }

    public int Count { get { lock (_lock) return _count; } }
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

/// <summary>
/// Wraps the embedded schema registry as a provider. Always constructed at
/// startup (airgapped — no remote registry option). Processors that need
/// registry-backed schemas (e.g. ConvertOCFToRecord with readerSchemaSubject
/// or autoRegisterSubject) request this provider via
/// requires=["schema_registry"].
/// </summary>
public sealed class SchemaRegistryProvider : IProvider
{
    public string Name => "schema_registry";
    public string ProviderType => "schema_registry";
    public ComponentState State { get; private set; } = ComponentState.Disabled;
    public CaravanFlow.StdLib.EmbeddedSchemaRegistry Registry { get; }

    public SchemaRegistryProvider(CaravanFlow.StdLib.EmbeddedSchemaRegistry registry) => Registry = registry;

    public void Enable() => State = ComponentState.Enabled;
    public void Disable(int drainTimeout) => State = ComponentState.Disabled;
    public void Shutdown() => State = ComponentState.Disabled;
}

/// <summary>
/// Shells out to the system <c>git</c> binary so the worker can commit +
/// push its own <c>config.yaml</c> after a UI-driven edit. LibGit2Sharp
/// intentionally NOT embedded — ops teams keep control of auth/hooks/
/// credential rotation through their existing git config.
///
/// Enable path in config.yaml:
/// <code>
/// vc:
///   enabled: true
///   repo: /etc/caravanflow        # working tree; defaults to cwd
///   git: /usr/bin/git             # executable; defaults to "git" on PATH
///   remote: origin                # default remote for push
///   branch: main                  # default branch for push
/// </code>
///
/// Operations:
///   - <see cref="GetStatus"/>            — clean + ahead/behind + current branch
///   - <see cref="Commit(string?, string)"/> — stage + commit one path
///   - <see cref="Push"/>                 — push to configured remote/branch
///
/// All commands run with a short timeout and stderr captured. Results
/// surface as records so the admin API can echo them as JSON.
/// Mirrors caravan-flow-java's VersionControlProvider — same config
/// shape, same four public operations, same result records, so the
/// <c>/api/vc/*</c> JSON shapes match across tracks.
/// </summary>
public sealed class VersionControlProvider : IProvider
{
    public const string NameConst = "version_control";
    public const string TypeConst = "VersionControlProvider";

    public sealed record CommandResult(bool Ok, int ExitCode, string Stdout, string Stderr)
    {
        public CommandResult() : this(false, -1, "", "") { }
    }

    public sealed record Status(bool Enabled, bool Clean, int Ahead, int Behind, string Branch, string Error);

    public string Name => NameConst;
    public string ProviderType => TypeConst;
    public ComponentState State { get; private set; } = ComponentState.Disabled;

    public string Repo { get; }
    public string GitBinary { get; }
    public string Remote { get; }
    public string Branch { get; }

    public VersionControlProvider(string? repo, string? gitBinary, string? remote, string? branch)
    {
        Repo = string.IsNullOrEmpty(repo) ? Directory.GetCurrentDirectory() : repo;
        GitBinary = string.IsNullOrEmpty(gitBinary) ? "git" : gitBinary;
        Remote = string.IsNullOrEmpty(remote) ? "origin" : remote;
        Branch = string.IsNullOrEmpty(branch) ? "main" : branch;
    }

    public void Enable() => State = ComponentState.Enabled;
    public void Disable(int drainTimeout) => State = ComponentState.Disabled;
    public void Shutdown() => State = ComponentState.Disabled;
    public bool IsEnabled => State == ComponentState.Enabled;

    public Status GetStatus()
    {
        if (!IsEnabled) return new Status(false, false, 0, 0, "", "provider disabled");
        try
        {
            var branchRes = Run(new[] { GitBinary, "rev-parse", "--abbrev-ref", "HEAD" });
            var currentBranch = branchRes.Ok ? branchRes.Stdout.Trim() : Branch;

            var dirty = Run(new[] { GitBinary, "status", "--porcelain" });
            var clean = dirty.Ok && string.IsNullOrWhiteSpace(dirty.Stdout);

            int ahead = 0, behind = 0;
            var revList = Run(new[] {
                GitBinary, "rev-list", "--left-right", "--count",
                $"HEAD...{Remote}/{currentBranch}"
            });
            if (revList.Ok)
            {
                var parts = revList.Stdout.Trim().Split(
                    new[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length >= 2)
                {
                    int.TryParse(parts[0], out ahead);
                    int.TryParse(parts[1], out behind);
                }
            }
            return new Status(true, clean, ahead, behind, currentBranch, "");
        }
        catch (Exception ex)
        {
            return new Status(true, false, 0, 0, "", ex.Message);
        }
    }

    public CommandResult Commit(string? relPath, string message)
    {
        if (!IsEnabled) return new CommandResult(false, -1, "", "provider disabled");
        if (string.IsNullOrEmpty(message))
            return new CommandResult(false, -1, "", "commit message must not be blank");
        try
        {
            var add = Run(new[] { GitBinary, "add", string.IsNullOrEmpty(relPath) ? "." : relPath });
            if (!add.Ok) return add;
            return Run(new[] { GitBinary, "commit", "-m", message });
        }
        catch (Exception ex)
        {
            return new CommandResult(false, -1, "", ex.Message);
        }
    }

    public CommandResult Push()
    {
        if (!IsEnabled) return new CommandResult(false, -1, "", "provider disabled");
        try { return Run(new[] { GitBinary, "push", Remote, Branch }); }
        catch (Exception ex) { return new CommandResult(false, -1, "", ex.Message); }
    }

    /// <summary>
    /// Runs an arbitrary process against the configured working tree
    /// with a 15-second timeout. Public so tests can drive git
    /// commands directly against the configured repo.
    /// </summary>
    public CommandResult Run(IReadOnlyList<string> command)
    {
        if (command.Count == 0)
            return new CommandResult(false, -1, "", "empty command");

        using var proc = new System.Diagnostics.Process();
        proc.StartInfo.FileName = command[0];
        for (int i = 1; i < command.Count; i++) proc.StartInfo.ArgumentList.Add(command[i]);
        proc.StartInfo.WorkingDirectory = Repo;
        proc.StartInfo.RedirectStandardOutput = true;
        proc.StartInfo.RedirectStandardError = true;
        proc.StartInfo.UseShellExecute = false;
        proc.StartInfo.CreateNoWindow = true;

        try { proc.Start(); }
        catch (Exception ex) { return new CommandResult(false, -1, "", ex.Message); }

        // Start async read tasks BEFORE WaitForExit so the child can't
        // block on a full pipe buffer (tens-of-KB limit on Linux).
        var stdoutTask = proc.StandardOutput.ReadToEndAsync();
        var stderrTask = proc.StandardError.ReadToEndAsync();

        var exited = proc.WaitForExit(milliseconds:15_000);
        if (!exited)
        {
            try { proc.Kill(entireProcessTree: true); } catch { /* best effort */ }
            proc.WaitForExit(milliseconds:2_000);
            var partialOut = stdoutTask.IsCompletedSuccessfully ? stdoutTask.Result : "";
            return new CommandResult(false, -1, partialOut, "timeout");
        }

        var stdout = stdoutTask.GetAwaiter().GetResult();
        var stderr = stderrTask.GetAwaiter().GetResult();
        var ok = proc.ExitCode == 0;
        return new CommandResult(ok, proc.ExitCode, stdout, stderr);
    }

    /// <summary>
    /// Returns the status shape used by <c>GET /api/vc/status</c>.
    /// Matches caravan-flow-java's response key set 1:1.
    /// </summary>
    public Dictionary<string, object?> StatusJson()
    {
        var s = GetStatus();
        var out_ = new Dictionary<string, object?>
        {
            ["enabled"] = s.Enabled,
            ["clean"] = s.Clean,
            ["ahead"] = s.Ahead,
            ["behind"] = s.Behind,
            ["branch"] = s.Branch,
            ["remote"] = Remote,
            ["repo"] = Repo,
        };
        if (!string.IsNullOrEmpty(s.Error)) out_["error"] = s.Error;
        return out_;
    }
}
