using ZincFlow.Core;

namespace ZincFlow.Fabric;

/// <summary>
/// Fabric: main runtime — wires processors, queues, connections, and lifecycle.
/// Uses NiFi-style explicit processor connections instead of global routing rules.
/// Async processor loops run on ThreadPool threads.
/// </summary>
public sealed class Fabric
{
    private readonly Registry _reg;
    private readonly ProcessorContext _globalCtx;
    private readonly Dictionary<string, IProcessor> _procs = new();
    private readonly List<string> _processorNames = new();
    private readonly Dictionary<string, FlowQueue> _queues = new();
    private readonly Dictionary<string, ProcessSession> _sessions = new();
    private readonly Dictionary<string, ComponentState> _processorStates = new();
    private readonly Dictionary<string, List<string>> _processorRequires = new();
    private readonly Dictionary<string, Dictionary<string, List<string>>> _processorConnections = new();
    private FlowQueue _ingestQueue;
    private readonly DLQ _dlq = new();
    private readonly Dictionary<string, IConnectorSource> _sources = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly Dictionary<string, CancellationTokenSource> _processorCts = new();
    private readonly Dictionary<string, (string Type, Dictionary<string, string> Config, List<string> Requires)> _processorDefs = new();
    private volatile List<string> _entryPoints = new();

    private long _processedCount;
    private volatile bool _running;

    // Defaults (overridable from config)
    private int _queueMaxCount = 10_000;
    private long _queueMaxBytes = 100 * 1024 * 1024;
    private long _visibilityTimeoutMs = 30_000;
    private int _drainTimeoutSeconds = 60;
    private int _maxRetries = 5;
    private int _maxHops = 50;
    private string _walDir = "";
    private int _walMaxSizeMb = 100;
    private int _walCompactIntervalMs = 60_000;
    private ProvenanceProvider? _provenance;
    private LoggingProvider? _log;

    public Fabric(Registry reg, ProcessorContext globalCtx)
    {
        _reg = reg;
        _globalCtx = globalCtx;
        _ingestQueue = new FlowQueue("ingest", _queueMaxCount, _queueMaxBytes, _visibilityTimeoutMs);
        _log = globalCtx.GetProvider("logging") as LoggingProvider;
        _provenance = globalCtx.GetProvider("provenance") as ProvenanceProvider;
    }

    // --- Config loading ---

    public void LoadFlow(Dictionary<string, object?> config)
    {
        // Read defaults
        if (TryGetConfig<int>(config, "defaults.backpressure.max_count", out var mc)) _queueMaxCount = mc;
        if (TryGetConfig<int>(config, "defaults.backpressure.max_retries", out var mr)) _maxRetries = mr;
        if (TryGetConfig<int>(config, "defaults.backpressure.drain_timeout", out var dt)) _drainTimeoutSeconds = dt;
        if (TryGetConfig<int>(config, "defaults.backpressure.max_hops", out var mh)) _maxHops = mh;

        // WAL config
        _walDir = GetStr(config, "defaults.wal.dir");
        if (string.IsNullOrEmpty(_walDir)) _walDir = "";
        if (TryGetConfig<int>(config, "defaults.wal.max_size_mb", out var wsm)) _walMaxSizeMb = wsm;
        if (TryGetConfig<int>(config, "defaults.wal.compact_interval_ms", out var wci)) _walCompactIntervalMs = wci;

        // Recreate ingest queue with WAL if configured
        if (_walDir != "")
        {
            Directory.CreateDirectory(_walDir);
            var ingestWal = new QueueWAL(Path.Combine(_walDir, "ingest.wal"), _walMaxSizeMb, _walCompactIntervalMs);
            _ingestQueue = new FlowQueue("ingest", _queueMaxCount, _queueMaxBytes, _visibilityTimeoutMs, ingestWal);
        }

        // Read processors
        var flowDict = AsStringDict(config.GetValueOrDefault("flow"));
        if (flowDict is not null)
        {
            var procDefs = AsStringDict(flowDict.GetValueOrDefault("processors"));
            if (procDefs is not null)
            {
                foreach (var (name, defObj) in procDefs)
                {
                    var def = AsStringDict(defObj);
                    if (def is null) continue;

                    var typeName = GetStr(def, "type");
                    if (!_reg.Has(typeName)) { Console.Error.WriteLine($"Unknown processor type: {typeName}"); continue; }

                    // Config map
                    var procConfig = new Dictionary<string, string>();
                    var cfgDict = AsStringDict(def.GetValueOrDefault("config"));
                    if (cfgDict is not null)
                    {
                        foreach (var (k, v) in cfgDict)
                            procConfig[k] = v?.ToString() ?? "";
                    }

                    // Requires
                    var requires = GetStringList(def, "requires") ?? [];
                    _processorRequires[name] = requires;

                    foreach (var pn in requires)
                        _globalCtx.RegisterDependent(pn, name);

                    // Connections
                    var connections = ParseConnections(def);
                    _processorConnections[name] = connections;

                    var ctx = BuildScopedContext(requires);
                    var proc = _reg.Create(typeName, ctx, procConfig);
                    _procs[name] = proc;
                    _processorNames.Add(name);
                    _processorStates[name] = ComponentState.Enabled;
                    _processorDefs[name] = (typeName, new Dictionary<string, string>(procConfig), new List<string>(requires));

                    QueueWAL? qWal = _walDir != "" ? new QueueWAL(Path.Combine(_walDir, $"{name}.wal"), _walMaxSizeMb, _walCompactIntervalMs) : null;
                    var queue = new FlowQueue(name, _queueMaxCount, _queueMaxBytes, _visibilityTimeoutMs, qWal);
                    _queues[name] = queue;

                    Console.WriteLine($"[fabric] processor created: {name} (type={typeName})");
                }

                // DAG validation and entry-point computation
                var dagResult = DagValidator.Validate(_processorConnections);
                foreach (var err in dagResult.Errors)
                    Console.Error.WriteLine($"[fabric] DAG error: {err}");
                foreach (var warn in dagResult.Warnings)
                    Console.WriteLine($"[fabric] DAG warning: {warn}");
                _entryPoints = dagResult.EntryPoints;
                Console.WriteLine($"[fabric] entry points: [{string.Join(", ", _entryPoints)}]");
            }
        }

        // Create sessions with per-processor connections
        foreach (var name in _processorNames)
        {
            var connections = _processorConnections.GetValueOrDefault(name) ?? new();
            var session = new ProcessSession(_queues[name], _procs[name], name, connections, _queues, _dlq, _maxRetries, _provenance, _maxHops);
            _sessions[name] = session;
        }
    }

    // --- Async start/stop ---

    public void StartAsync()
    {
        _running = true;

        // Replay WAL for all queues before processing begins
        int totalRestored = _ingestQueue.ReplayWAL();
        foreach (var (name, queue) in _queues)
            totalRestored += queue.ReplayWAL();
        if (totalRestored > 0)
        {
            _log?.Log("INFO", "fabric", $"WAL replay: restored {totalRestored} entries");
            Console.WriteLine($"[fabric] WAL replay: restored {totalRestored} entries");
        }

        _ingestQueue.StartReaper(_cts.Token);
        _ingestQueue.StartWALCompaction(_cts.Token);

        // Processor loops
        foreach (var name in _processorNames)
        {
            _queues[name].StartReaper(_cts.Token);
            _queues[name].StartWALCompaction(_cts.Token);
            var procCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
            _processorCts[name] = procCts;
            var procName = name;
            _ = Task.Run(() => ProcessorLoop(procName, procCts.Token), _cts.Token);
        }

        // Ingest fan-out loop
        _ = Task.Run(IngestFanOutLoop, _cts.Token);

        // Start all connector sources
        foreach (var (name, source) in _sources)
        {
            if (!source.IsRunning)
                source.Start(Ingest, _cts.Token);
            Console.WriteLine($"[fabric] source started: {name} (type={source.SourceType})");
        }

        _log?.Log("INFO", "fabric", $"started: {_processorNames.Count} processors, {_sources.Count} sources");
        Console.WriteLine($"[fabric] started ({_processorNames.Count} processors, {_sources.Count} sources)");
    }

    private void ProcessorLoop(string procName, CancellationToken ct)
    {
        while (_running && !ct.IsCancellationRequested)
        {
            if (_processorStates.GetValueOrDefault(procName) != ComponentState.Enabled)
            {
                Thread.Sleep(100);
                continue;
            }
            if (_sessions.TryGetValue(procName, out var session) && session.Execute())
                Interlocked.Increment(ref _processedCount);
            else
                Thread.Sleep(10);
        }
    }

    /// <summary>
    /// Fan-out ingested FlowFiles to all entry-point processor queues.
    /// Entry points are processors not referenced as a target in any connection.
    /// </summary>
    private void IngestFanOutLoop()
    {
        while (_running)
        {
            var entry = _ingestQueue.Claim();
            if (entry is null)
            {
                Thread.Sleep(10);
                continue;
            }

            var entryPoints = _entryPoints; // volatile read
            if (entryPoints.Count == 0)
            {
                _log?.Log("WARN", "fabric", $"no entry-point processors, dropping ff-{entry.FlowFile.NumericId}");
                _ingestQueue.Ack(entry.Id);
                FlowFile.Return(entry.FlowFile);
                continue;
            }

            // Pre-check all entry-point queues
            bool allReady = true;
            foreach (var ep in entryPoints)
            {
                if (_queues.TryGetValue(ep, out var q) && !q.HasCapacity())
                {
                    allReady = false;
                    break;
                }
            }

            if (!allReady)
            {
                _ingestQueue.Nack(entry.Id);
                Thread.Sleep(10);
                continue;
            }

            // Fan out to all entry-point queues
            foreach (var ep in entryPoints)
            {
                if (_queues.TryGetValue(ep, out var q))
                    q.Offer(entry.FlowFile);
            }
            _ingestQueue.Ack(entry.Id);
        }
    }

    public void StopAsync()
    {
        _running = false;
        foreach (var source in _sources.Values)
            source.Stop();
        _cts.Cancel();
    }

    // --- Connector source management ---

    public void AddSource(IConnectorSource source)
    {
        _sources[source.Name] = source;
        if (_running)
            source.Start(Ingest, _cts.Token);
    }

    public bool StartSource(string name)
    {
        if (!_sources.TryGetValue(name, out var source)) return false;
        if (!source.IsRunning)
            source.Start(Ingest, _cts.Token);
        return true;
    }

    public bool StopSource(string name)
    {
        if (!_sources.TryGetValue(name, out var source)) return false;
        source.Stop();
        return true;
    }

    public List<(string Name, string Type, bool Running)> GetSources()
    {
        var result = new List<(string, string, bool)>();
        foreach (var (name, src) in _sources)
            result.Add((name, src.SourceType, src.IsRunning));
        return result;
    }

    // --- Ingest ---

    public bool Ingest(FlowFile ff) => _ingestQueue.Offer(ff);

    public bool ReplayToQueue(string queueName, FlowFile ff)
    {
        if (_queues.TryGetValue(queueName, out var q))
            return q.Offer(ff);
        return _ingestQueue.Offer(ff);
    }

    // --- Accessors ---

    public ProcessorContext GetContext() => _globalCtx;
    public FlowQueue GetIngestQueue() => _ingestQueue;
    public DLQ GetDLQ() => _dlq;
    public ProvenanceProvider? GetProvenance() => _provenance;
    public List<string> GetProcessorNames() => new(_processorNames);
    public Registry GetRegistry() => _reg;
    public Dictionary<string, Dictionary<string, List<string>>> GetConnections() => new(_processorConnections);
    public List<string> GetEntryPoints() => new(_entryPoints);

    public Dictionary<string, int> GetStats() => new()
    {
        ["processed"] = (int)Interlocked.Read(ref _processedCount),
        ["dlq"] = _dlq.Count
    };

    public Dictionary<string, Dictionary<string, int>> GetQueueStats()
    {
        var stats = new Dictionary<string, Dictionary<string, int>>();
        foreach (var name in _processorNames)
        {
            stats[name] = new Dictionary<string, int>
            {
                ["visible"] = _queues[name].VisibleCount,
                ["invisible"] = _queues[name].InvisibleCount,
                ["total"] = _queues[name].VisibleCount + _queues[name].InvisibleCount
            };
        }
        return stats;
    }

    // --- Processor lifecycle ---

    public bool EnableProcessor(string name)
    {
        if (!_procs.ContainsKey(name)) return false;
        if (_processorRequires.TryGetValue(name, out var requires))
        {
            foreach (var pn in requires)
            {
                var prov = _globalCtx.GetProvider(pn);
                if (prov is null || !prov.IsEnabled) return false;
            }
        }
        _processorStates[name] = ComponentState.Enabled;
        return true;
    }

    public bool DisableProcessor(string name, int drainSecs)
    {
        if (!_procs.ContainsKey(name)) return false;
        _processorStates[name] = ComponentState.Draining;

        _ = Task.Run(async () =>
        {
            var elapsed = 0;
            while (elapsed < drainSecs)
            {
                var depth = _queues[name].VisibleCount + _queues[name].InvisibleCount;
                if (depth == 0) break;
                await Task.Delay(100);
                elapsed++;
            }
            _processorStates[name] = ComponentState.Disabled;
        });
        return true;
    }

    public ComponentState GetProcessorState(string name)
        => _processorStates.GetValueOrDefault(name, ComponentState.Disabled);

    // --- Provider lifecycle ---

    public bool EnableProvider(string name)
    {
        var prov = _globalCtx.GetProvider(name);
        if (prov is null) return false;
        prov.Enable();
        return true;
    }

    public bool DisableProvider(string name, int drainSecs)
    {
        var prov = _globalCtx.GetProvider(name);
        if (prov is null) return false;

        // Cascade: drain dependent processors
        foreach (var procName in _globalCtx.GetDependents(name))
        {
            if (_processorStates.GetValueOrDefault(procName) == ComponentState.Enabled)
                DisableProcessor(procName, drainSecs);
        }
        prov.Disable(drainSecs);
        return true;
    }

    // --- Dynamic processor management ---

    public bool AddProcessor(string name, string typeName, Dictionary<string, string> config,
        List<string>? requires = null, Dictionary<string, List<string>>? connections = null)
    {
        if (_procs.ContainsKey(name) || !_reg.Has(typeName)) return false;
        requires ??= [];
        connections ??= new();
        _processorRequires[name] = requires;
        _processorConnections[name] = connections;
        foreach (var pn in requires)
            _globalCtx.RegisterDependent(pn, name);

        var ctx = BuildScopedContext(requires);
        var proc = _reg.Create(typeName, ctx, config);
        _procs[name] = proc;
        _processorNames.Add(name);
        _processorStates[name] = ComponentState.Enabled;
        _processorDefs[name] = (typeName, new Dictionary<string, string>(config), new List<string>(requires));

        QueueWAL? qWal = _walDir != "" ? new QueueWAL(Path.Combine(_walDir, $"{name}.wal"), _walMaxSizeMb, _walCompactIntervalMs) : null;
        var queue = new FlowQueue(name, _queueMaxCount, _queueMaxBytes, _visibilityTimeoutMs, qWal);
        _queues[name] = queue;
        var session = new ProcessSession(queue, proc, name, connections, _queues, _dlq, _maxRetries, _provenance, _maxHops);
        _sessions[name] = session;

        RecomputeEntryPoints();

        if (_running)
        {
            queue.StartReaper(_cts.Token);
            queue.StartWALCompaction(_cts.Token);
            var procCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
            _processorCts[name] = procCts;
            _ = Task.Run(() => ProcessorLoop(name, procCts.Token), _cts.Token);
        }
        return true;
    }

    public bool RemoveProcessor(string name)
    {
        if (!_procs.ContainsKey(name)) return false;
        _processorStates[name] = ComponentState.Disabled;
        if (_processorCts.TryGetValue(name, out var cts))
        {
            cts.Cancel();
            _processorCts.Remove(name);
        }
        _procs.Remove(name);
        _processorNames.Remove(name);
        _sessions.Remove(name);
        _processorDefs.Remove(name);
        _processorRequires.Remove(name);
        _processorConnections.Remove(name);
        RecomputeEntryPoints();
        return true;
    }

    // --- Hot reload ---

    /// <summary>
    /// Reload flow from new config. Diffs processors and connections against current state.
    /// Returns (added, removed, updated, connectionsChanged).
    /// Does NOT reload sources or providers (those are infrastructure — require restart).
    /// </summary>
    public (int Added, int Removed, int Updated, int ConnectionsChanged) ReloadFlow(Dictionary<string, object?> config)
    {
        int added = 0, removed = 0, updated = 0, connectionsChanged = 0;

        // Read new defaults
        if (TryGetConfig<int>(config, "defaults.backpressure.max_count", out var mc)) _queueMaxCount = mc;
        if (TryGetConfig<int>(config, "defaults.backpressure.max_retries", out var mr)) _maxRetries = mr;
        if (TryGetConfig<int>(config, "defaults.backpressure.drain_timeout", out var dt)) _drainTimeoutSeconds = dt;
        if (TryGetConfig<int>(config, "defaults.backpressure.max_hops", out var mh)) _maxHops = mh;

        // Parse new processor defs from config
        var newDefs = new Dictionary<string, (string Type, Dictionary<string, string> Config, List<string> Requires, Dictionary<string, List<string>> Connections)>();
        var flowDict = AsStringDict(config.GetValueOrDefault("flow"));
        if (flowDict is not null)
        {
            var procDefs = AsStringDict(flowDict.GetValueOrDefault("processors"));
            if (procDefs is not null)
            {
                foreach (var (name, defObj) in procDefs)
                {
                    var def = AsStringDict(defObj);
                    if (def is null) continue;
                    var typeName = GetStr(def, "type");
                    if (!_reg.Has(typeName)) continue;
                    var procConfig = new Dictionary<string, string>();
                    var cfgDict = AsStringDict(def.GetValueOrDefault("config"));
                    if (cfgDict is not null)
                        foreach (var (k, v) in cfgDict)
                            procConfig[k] = v?.ToString() ?? "";
                    var requires = GetStringList(def, "requires") ?? [];
                    var connections = ParseConnections(def);
                    newDefs[name] = (typeName, procConfig, requires, connections);
                }
            }
        }

        // Remove processors no longer in config
        var currentNames = new List<string>(_processorDefs.Keys);
        foreach (var name in currentNames)
        {
            if (!newDefs.ContainsKey(name))
            {
                RemoveProcessor(name);
                removed++;
                _log?.Log("INFO", "hot-reload", $"removed processor: {name}");
            }
        }

        // Add new processors
        foreach (var (name, def) in newDefs)
        {
            if (!_processorDefs.ContainsKey(name))
            {
                if (AddProcessor(name, def.Type, def.Config, def.Requires, def.Connections))
                {
                    added++;
                    _log?.Log("INFO", "hot-reload", $"added processor: {name} (type={def.Type})");
                }
            }
        }

        // Update changed processors or connections
        foreach (var (name, newDef) in newDefs)
        {
            if (!_processorDefs.TryGetValue(name, out var oldDef)) continue;
            var oldConn = _processorConnections.GetValueOrDefault(name) ?? new();
            bool procChanged = oldDef.Type != newDef.Type || !DictEqual(oldDef.Config, newDef.Config) || !ListEqual(oldDef.Requires, newDef.Requires);
            bool connChanged = !ConnectionsEqual(oldConn, newDef.Connections);

            if (!procChanged && !connChanged) continue;

            if (procChanged)
            {
                // Processor type/config changed — rebuild processor + session
                _processorStates[name] = ComponentState.Disabled;
                if (_processorCts.TryGetValue(name, out var cts))
                {
                    cts.Cancel();
                    _processorCts.Remove(name);
                }

                var ctx = BuildScopedContext(newDef.Requires);
                var proc = _reg.Create(newDef.Type, ctx, newDef.Config);
                _procs[name] = proc;
                _processorRequires[name] = newDef.Requires;
                _processorDefs[name] = (newDef.Type, new Dictionary<string, string>(newDef.Config), new List<string>(newDef.Requires));
                _processorConnections[name] = newDef.Connections;

                var session = new ProcessSession(_queues[name], proc, name, newDef.Connections, _queues, _dlq, _maxRetries, _provenance, _maxHops);
                _sessions[name] = session;
                _processorStates[name] = ComponentState.Enabled;

                if (_running)
                {
                    var procCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
                    _processorCts[name] = procCts;
                    _ = Task.Run(() => ProcessorLoop(name, procCts.Token), _cts.Token);
                }
                updated++;
                _log?.Log("INFO", "hot-reload", $"updated processor: {name} (type={newDef.Type})");
            }
            else
            {
                // Only connections changed — rebuild session with same processor
                _processorConnections[name] = newDef.Connections;
                var session = new ProcessSession(_queues[name], _procs[name], name, newDef.Connections, _queues, _dlq, _maxRetries, _provenance, _maxHops);
                _sessions[name] = session;
                connectionsChanged++;
                _log?.Log("INFO", "hot-reload", $"updated connections: {name}");
            }
        }

        RecomputeEntryPoints();

        var total = added + removed + updated + connectionsChanged;
        if (total > 0)
            Console.WriteLine($"[hot-reload] applied: +{added} -{removed} ~{updated} processors, {connectionsChanged} connections");
        else
            Console.WriteLine("[hot-reload] no changes detected");

        return (added, removed, updated, connectionsChanged);
    }

    private void RecomputeEntryPoints()
    {
        var dagResult = DagValidator.Validate(_processorConnections);
        _entryPoints = dagResult.EntryPoints;
    }

    public void Status()
    {
        Console.WriteLine($"[fabric] processors={_processorNames.Count} processed={Interlocked.Read(ref _processedCount)} dlq={_dlq.Count}");
    }

    // --- Helpers ---

    private ScopedContext BuildScopedContext(List<string> requires)
    {
        var scoped = new Dictionary<string, IProvider>();
        if (requires.Count == 0)
        {
            foreach (var pn in _globalCtx.ListProviders())
            {
                var p = _globalCtx.GetProvider(pn);
                if (p is not null) scoped[pn] = p;
            }
        }
        else
        {
            foreach (var pn in requires)
            {
                var p = _globalCtx.GetProvider(pn);
                if (p is not null) scoped[pn] = p;
            }
        }
        return new ScopedContext(scoped);
    }

    internal static Dictionary<string, List<string>> ParseConnections(Dictionary<string, object?> def)
    {
        var connections = new Dictionary<string, List<string>>();
        var connDefs = AsStringDict(def.GetValueOrDefault("connections"));
        if (connDefs is null) return connections;
        foreach (var (rel, _) in connDefs)
        {
            var dests = GetStringList(connDefs, rel);
            if (dests is not null && dests.Count > 0)
                connections[rel] = dests;
            else
            {
                // Handle single string value (not a list)
                var single = GetStr(connDefs, rel);
                if (!string.IsNullOrEmpty(single))
                    connections[rel] = [single];
            }
        }
        return connections;
    }

    private static bool DictEqual(Dictionary<string, string> a, Dictionary<string, string> b)
    {
        if (a.Count != b.Count) return false;
        foreach (var (k, v) in a)
            if (!b.TryGetValue(k, out var bv) || v != bv) return false;
        return true;
    }

    private static bool ListEqual(List<string> a, List<string> b)
    {
        if (a.Count != b.Count) return false;
        for (int i = 0; i < a.Count; i++)
            if (a[i] != b[i]) return false;
        return true;
    }

    private static bool ConnectionsEqual(Dictionary<string, List<string>> a, Dictionary<string, List<string>> b)
    {
        if (a.Count != b.Count) return false;
        foreach (var (k, v) in a)
        {
            if (!b.TryGetValue(k, out var bv)) return false;
            if (!ListEqual(v, bv)) return false;
        }
        return true;
    }

    private static bool TryGetConfig<T>(Dictionary<string, object?> config, string dotPath, out T value)
    {
        value = default!;
        var parts = dotPath.Split('.');
        object? current = config;
        foreach (var part in parts)
        {
            if (TryGetDictValue(current, part, out current))
                continue;
            return false;
        }
        if (current is T t) { value = t; return true; }
        if (current is not null && typeof(T) == typeof(int) && int.TryParse(current.ToString(), out var i))
        {
            value = (T)(object)i;
            return true;
        }
        return false;
    }

    // --- YAML normalization: handle both Dictionary<string,object?> and Dictionary<object,object?> ---

    internal static bool TryGetDictValue(object? dict, string key, out object? value)
    {
        value = null;
        if (dict is Dictionary<string, object?> sd)
            return sd.TryGetValue(key, out value);
        if (dict is Dictionary<object, object?> od)
            return od.TryGetValue(key, out value);
        return false;
    }

    internal static Dictionary<string, object?>? AsStringDict(object? obj)
    {
        if (obj is Dictionary<string, object?> sd) return sd;
        if (obj is Dictionary<object, object?> od)
            return od.ToDictionary(kv => kv.Key.ToString()!, kv => kv.Value);
        return null;
    }

    internal static string GetStr(object? dict, string key, string def = "")
    {
        if (TryGetDictValue(dict, key, out var val) && val is not null)
            return val.ToString() ?? def;
        return def;
    }

    internal static List<string>? GetStringList(object? dict, string key)
    {
        if (!TryGetDictValue(dict, key, out var val)) return null;
        if (val is List<object?> list)
            return list.Where(x => x is not null).Select(x => x!.ToString()!).ToList();
        return null;
    }
}
