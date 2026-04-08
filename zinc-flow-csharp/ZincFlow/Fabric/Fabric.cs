using ZincFlow.Core;

namespace ZincFlow.Fabric;

/// <summary>
/// Fabric: main runtime — wires processors, queues, routing, and lifecycle.
/// Async processor loops run on ThreadPool threads.
/// </summary>
public sealed class Fabric
{
    private readonly Registry _reg;
    private readonly RulesEngine _engine = new();
    private readonly ProcessorContext _globalCtx;
    private readonly Dictionary<string, IProcessor> _procs = new();
    private readonly List<string> _processorNames = new();
    private readonly Dictionary<string, FlowQueue> _queues = new();
    private readonly Dictionary<string, ProcessSession> _sessions = new();
    private readonly Dictionary<string, ComponentState> _processorStates = new();
    private readonly Dictionary<string, List<string>> _processorRequires = new();
    private FlowQueue _ingestQueue;
    private readonly DLQ _dlq = new();
    private readonly Dictionary<string, IConnectorSource> _sources = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly Dictionary<string, CancellationTokenSource> _processorCts = new();
    private readonly Dictionary<string, (string Type, Dictionary<string, string> Config, List<string> Requires)> _processorDefs = new();

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
            }

            // Read routes
            var routeDefs = AsStringDict(flowDict.GetValueOrDefault("routes"));
            if (routeDefs is not null)
            {
                var rules = new List<RoutingRule>();
                foreach (var (ruleName, rObj) in routeDefs)
                {
                    var rDef = AsStringDict(rObj);
                    if (rDef is null) continue;
                    var dest = GetStr(rDef, "destination");
                    var condDict = AsStringDict(rDef.GetValueOrDefault("condition"));
                    if (condDict is null) continue;

                    var attr = GetStr(condDict, "attribute");
                    var op = GetStr(condDict, "operator");
                    var val = GetStr(condDict, "value");

                    rules.Add(new RoutingRule(ruleName, attr, ParseOperator(op), val, dest));
                }
                if (rules.Count > 0)
                {
                    _engine.AddOrReplaceRuleset("flow", rules);
                    Console.WriteLine($"[fabric] routes configured: {rules.Count}");
                }
            }
        }

        // Create sessions
        foreach (var name in _processorNames)
        {
            var session = new ProcessSession(_queues[name], _procs[name], name, _engine, _queues, _dlq, _maxRetries, _provenance, _maxHops);
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

        // Ingest router loop
        _ = Task.Run(IngestRouterLoop, _cts.Token);

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

    private void IngestRouterLoop()
    {
        // Reusable destination buffer for ingest routing
        var destBuffer = new List<string>();

        while (_running)
        {
            var entry = _ingestQueue.Claim();
            if (entry is null)
            {
                Thread.Sleep(10);
                continue;
            }

            _engine.GetDestinations(entry.FlowFile.Attributes, destBuffer);
            if (destBuffer.Count == 0)
            {
                _log?.Log("WARN", "fabric", $"no routes matched for ff-{entry.FlowFile.NumericId}, dropping");
                var ff = entry.FlowFile;
                _ingestQueue.Ack(entry.Id);
                FlowFile.Return(ff);
                continue;
            }

            // Pre-check all destinations
            bool allReady = true;
            foreach (var dest in destBuffer)
            {
                if (_queues.TryGetValue(dest, out var q) && !q.HasCapacity())
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

            // All-or-nothing commit
            foreach (var dest in destBuffer)
            {
                if (_queues.TryGetValue(dest, out var q))
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
    public RulesEngine GetEngine() => _engine;

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

    // --- Dynamic processor/route management ---

    public bool AddProcessor(string name, string typeName, Dictionary<string, string> config, List<string>? requires = null)
    {
        if (_procs.ContainsKey(name) || !_reg.Has(typeName)) return false;
        requires ??= [];
        _processorRequires[name] = requires;
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
        var session = new ProcessSession(queue, proc, name, _engine, _queues, _dlq, _maxRetries, _provenance, _maxHops);
        _sessions[name] = session;

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
        return true;
    }

    public void AddRoute(string name, string attr, string op, string value, string dest)
    {
        var rule = new RoutingRule(name, attr, ParseOperator(op), value, dest);
        var existing = _engine.GetAllRules();
        existing.Add(rule);
        _engine.AddOrReplaceRuleset("flow", existing);
    }

    public bool RemoveRoute(string name)
    {
        var existing = _engine.GetAllRules();
        var updated = existing.Where(r => r.Name != name).ToList();
        if (updated.Count == existing.Count) return false;
        _engine.AddOrReplaceRuleset("flow", updated);
        return true;
    }

    public bool ToggleRoute(string name)
    {
        var exists = _engine.GetAllRules().Any(r => r.Name == name);
        if (!exists) return false;
        _engine.ToggleRule("flow", name);
        return true;
    }

    // --- Hot reload ---

    /// <summary>
    /// Reload flow from new config. Diffs processors and routes against current state.
    /// Returns (added, removed, updated, routesChanged).
    /// Does NOT reload sources or providers (those are infrastructure — require restart).
    /// </summary>
    public (int Added, int Removed, int Updated, int RoutesChanged) ReloadFlow(Dictionary<string, object?> config)
    {
        int added = 0, removed = 0, updated = 0, routesChanged = 0;

        // Read new defaults
        if (TryGetConfig<int>(config, "defaults.backpressure.max_count", out var mc)) _queueMaxCount = mc;
        if (TryGetConfig<int>(config, "defaults.backpressure.max_retries", out var mr)) _maxRetries = mr;
        if (TryGetConfig<int>(config, "defaults.backpressure.drain_timeout", out var dt)) _drainTimeoutSeconds = dt;
        if (TryGetConfig<int>(config, "defaults.backpressure.max_hops", out var mh)) _maxHops = mh;

        // Parse new processor defs from config
        var newDefs = new Dictionary<string, (string Type, Dictionary<string, string> Config, List<string> Requires)>();
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
                    newDefs[name] = (typeName, procConfig, requires);
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
                if (AddProcessor(name, def.Type, def.Config, def.Requires))
                {
                    added++;
                    _log?.Log("INFO", "hot-reload", $"added processor: {name} (type={def.Type})");
                }
            }
        }

        // Update changed processors (different type or config)
        foreach (var (name, newDef) in newDefs)
        {
            if (!_processorDefs.TryGetValue(name, out var oldDef)) continue;
            if (oldDef.Type == newDef.Type && DictEqual(oldDef.Config, newDef.Config) && ListEqual(oldDef.Requires, newDef.Requires))
                continue;

            // Drain the queue before swapping — keep the queue, replace the processor + session
            _processorStates[name] = ComponentState.Disabled;
            if (_processorCts.TryGetValue(name, out var cts))
            {
                cts.Cancel();
                _processorCts.Remove(name);
            }

            // Rebuild processor with new config
            var ctx = BuildScopedContext(newDef.Requires);
            var proc = _reg.Create(newDef.Type, ctx, newDef.Config);
            _procs[name] = proc;
            _processorRequires[name] = newDef.Requires;
            _processorDefs[name] = (newDef.Type, new Dictionary<string, string>(newDef.Config), new List<string>(newDef.Requires));

            // Rebuild session pointing to the same queue
            var session = new ProcessSession(_queues[name], proc, name, _engine, _queues, _dlq, _maxRetries, _provenance, _maxHops);
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

        // Reload routes
        if (flowDict is not null)
        {
            var newRules = ParseRoutes(flowDict);
            var oldRules = _engine.GetAllRules();
            if (!RulesEqual(oldRules, newRules))
            {
                _engine.AddOrReplaceRuleset("flow", newRules);
                routesChanged = newRules.Count;
                _log?.Log("INFO", "hot-reload", $"routes updated: {newRules.Count} rules");
            }
        }

        var total = added + removed + updated + routesChanged;
        if (total > 0)
            Console.WriteLine($"[hot-reload] applied: +{added} -{removed} ~{updated} processors, {routesChanged} routes");
        else
            Console.WriteLine("[hot-reload] no changes detected");

        return (added, removed, updated, routesChanged);
    }

    private List<RoutingRule> ParseRoutes(Dictionary<string, object?> flowDict)
    {
        var rules = new List<RoutingRule>();
        var routeDefs = AsStringDict(flowDict.GetValueOrDefault("routes"));
        if (routeDefs is null) return rules;
        foreach (var (ruleName, rObj) in routeDefs)
        {
            var rDef = AsStringDict(rObj);
            if (rDef is null) continue;
            var dest = GetStr(rDef, "destination");
            var condDict = AsStringDict(rDef.GetValueOrDefault("condition"));
            if (condDict is null) continue;
            var attr = GetStr(condDict, "attribute");
            var op = GetStr(condDict, "operator");
            var val = GetStr(condDict, "value");
            rules.Add(new RoutingRule(ruleName, attr, ParseOperator(op), val, dest));
        }
        return rules;
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

    private static bool RulesEqual(List<RoutingRule> a, List<RoutingRule> b)
    {
        if (a.Count != b.Count) return false;
        for (int i = 0; i < a.Count; i++)
        {
            if (a[i].Name != b[i].Name || a[i].Destination != b[i].Destination || a[i].Enabled != b[i].Enabled)
                return false;
            // Compare conditions
            if (a[i].Condition is BaseRule ab && b[i].Condition is BaseRule bb)
            {
                if (ab.Attribute != bb.Attribute || ab.Operator != bb.Operator || ab.Value != bb.Value)
                    return false;
            }
            else return false;
        }
        return true;
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

    private static Operator ParseOperator(string op) => op.ToUpperInvariant() switch
    {
        "EQ" => Operator.Eq,
        "NEQ" => Operator.Neq,
        "CONTAINS" => Operator.Contains,
        "STARTSWITH" => Operator.StartsWith,
        "ENDSWITH" => Operator.EndsWith,
        "GT" => Operator.Gt,
        "LT" => Operator.Lt,
        _ => Operator.Exists
    };

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
