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
    private readonly FlowQueue _ingestQueue;
    private readonly DLQ _dlq = new();
    private readonly Dictionary<string, IConnectorSource> _sources = new();
    private readonly CancellationTokenSource _cts = new();

    private long _processedCount;
    private volatile bool _running;

    // Defaults (overridable from config)
    private int _queueMaxCount = 10_000;
    private long _queueMaxBytes = 100 * 1024 * 1024;
    private long _visibilityTimeoutMs = 30_000;
    private int _drainTimeoutSeconds = 60;
    private int _maxRetries = 5;

    public Fabric(Registry reg, ProcessorContext globalCtx)
    {
        _reg = reg;
        _globalCtx = globalCtx;
        _ingestQueue = new FlowQueue("ingest", _queueMaxCount, _queueMaxBytes, _visibilityTimeoutMs);
    }

    // --- Config loading ---

    public void LoadFlow(Dictionary<string, object?> config)
    {
        // Read defaults
        if (TryGetConfig<int>(config, "defaults.backpressure.max_count", out var mc)) _queueMaxCount = mc;
        if (TryGetConfig<int>(config, "defaults.backpressure.max_retries", out var mr)) _maxRetries = mr;
        if (TryGetConfig<int>(config, "defaults.backpressure.drain_timeout", out var dt)) _drainTimeoutSeconds = dt;

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

                    var queue = new FlowQueue(name, _queueMaxCount, _queueMaxBytes, _visibilityTimeoutMs);
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
            var session = new ProcessSession(_queues[name], _procs[name], name, _engine, _queues, _dlq, _maxRetries);
            _sessions[name] = session;
        }
    }

    // --- Async start/stop ---

    public void StartAsync()
    {
        _running = true;
        _ingestQueue.StartReaper(_cts.Token);

        // Processor loops
        foreach (var name in _processorNames)
        {
            _queues[name].StartReaper(_cts.Token);
            var procName = name;
            _ = Task.Run(() => ProcessorLoop(procName), _cts.Token);
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

        Console.WriteLine($"[fabric] started ({_processorNames.Count} processors, {_sources.Count} sources)");
    }

    private void ProcessorLoop(string procName)
    {
        while (_running)
        {
            if (_processorStates.GetValueOrDefault(procName) != ComponentState.Enabled)
            {
                Thread.Sleep(100);
                continue;
            }
            if (_sessions[procName].Execute())
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
                var ff = entry.FlowFile;
                _ingestQueue.Ack(entry.Id); // returns QueueEntry to pool
                FlowFile.Return(ff);        // return FlowFile to pool (no destinations)
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

    public bool AddProcessor(string name, string typeName, Dictionary<string, string> config)
    {
        if (_procs.ContainsKey(name) || !_reg.Has(typeName)) return false;
        var ctx = BuildScopedContext([]);
        var proc = _reg.Create(typeName, ctx, config);
        _procs[name] = proc;
        _processorNames.Add(name);
        _processorStates[name] = ComponentState.Enabled;
        _processorRequires[name] = [];

        var queue = new FlowQueue(name, _queueMaxCount, _queueMaxBytes, _visibilityTimeoutMs);
        _queues[name] = queue;
        var session = new ProcessSession(queue, proc, name, _engine, _queues, _dlq, _maxRetries);
        _sessions[name] = session;

        if (_running)
        {
            queue.StartReaper(_cts.Token);
            _ = Task.Run(() => ProcessorLoop(name), _cts.Token);
        }
        return true;
    }

    public bool RemoveProcessor(string name)
    {
        if (!_procs.ContainsKey(name)) return false;
        _processorStates[name] = ComponentState.Disabled;
        _procs.Remove(name);
        _processorNames.Remove(name);
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
