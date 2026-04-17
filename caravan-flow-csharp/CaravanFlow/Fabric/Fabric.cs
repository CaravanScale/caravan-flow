using System.Collections.Concurrent;
using CaravanFlow.Core;

namespace CaravanFlow.Fabric;

/// <summary>
/// Fabric: direct pipeline executor — wires processors into a DAG and executes
/// FlowFiles through it synchronously. No inter-stage queues. Concurrency comes
/// from sources (each source thread runs its own independent graph traversal).
/// </summary>
public sealed class Fabric
{
    private readonly Registry _reg;
    private readonly ProcessorContext _globalCtx;
    private readonly Dictionary<string, IConnectorSource> _sources = new();
    private readonly CancellationTokenSource _cts = new();
    // Replaceable in LoadFlow when defaults.max_concurrent_executions is set.
    // Safe on initial load (no in-flight Execute yet). On hot reload, the swap
    // is best-effort — in-flight Wait()/Release() calls against the old
    // semaphore drain naturally; new requests use the new one.
    private SemaphoreSlim _executionGate;

    // Pipeline graph — swapped atomically on hot reload
    private volatile PipelineGraph _graph = PipelineGraph.Empty;

    // Metrics
    private readonly ConcurrentDictionary<string, long> _processorCounts = new();
    private readonly ConcurrentDictionary<string, long> _processorErrors = new();
    private int _activeExecutions;
    private long _totalProcessed;

    private volatile bool _running;

    // Defaults (overridable from config)
    private int _maxHops = 50;
    private int _maxConcurrentExecutions = 100;
    private ProvenanceProvider? _provenance;
    private LoggingProvider? _log;

    public Fabric(Registry reg, ProcessorContext globalCtx)
    {
        _reg = reg;
        _globalCtx = globalCtx;
        _log = globalCtx.GetProvider("logging") as LoggingProvider;
        _provenance = globalCtx.GetProvider("provenance") as ProvenanceProvider;
        _executionGate = new SemaphoreSlim(_maxConcurrentExecutions, _maxConcurrentExecutions);
    }

    // --- Config loading ---

    public void LoadFlow(Dictionary<string, object?> config)
    {
        // Read defaults
        if (TryGetConfig<int>(config, "defaults.max_hops", out var mh)) _maxHops = mh;
        if (TryGetConfig<int>(config, "defaults.max_concurrent_executions", out var mce)
            && mce != _maxConcurrentExecutions)
        {
            // Replace the gate with one sized to the configured limit. Without
            // this swap, the constructor-default-sized semaphore would silently
            // ignore the config value (caught by SustainedLoadTests backpressure case).
            _maxConcurrentExecutions = mce;
            var oldGate = Interlocked.Exchange(ref _executionGate, new SemaphoreSlim(mce, mce));
            oldGate.Dispose();
        }

        var processors = new Dictionary<string, IProcessor>();
        var connections = new Dictionary<string, Dictionary<string, List<string>>>();
        var processorNames = new List<string>();
        var processorStates = new Dictionary<string, ComponentState>();
        var processorDefs = new Dictionary<string, (string Type, Dictionary<string, string> Config, List<string> Requires)>();

        // Collect every ConfigException across the load pass so the operator
        // sees all config problems at once instead of fixing one, re-running,
        // and hitting the next. Any accumulated entries → AggregateException
        // thrown at the end (empty pipeline never silently loads).
        var loadErrors = new List<ConfigException>();

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
                    if (def is null)
                    {
                        loadErrors.Add(new ConfigException(name, "processor definition missing or not a map"));
                        continue;
                    }

                    var typeName = GetStr(def, "type");
                    if (string.IsNullOrEmpty(typeName))
                    {
                        loadErrors.Add(new ConfigException(name, "missing required key: type"));
                        continue;
                    }
                    if (!_reg.Has(typeName))
                    {
                        loadErrors.Add(new ConfigException(name, $"unknown processor type: {typeName}"));
                        continue;
                    }

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
                    foreach (var pn in requires)
                        _globalCtx.RegisterDependent(pn, name);

                    // Connections
                    var conns = ParseConnections(def);
                    connections[name] = conns;

                    // Factory invocation — any ConfigException raised by the
                    // factory (missing key, unparseable value, unknown op)
                    // gets tagged with the processor name and collected.
                    var ctx = BuildScopedContext(requires);
                    IProcessor proc;
                    try
                    {
                        proc = _reg.Create(typeName, ctx, procConfig);
                    }
                    catch (ConfigException ex)
                    {
                        loadErrors.Add(ex.ComponentName is null
                            ? new ConfigException(name, ex.Message)
                            : ex);
                        continue;
                    }

                    processors[name] = proc;
                    processorNames.Add(name);
                    processorStates[name] = ComponentState.Enabled;
                    processorDefs[name] = (typeName, new Dictionary<string, string>(procConfig), new List<string>(requires));

                    Console.WriteLine($"[fabric] processor created: {name} (type={typeName})");
                }
            }
        }

        // DAG validation. Errors collected into the same aggregate so the
        // operator sees connection problems alongside processor-factory
        // problems in one report. Warnings keep going to stdout — they're
        // not fatal.
        var dagResult = DagValidator.Validate(connections);
        foreach (var err in dagResult.Errors)
            loadErrors.Add(new ConfigException($"DAG: {err}"));
        foreach (var warn in dagResult.Warnings)
            Console.WriteLine($"[fabric] DAG warning: {warn}");

        // Abort the load if anything went wrong. No partial-graph
        // publish — the pipeline either loads cleanly or doesn't load
        // at all, so a typo can never produce a half-wired runtime.
        if (loadErrors.Count > 0)
            throw new AggregateException(
                $"flow load failed with {loadErrors.Count} config error(s)",
                loadErrors);

        Console.WriteLine($"[fabric] entry points: [{string.Join(", ", dagResult.EntryPoints)}]");

        // Initialize per-processor counters
        foreach (var name in processorNames)
        {
            _processorCounts.TryAdd(name, 0);
            _processorErrors.TryAdd(name, 0);
        }

        // Build and swap graph
        _graph = new PipelineGraph(processors, connections, dagResult.EntryPoints, processorNames, processorStates, processorDefs);
    }

    // --- Pipeline execution ---

    /// <summary>
    /// Execute a FlowFile through the pipeline starting at the given entry point.
    /// Returns false if backpressure (semaphore full).
    /// Called from source threads — each call is an independent synchronous traversal.
    /// </summary>
    public bool Execute(FlowFile ff, string entryPoint)
    {
        if (!_executionGate.Wait(0))
            return false; // backpressure

        try
        {
            Interlocked.Increment(ref _activeExecutions);
            ExecuteGraph(ff, entryPoint);
            return true;
        }
        finally
        {
            Interlocked.Decrement(ref _activeExecutions);
            _executionGate.Release();
        }
    }

    /// <summary>
    /// Iterative work-stack graph traversal. Depth-first: each branch completes
    /// before the next starts. No recursion, no stack overflow risk.
    /// </summary>
    private void ExecuteGraph(FlowFile ff, string entryPoint)
    {
        var graph = _graph; // volatile read — stable for this traversal
        var work = new Stack<(FlowFile Ff, string Processor, int Hops)>();
        work.Push((ff, entryPoint, 0));

        while (work.Count > 0)
        {
            var (currentFf, procName, hops) = work.Pop();

            // Hop limit
            if (hops >= _maxHops)
            {
                _log?.Log("ERROR", procName, $"max hops exceeded ({_maxHops}), dropping ff-{currentFf.NumericId}");
                _processorErrors.AddOrUpdate(procName, 1, (_, v) => v + 1);
                FlowFile.Return(currentFf);
                continue;
            }

            // Processor lookup
            if (!graph.Processors.TryGetValue(procName, out var processor))
            {
                _log?.Log("ERROR", "fabric", $"unknown processor '{procName}', dropping ff-{currentFf.NumericId}");
                FlowFile.Return(currentFf);
                continue;
            }

            // Skip disabled processors
            if (graph.ProcessorStates.GetValueOrDefault(procName) != ComponentState.Enabled)
            {
                FlowFile.Return(currentFf);
                continue;
            }

            // Process — catch exceptions to prevent FlowFile leaks
            _provenance?.Record(currentFf.NumericId, ProvenanceEventType.Processed, procName);
            ProcessorResult result;
            try
            {
                result = processor.Process(currentFf);
            }
            catch (Exception ex)
            {
                _log?.Log("ERROR", procName, $"processor threw exception: {ex.Message}, ff-{currentFf.NumericId}");
                _processorErrors.AddOrUpdate(procName, 1, (_, v) => v + 1);
                Interlocked.Increment(ref _totalProcessed);

                // Route to failure connection if available, otherwise drop
                var exConns = graph.Connections.GetValueOrDefault(procName);
                if (exConns is not null && exConns.ContainsKey("failure"))
                {
                    PushDownstream(work, graph, currentFf, procName, "failure", hops + 1);
                }
                else
                {
                    FlowFile.Return(currentFf);
                }
                continue;
            }
            _processorCounts.AddOrUpdate(procName, 1, (_, v) => v + 1);
            Interlocked.Increment(ref _totalProcessed);

            // Route based on result type
            switch (result)
            {
                case SingleResult single:
                {
                    var outFf = single.FlowFile;
                    SingleResult.Return(single);
                    PushDownstream(work, graph, outFf, procName, "success", hops + 1);
                    break;
                }

                case MultipleResult multiple:
                {
                    // Push downstream before returning to pool (Return clears FlowFiles)
                    foreach (var outFf in multiple.FlowFiles)
                        PushDownstream(work, graph, outFf, procName, "success", hops + 1);
                    MultipleResult.Return(multiple);
                    break;
                }

                case RoutedResult routed:
                {
                    var outFf = routed.FlowFile;
                    var route = routed.Route;
                    RoutedResult.Return(routed);
                    PushDownstream(work, graph, outFf, procName, route, hops + 1);
                    break;
                }

                case DroppedResult:
                    _provenance?.Record(currentFf.NumericId, ProvenanceEventType.Dropped, procName);
                    FlowFile.Return(currentFf);
                    break;

                case FailureResult failure:
                {
                    var failFf = failure.FlowFile;
                    var reason = failure.Reason;
                    FailureResult.Return(failure);

                    var conns = graph.Connections.GetValueOrDefault(procName);
                    if (conns is not null && conns.ContainsKey("failure"))
                    {
                        PushDownstream(work, graph, failFf, procName, "failure", hops + 1);
                    }
                    else
                    {
                        _log?.Log("ERROR", procName, $"failure (no handler): {reason}, ff-{failFf.NumericId}");
                        _processorErrors.AddOrUpdate(procName, 1, (_, v) => v + 1);
                        FlowFile.Return(failFf);
                    }
                    break;
                }
            }
        }
    }

    /// <summary>
    /// Push FlowFile to downstream targets on the work stack.
    /// Fan-out: first target gets original, rest get clones.
    /// </summary>
    private void PushDownstream(Stack<(FlowFile, string, int)> work, PipelineGraph graph,
        FlowFile ff, string fromProcessor, string relationship, int hops)
    {
        var conns = graph.Connections.GetValueOrDefault(fromProcessor);
        if (conns is null || !conns.TryGetValue(relationship, out var targets) || targets.Count == 0)
        {
            // No downstream = sink/terminal
            FlowFile.Return(ff);
            return;
        }

        // Push in reverse order so first target pops first (depth-first)
        for (int i = targets.Count - 1; i >= 1; i--)
        {
            ff.Content.AddRef();
            var clone = FlowFile.Rent(ff.NumericId, ff.Attributes, ff.Content, ff.Timestamp, ff.HopCount);
            _provenance?.Record(clone.NumericId, ProvenanceEventType.Routed, fromProcessor, targets[i]);
            work.Push((clone, targets[i], hops));
        }

        _provenance?.Record(ff.NumericId, ProvenanceEventType.Routed, fromProcessor, targets[0]);
        work.Push((ff, targets[0], hops));
    }

    // --- Ingest callback for sources ---

    /// <summary>
    /// Callback passed to connector sources. Fans out to all entry-point processors.
    /// Returns false on backpressure.
    /// </summary>
    public bool IngestAndExecute(FlowFile ff)
    {
        var graph = _graph;
        var entryPoints = graph.EntryPoints;

        if (entryPoints.Count == 0)
        {
            _log?.Log("WARN", "fabric", $"no entry-point processors, dropping ff-{ff.NumericId}");
            FlowFile.Return(ff);
            return false;
        }

        if (entryPoints.Count == 1)
            return Execute(ff, entryPoints[0]);

        // Multiple entry points: clone for each, execute all
        for (int i = 1; i < entryPoints.Count; i++)
        {
            ff.Content.AddRef();
            var clone = FlowFile.Rent(ff.NumericId, ff.Attributes, ff.Content, ff.Timestamp, ff.HopCount);
            Execute(clone, entryPoints[i]);
        }
        return Execute(ff, entryPoints[0]);
    }

    // --- Async start/stop ---

    public void StartAsync()
    {
        _running = true;

        // Start all connector sources — they call IngestAndExecute directly
        foreach (var (name, source) in _sources)
        {
            if (!source.IsRunning)
                source.Start(IngestAndExecute, _cts.Token);
            Console.WriteLine($"[fabric] source started: {name} (type={source.SourceType})");
        }

        _log?.Log("INFO", "fabric", $"started: {_graph.ProcessorNames.Count} processors, {_sources.Count} sources");
        Console.WriteLine($"[fabric] started ({_graph.ProcessorNames.Count} processors, {_sources.Count} sources)");
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
            source.Start(IngestAndExecute, _cts.Token);
    }

    public bool StartSource(string name)
    {
        if (!_sources.TryGetValue(name, out var source)) return false;
        if (!source.IsRunning)
            source.Start(IngestAndExecute, _cts.Token);
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

    // --- Accessors ---

    public ProcessorContext GetContext() => _globalCtx;
    public ProvenanceProvider? GetProvenance() => _provenance;
    public List<string> GetProcessorNames() => new(_graph.ProcessorNames);
    public Registry GetRegistry() => _reg;
    public Dictionary<string, Dictionary<string, List<string>>> GetConnections() => new(_graph.Connections);
    public List<string> GetEntryPoints() => new(_graph.EntryPoints);
    public string GetProcessorType(string name) => _graph.ProcessorDefs.TryGetValue(name, out var def) ? def.Type : "unknown";

    public Dictionary<string, object> GetStats() => new()
    {
        ["processed"] = Interlocked.Read(ref _totalProcessed),
        ["active_executions"] = Volatile.Read(ref _activeExecutions),
        ["processors"] = _graph.ProcessorNames.Count,
        ["sources"] = _sources.Count
    };

    public Dictionary<string, Dictionary<string, object>> GetProcessorStats()
    {
        var stats = new Dictionary<string, Dictionary<string, object>>();
        foreach (var name in _graph.ProcessorNames)
        {
            stats[name] = new Dictionary<string, object>
            {
                ["processed"] = _processorCounts.GetValueOrDefault(name),
                ["errors"] = _processorErrors.GetValueOrDefault(name)
            };
        }
        return stats;
    }

    // --- Processor lifecycle ---

    public ComponentState GetProcessorState(string name)
        => _graph.ProcessorStates.GetValueOrDefault(name, ComponentState.Disabled);

    public bool EnableProcessor(string name)
    {
        var graph = _graph;
        if (!graph.Processors.ContainsKey(name)) return false;
        graph.ProcessorStates[name] = ComponentState.Enabled;
        return true;
    }

    public bool DisableProcessor(string name)
    {
        var graph = _graph;
        if (!graph.Processors.ContainsKey(name)) return false;
        graph.ProcessorStates[name] = ComponentState.Disabled;
        return true;
    }

    // --- Provider lifecycle ---

    public bool EnableProvider(string name)
    {
        var prov = _globalCtx.GetProvider(name);
        if (prov is null) return false;
        prov.Enable();
        return true;
    }

    public bool DisableProvider(string name)
    {
        var prov = _globalCtx.GetProvider(name);
        if (prov is null) return false;

        // Cascade: disable dependent processors
        foreach (var procName in _globalCtx.GetDependents(name))
        {
            if (_graph.ProcessorStates.GetValueOrDefault(procName) == ComponentState.Enabled)
                DisableProcessor(procName);
        }
        prov.Disable(0);
        return true;
    }

    // --- Dynamic processor management ---

    public bool AddProcessor(string name, string typeName, Dictionary<string, string> config,
        List<string>? requires = null, Dictionary<string, List<string>>? connections = null)
    {
        var graph = _graph;
        if (graph.Processors.ContainsKey(name) || !_reg.Has(typeName)) return false;
        requires ??= [];
        connections ??= new();

        foreach (var pn in requires)
            _globalCtx.RegisterDependent(pn, name);

        var ctx = BuildScopedContext(requires);
        var proc = _reg.Create(typeName, ctx, config);

        // Build new graph with the additional processor
        var newProcessors = new Dictionary<string, IProcessor>(graph.Processors) { [name] = proc };
        var newConnections = new Dictionary<string, Dictionary<string, List<string>>>(graph.Connections) { [name] = connections };
        var newNames = new List<string>(graph.ProcessorNames) { name };
        var newStates = new Dictionary<string, ComponentState>(graph.ProcessorStates) { [name] = ComponentState.Enabled };
        var newDefs = new Dictionary<string, (string, Dictionary<string, string>, List<string>)>(graph.ProcessorDefs)
            { [name] = (typeName, new Dictionary<string, string>(config), new List<string>(requires)) };

        var dagResult = DagValidator.Validate(newConnections);
        _graph = new PipelineGraph(newProcessors, newConnections, dagResult.EntryPoints, newNames, newStates, newDefs);

        _processorCounts.TryAdd(name, 0);
        _processorErrors.TryAdd(name, 0);
        return true;
    }

    public bool RemoveProcessor(string name)
    {
        var graph = _graph;
        if (!graph.Processors.ContainsKey(name)) return false;

        var newProcessors = new Dictionary<string, IProcessor>(graph.Processors);
        newProcessors.Remove(name);
        var newConnections = new Dictionary<string, Dictionary<string, List<string>>>(graph.Connections);
        newConnections.Remove(name);
        var newNames = new List<string>(graph.ProcessorNames);
        newNames.Remove(name);
        var newStates = new Dictionary<string, ComponentState>(graph.ProcessorStates);
        newStates.Remove(name);
        var newDefs = new Dictionary<string, (string, Dictionary<string, string>, List<string>)>(graph.ProcessorDefs);
        newDefs.Remove(name);

        var dagResult = DagValidator.Validate(newConnections);
        _graph = new PipelineGraph(newProcessors, newConnections, dagResult.EntryPoints, newNames, newStates, newDefs);
        return true;
    }

    // --- Hot reload ---

    /// <summary>
    /// Reload flow from new config. Diffs processors and connections against current state.
    /// Builds a new PipelineGraph and swaps atomically — in-flight executions complete
    /// on the old graph, new executions use the new graph.
    /// </summary>
    public (int Added, int Removed, int Updated, int ConnectionsChanged) ReloadFlow(Dictionary<string, object?> config)
    {
        int added = 0, removed = 0, updated = 0, connectionsChanged = 0;

        // Read new defaults
        if (TryGetConfig<int>(config, "defaults.max_hops", out var mh)) _maxHops = mh;

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

        var oldGraph = _graph;
        var newProcessors = new Dictionary<string, IProcessor>();
        var newConnections = new Dictionary<string, Dictionary<string, List<string>>>();
        var newNames = new List<string>();
        var newStates = new Dictionary<string, ComponentState>();
        var newProcessorDefs = new Dictionary<string, (string Type, Dictionary<string, string> Config, List<string> Requires)>();

        // Detect removed processors
        foreach (var name in oldGraph.ProcessorDefs.Keys)
        {
            if (!newDefs.ContainsKey(name))
            {
                removed++;
                _log?.Log("INFO", "hot-reload", $"removed processor: {name}");
            }
        }

        // Build new graph — reuse unchanged processor instances
        foreach (var (name, newDef) in newDefs)
        {
            var oldDef = oldGraph.ProcessorDefs.GetValueOrDefault(name);
            bool isNew = oldDef == default;
            bool procChanged = !isNew && (oldDef.Type != newDef.Type || !DictEqual(oldDef.Config, newDef.Config) || !ListEqual(oldDef.Requires, newDef.Requires));
            var oldConn = oldGraph.Connections.GetValueOrDefault(name);
            bool connChanged = !isNew && oldConn is not null && !ConnectionsEqual(oldConn, newDef.Connections);

            if (isNew)
            {
                // New processor
                foreach (var pn in newDef.Requires)
                    _globalCtx.RegisterDependent(pn, name);
                var ctx = BuildScopedContext(newDef.Requires);
                newProcessors[name] = _reg.Create(newDef.Type, ctx, newDef.Config);
                added++;
                _log?.Log("INFO", "hot-reload", $"added processor: {name} (type={newDef.Type})");
            }
            else if (procChanged)
            {
                // Processor type/config changed — rebuild
                var ctx = BuildScopedContext(newDef.Requires);
                newProcessors[name] = _reg.Create(newDef.Type, ctx, newDef.Config);
                updated++;
                _log?.Log("INFO", "hot-reload", $"updated processor: {name} (type={newDef.Type})");
            }
            else
            {
                // Unchanged — reuse existing instance
                newProcessors[name] = oldGraph.Processors[name];
                if (connChanged)
                {
                    connectionsChanged++;
                    _log?.Log("INFO", "hot-reload", $"updated connections: {name}");
                }
            }

            newConnections[name] = newDef.Connections;
            newNames.Add(name);
            newStates[name] = oldGraph.ProcessorStates.GetValueOrDefault(name, ComponentState.Enabled);
            newProcessorDefs[name] = (newDef.Type, new Dictionary<string, string>(newDef.Config), new List<string>(newDef.Requires));

            _processorCounts.TryAdd(name, 0);
            _processorErrors.TryAdd(name, 0);
        }

        var dagResult = DagValidator.Validate(newConnections);

        // Atomic swap
        _graph = new PipelineGraph(newProcessors, newConnections, dagResult.EntryPoints, newNames, newStates, newProcessorDefs);

        var total = added + removed + updated + connectionsChanged;
        if (total > 0)
            Console.WriteLine($"[hot-reload] applied: +{added} -{removed} ~{updated} processors, {connectionsChanged} connections");
        else
            Console.WriteLine("[hot-reload] no changes detected");

        return (added, removed, updated, connectionsChanged);
    }

    public void Status()
    {
        Console.WriteLine($"[fabric] processors={_graph.ProcessorNames.Count} processed={Interlocked.Read(ref _totalProcessed)} active={Volatile.Read(ref _activeExecutions)}");
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

/// <summary>
/// Immutable pipeline graph — processors, connections, entry points.
/// Swapped atomically on hot reload via volatile write.
/// </summary>
internal sealed class PipelineGraph
{
    public readonly Dictionary<string, IProcessor> Processors;
    public readonly Dictionary<string, Dictionary<string, List<string>>> Connections;
    public readonly List<string> EntryPoints;
    public readonly List<string> ProcessorNames;
    public readonly Dictionary<string, ComponentState> ProcessorStates;
    public readonly Dictionary<string, (string Type, Dictionary<string, string> Config, List<string> Requires)> ProcessorDefs;

    public PipelineGraph(
        Dictionary<string, IProcessor> processors,
        Dictionary<string, Dictionary<string, List<string>>> connections,
        List<string> entryPoints,
        List<string> processorNames,
        Dictionary<string, ComponentState> processorStates,
        Dictionary<string, (string Type, Dictionary<string, string> Config, List<string> Requires)> processorDefs)
    {
        Processors = processors;
        Connections = connections;
        EntryPoints = entryPoints;
        ProcessorNames = processorNames;
        ProcessorStates = processorStates;
        ProcessorDefs = processorDefs;
    }

    public static readonly PipelineGraph Empty = new(
        new(), new(), new(), new(), new(), new());
}
