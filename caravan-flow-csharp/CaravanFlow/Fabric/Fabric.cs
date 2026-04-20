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
    // Per-source outbound connections: sourceName → relationship → target
    // processor names. Sources are first-class graph nodes in the tighter
    // visual-FP model — a source's FlowFile flows to its declared
    // downstreams via the regular connection mechanism, not via a shared
    // entryPoints fan-out. When a source has no connections declared,
    // the fabric falls back to the legacy entryPoints list for back-compat.
    private readonly Dictionary<string, Dictionary<string, List<string>>> _sourceConnections = new();
    private readonly CancellationTokenSource _cts = new();
    // Replaceable in LoadFlow when defaults.maxConcurrentExecutions is set.
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
        if (TryGetConfig<int>(config, "defaults.maxHops", out var mh)) _maxHops = mh;
        if (TryGetConfig<int>(config, "defaults.maxConcurrentExecutions", out var mce)
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

        // Read connections — canonical shape is sibling flow.connections:
        // (matches caravan-flow-java's separated layout). If that block
        // isn't present, fall back to per-processor connections: inlined
        // in each processor's dict — the original C# shape. All example
        // YAMLs + docs use the separated shape now; the inlined path
        // stays to keep existing in-memory test fixtures working during
        // migration and will be removed once tests are ported.
        if (flowDict is not null)
        {
            var flowConns = ParseFlowConnections(flowDict);
            if (flowConns.Count > 0)
            {
                foreach (var (from, _) in flowConns)
                    if (!processors.ContainsKey(from))
                        loadErrors.Add(new ConfigException(from, "connections source refers to unknown processor"));
                foreach (var (_, rels) in flowConns)
                    foreach (var (_, dests) in rels)
                        foreach (var d in dests)
                            if (!processors.ContainsKey(d) && !loadErrors.Any(e => e.Message.Contains(d)))
                                loadErrors.Add(new ConfigException($"connection target '{d}' is not a defined processor"));
                connections = flowConns;
            }
            else
            {
                // Legacy per-processor-inlined connections path.
                var procDefsLegacy = AsStringDict(flowDict.GetValueOrDefault("processors"));
                if (procDefsLegacy is not null)
                {
                    foreach (var (name, defObj) in procDefsLegacy)
                    {
                        var def = AsStringDict(defObj);
                        if (def is null) continue;
                        var inline = ParseInlineConnections(def);
                        if (inline.Count > 0) connections[name] = inline;
                    }
                }
            }
        }

        // Pre-populate empty-connection entries for every declared
        // processor so DagValidator sees the full processor set (it
        // derives its allProcessors from the connections map keys).
        foreach (var name in processorNames)
            if (!connections.ContainsKey(name))
                connections[name] = new Dictionary<string, List<string>>();

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

        // Explicit entryPoints override inferred. Flag divergence so
        // operators catch the common "I thought X was an entry but it's
        // targeted from Y" bug. Explicit wins at runtime — that's the
        // whole point of declaring intent.
        var explicitEntries = flowDict is not null ? GetStringList(flowDict, "entryPoints") : null;
        var effectiveEntries = (explicitEntries is not null && explicitEntries.Count > 0)
            ? explicitEntries
            : dagResult.EntryPoints;

        if (explicitEntries is not null && explicitEntries.Count > 0)
        {
            var inferredSet = new HashSet<string>(dagResult.EntryPoints);
            var explicitSet = new HashSet<string>(explicitEntries);
            if (!inferredSet.SetEquals(explicitSet))
            {
                var explicitOnly = explicitSet.Except(inferredSet).ToList();
                var inferredOnly = inferredSet.Except(explicitSet).ToList();
                if (explicitOnly.Count > 0)
                    Console.WriteLine($"[fabric] entry-point warning: explicit-only [{string.Join(", ", explicitOnly)}] — these are declared entry points but also have inbound connections");
                if (inferredOnly.Count > 0)
                    Console.WriteLine($"[fabric] entry-point warning: inferred-only [{string.Join(", ", inferredOnly)}] — these have no inbound connections but aren't in entryPoints: (add them or connect them)");
            }
        }

        Console.WriteLine($"[fabric] entry points: [{string.Join(", ", effectiveEntries)}]");

        // Initialize per-processor counters
        foreach (var name in processorNames)
        {
            _processorCounts.TryAdd(name, 0);
            _processorErrors.TryAdd(name, 0);
        }

        // Build and swap graph
        _graph = new PipelineGraph(processors, connections, effectiveEntries, processorNames, processorStates, processorDefs);
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

                case MultiRoutedResult multiRouted:
                {
                    foreach (var (route, outFf) in multiRouted.Outputs)
                        PushDownstream(work, graph, outFf, procName, route, hops + 1);
                    MultiRoutedResult.Return(multiRouted);
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

    // --- Ingest callbacks for sources ---

    /// <summary>
    /// Legacy callback used by sources that have no outbound connections
    /// configured. Fans a FlowFile out to the graph's entryPoints list —
    /// the pre-tightening model. Kept so existing config.yaml files with
    /// entryPoints but no source-connections continue to work.
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

        for (int i = 1; i < entryPoints.Count; i++)
        {
            ff.Content.AddRef();
            var clone = FlowFile.Rent(ff.NumericId, ff.Attributes, ff.Content, ff.Timestamp, ff.HopCount);
            Execute(clone, entryPoints[i]);
        }
        return Execute(ff, entryPoints[0]);
    }

    /// <summary>
    /// Route a FlowFile out of a named source to its declared outbound
    /// connections. Preferred over <see cref="IngestAndExecute"/> — uses
    /// the source's own connections instead of a shared entryPoints list,
    /// so each source independently decides where its output goes. If
    /// the source has no connections declared, falls back to entryPoints
    /// so legacy configs keep working.
    /// </summary>
    public bool IngestFromSource(string sourceName, FlowFile ff)
    {
        if (_sourceConnections.TryGetValue(sourceName, out var connsByRel) && connsByRel.Count > 0)
        {
            // "success" is the default output relationship for sources.
            var targets = connsByRel.GetValueOrDefault("success", new List<string>());
            if (targets.Count == 0)
            {
                _log?.Log("WARN", "fabric", $"source '{sourceName}' has connections but none on 'success' — dropping ff-{ff.NumericId}");
                FlowFile.Return(ff);
                return false;
            }
            if (targets.Count == 1) return Execute(ff, targets[0]);
            for (int i = 1; i < targets.Count; i++)
            {
                ff.Content.AddRef();
                var clone = FlowFile.Rent(ff.NumericId, ff.Attributes, ff.Content, ff.Timestamp, ff.HopCount);
                Execute(clone, targets[i]);
            }
            return Execute(ff, targets[0]);
        }
        return IngestAndExecute(ff);
    }

    // --- Per-source connection management ---

    public Dictionary<string, Dictionary<string, List<string>>> GetSourceConnections()
        => new(_sourceConnections);

    public EditResult AddSourceConnection(string sourceName, string relationship, string target)
    {
        if (string.IsNullOrEmpty(sourceName)) return EditResult.Fail("from must not be blank");
        if (string.IsNullOrEmpty(relationship)) return EditResult.Fail("relationship must not be blank");
        if (string.IsNullOrEmpty(target)) return EditResult.Fail("to must not be blank");
        if (!_sources.ContainsKey(sourceName)) return EditResult.Fail($"source '{sourceName}' not found");
        if (!_graph.Processors.ContainsKey(target)) return EditResult.Fail($"processor '{target}' not found");
        var byRel = _sourceConnections.GetValueOrDefault(sourceName) ?? new Dictionary<string, List<string>>();
        var targets = byRel.GetValueOrDefault(relationship) ?? new List<string>();
        if (!targets.Contains(target)) targets.Add(target);
        byRel[relationship] = targets;
        _sourceConnections[sourceName] = byRel;
        return EditResult.Success();
    }

    public EditResult RemoveSourceConnection(string sourceName, string relationship, string target)
    {
        if (!_sourceConnections.TryGetValue(sourceName, out var byRel))
            return EditResult.Fail($"source '{sourceName}' has no connections");
        if (!byRel.TryGetValue(relationship, out var targets))
            return EditResult.Fail($"no '{relationship}' connection");
        return targets.Remove(target)
            ? EditResult.Success()
            : EditResult.Fail($"'{target}' not in '{relationship}' connections");
    }

    public void SetSourceConnections(string sourceName, Dictionary<string, List<string>> connections)
    {
        _sourceConnections[sourceName] = connections;
    }

    // --- Async start/stop ---

    public void StartAsync()
    {
        _running = true;

        // Each source gets a per-source ingest callback that routes via
        // that source's declared connections (with entryPoints fallback).
        // This keeps source→processor flow explicit in the graph.
        foreach (var (name, source) in _sources)
        {
            if (!source.IsRunning)
            {
                var sourceName = name;
                source.Start(ff => IngestFromSource(sourceName, ff), _cts.Token);
            }
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
        {
            var sourceName = source.Name;
            source.Start(ff => IngestFromSource(sourceName, ff), _cts.Token);
        }
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

    /// <summary>
    /// Build a nested Dictionary config tree from the runtime graph,
    /// shaped exactly as the canonical separated YAML layout:
    ///
    /// <code>
    /// flow:
    ///   entryPoints: [...]
    ///   processors:
    ///     name: { type, config, requires }
    ///   connections:
    ///     from: { rel: [targets] }
    /// </code>
    ///
    /// Used by <c>POST /api/flow/save</c> to serialize the runtime
    /// state back to disk. Insertion order of the outer dict is
    /// deliberate so the YAML emitter produces stable diffs between
    /// saves. Processor names iterate in <c>_graph.ProcessorNames</c>
    /// order (insertion order from load); connection keys iterate in
    /// the same processor order so adjacent processor+connections
    /// entries stay aligned under review.
    /// </summary>
    public Dictionary<string, object?> ExportToConfig()
    {
        // flow.processors: map of name → { type, config?, requires? }
        var processors = new Dictionary<string, object?>();
        foreach (var name in _graph.ProcessorNames)
        {
            if (!_graph.ProcessorDefs.TryGetValue(name, out var def)) continue;
            var procEntry = new Dictionary<string, object?> { ["type"] = def.Type };
            if (def.Config.Count > 0)
            {
                // Preserve insertion order of config keys as given at
                // load time — matches the original YAML's ordering.
                var cfgCopy = new Dictionary<string, object?>(def.Config.Count);
                foreach (var (k, v) in def.Config) cfgCopy[k] = v;
                procEntry["config"] = cfgCopy;
            }
            if (def.Requires.Count > 0)
                procEntry["requires"] = new List<string>(def.Requires);
            processors[name] = procEntry;
        }

        // flow.connections: map of from → { rel: [targets] }. Skip
        // processors with no outbound connections so the emitted YAML
        // doesn't carry empty dicts.
        var connections = new Dictionary<string, object?>();
        foreach (var name in _graph.ProcessorNames)
        {
            if (!_graph.Connections.TryGetValue(name, out var rels) || rels.Count == 0) continue;
            var relMap = new Dictionary<string, object?>();
            foreach (var (rel, targets) in rels)
            {
                if (targets.Count > 0) relMap[rel] = new List<string>(targets);
            }
            if (relMap.Count > 0) connections[name] = relMap;
        }

        var flow = new Dictionary<string, object?>
        {
            ["entryPoints"] = new List<string>(_graph.EntryPoints),
            ["processors"] = processors,
        };
        if (connections.Count > 0) flow["connections"] = connections;

        return new Dictionary<string, object?> { ["flow"] = flow };
    }

    public Dictionary<string, object> GetStats() => new()
    {
        ["processed"] = Interlocked.Read(ref _totalProcessed),
        ["activeExecutions"] = Volatile.Read(ref _activeExecutions),
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

    public bool ResetProcessorStats(string name)
    {
        if (!_graph.Processors.ContainsKey(name)) return false;
        _processorCounts[name] = 0;
        _processorErrors[name] = 0;
        return true;
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

        // Preserve the graph's explicit EntryPoints across mutations —
        // only SetEntryPoints changes them. DAG re-inference here
        // would clobber an operator's explicit entry-points choice
        // every time they added/removed a processor or connection.
        // Add-processor implicitly adds it as a potential entry point
        // only if the current list is empty (load-time inference).
        var newEntries = new List<string>(graph.EntryPoints);
        if (newEntries.Count == 0) newEntries.Add(name);

        _graph = new PipelineGraph(newProcessors, newConnections, newEntries, newNames, newStates, newDefs);

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
        // Also strip any inbound references to the deleted processor.
        foreach (var (from, rels) in newConnections.ToList())
        {
            var trimmed = new Dictionary<string, List<string>>();
            foreach (var (rel, targets) in rels)
            {
                var keep = targets.Where(t => t != name).ToList();
                if (keep.Count > 0) trimmed[rel] = keep;
            }
            if (trimmed.Count > 0) newConnections[from] = trimmed;
            else newConnections[from] = new Dictionary<string, List<string>>();
        }
        var newNames = new List<string>(graph.ProcessorNames);
        newNames.Remove(name);
        var newStates = new Dictionary<string, ComponentState>(graph.ProcessorStates);
        newStates.Remove(name);
        var newDefs = new Dictionary<string, (string, Dictionary<string, string>, List<string>)>(graph.ProcessorDefs);
        newDefs.Remove(name);
        // Filter the deleted name out of explicit entry points; preserve
        // everything else so the operator's intent survives the edit.
        var newEntries = graph.EntryPoints.Where(ep => ep != name).ToList();

        _graph = new PipelineGraph(newProcessors, newConnections, newEntries, newNames, newStates, newDefs);
        return true;
    }

    /// <summary>
    /// Result of a graph-edit call. <c>Ok == false</c> means the edit
    /// was rejected (unknown processor, duplicate edge, etc.);
    /// <c>Reason</c> carries a short human-readable explanation the
    /// admin API echoes to the operator. Mirrors caravan-flow-java's
    /// <c>Pipeline.EditResult</c> so HTTP response shapes match.
    /// </summary>
    public sealed record EditResult(bool Ok, string Reason)
    {
        public static EditResult Success() => new(true, "");
        public static EditResult Fail(string reason) => new(false, reason);
    }

    /// <summary>
    /// Add a single outbound connection. Rejects unknown processors on
    /// either end and duplicate edges. Atomic swap on success.
    /// </summary>
    public EditResult AddConnection(string from, string relationship, string to)
    {
        if (string.IsNullOrEmpty(from)) return EditResult.Fail("from must not be blank");
        if (string.IsNullOrEmpty(relationship)) return EditResult.Fail("relationship must not be blank");
        if (string.IsNullOrEmpty(to)) return EditResult.Fail("to must not be blank");
        var graph = _graph;
        if (!graph.Processors.ContainsKey(from)) return EditResult.Fail($"processor '{from}' not found");
        if (!graph.Processors.ContainsKey(to)) return EditResult.Fail($"processor '{to}' not found");

        var newConns = new Dictionary<string, Dictionary<string, List<string>>>(graph.Connections);
        var rels = newConns.TryGetValue(from, out var existing)
            ? new Dictionary<string, List<string>>(existing)
            : new Dictionary<string, List<string>>();
        var targets = rels.TryGetValue(relationship, out var current)
            ? new List<string>(current)
            : new List<string>();
        if (targets.Contains(to))
            return EditResult.Fail($"connection '{from}:{relationship} → {to}' already exists");
        targets.Add(to);
        rels[relationship] = targets;
        newConns[from] = rels;

        // Connection-level mutations don't touch entry points —
        // preserve whatever's there so explicit operator intent
        // survives downstream edits. DagValidator stays available
        // for warnings elsewhere; not authoritative here.
        _graph = new PipelineGraph(graph.Processors, newConns, graph.EntryPoints,
            graph.ProcessorNames, graph.ProcessorStates, graph.ProcessorDefs);
        return EditResult.Success();
    }

    public EditResult RemoveConnection(string from, string relationship, string to)
    {
        if (string.IsNullOrEmpty(from) || string.IsNullOrEmpty(relationship) || string.IsNullOrEmpty(to))
            return EditResult.Fail("from, relationship, and to must not be blank");
        var graph = _graph;
        if (!graph.Connections.TryGetValue(from, out var rels)
            || !rels.TryGetValue(relationship, out var targets)
            || !targets.Contains(to))
        {
            return EditResult.Fail($"connection '{from}:{relationship} → {to}' not found");
        }

        var newConns = new Dictionary<string, Dictionary<string, List<string>>>(graph.Connections);
        var newRels = new Dictionary<string, List<string>>(rels);
        var newTargets = new List<string>(targets);
        newTargets.Remove(to);
        if (newTargets.Count == 0) newRels.Remove(relationship);
        else newRels[relationship] = newTargets;

        if (newRels.Count == 0) newConns.Remove(from);
        else newConns[from] = newRels;

        // Pre-populate empty entries for every declared processor so
        // DagValidator sees the full set (matches LoadFlow's prep).
        foreach (var name in graph.ProcessorNames)
            if (!newConns.ContainsKey(name))
                newConns[name] = new Dictionary<string, List<string>>();

        // Connection-level mutations don't touch entry points —
        // preserve whatever's there so explicit operator intent
        // survives downstream edits. DagValidator stays available
        // for warnings elsewhere; not authoritative here.
        _graph = new PipelineGraph(graph.Processors, newConns, graph.EntryPoints,
            graph.ProcessorNames, graph.ProcessorStates, graph.ProcessorDefs);
        return EditResult.Success();
    }

    /// <summary>
    /// Replace every outbound connection of a processor in one atomic
    /// swap. An empty <c>rels</c> map clears the processor's outbound
    /// connections entirely.
    /// </summary>
    public EditResult SetConnections(string from, Dictionary<string, List<string>> rels)
    {
        if (string.IsNullOrEmpty(from)) return EditResult.Fail("from must not be blank");
        var graph = _graph;
        if (!graph.Processors.ContainsKey(from)) return EditResult.Fail($"processor '{from}' not found");
        rels ??= new Dictionary<string, List<string>>();
        foreach (var (_, targets) in rels)
        {
            foreach (var t in targets)
            {
                if (!graph.Processors.ContainsKey(t))
                    return EditResult.Fail($"target processor '{t}' not found");
            }
        }

        var newConns = new Dictionary<string, Dictionary<string, List<string>>>(graph.Connections);
        if (rels.Count == 0) newConns.Remove(from);
        else
        {
            var copy = new Dictionary<string, List<string>>();
            foreach (var (rel, targets) in rels) copy[rel] = new List<string>(targets);
            newConns[from] = copy;
        }
        foreach (var name in graph.ProcessorNames)
            if (!newConns.ContainsKey(name))
                newConns[name] = new Dictionary<string, List<string>>();

        // Connection-level mutations don't touch entry points —
        // preserve whatever's there so explicit operator intent
        // survives downstream edits. DagValidator stays available
        // for warnings elsewhere; not authoritative here.
        _graph = new PipelineGraph(graph.Processors, newConns, graph.EntryPoints,
            graph.ProcessorNames, graph.ProcessorStates, graph.ProcessorDefs);
        return EditResult.Success();
    }

    /// <summary>
    /// Replace the set of entry points. Every name must already be a
    /// defined processor. Explicit override of the DAG-inferred set.
    /// </summary>
    public EditResult SetEntryPoints(List<string> names)
    {
        if (names is null) return EditResult.Fail("names must not be null");
        var graph = _graph;
        foreach (var name in names)
        {
            if (!graph.Processors.ContainsKey(name))
                return EditResult.Fail($"processor '{name}' not found");
        }
        _graph = new PipelineGraph(graph.Processors, graph.Connections, new List<string>(names),
            graph.ProcessorNames, graph.ProcessorStates, graph.ProcessorDefs);
        return EditResult.Success();
    }

    /// <summary>
    /// Update a provider's live config. Scope is intentionally narrow —
    /// only providers with genuinely editable runtime state are
    /// supported; others return a rejection pointing the operator at
    /// the edit-YAML-then-reload path. Supported today:
    ///   - <see cref="VersionControlProvider"/> — rebuild with new
    ///     repo/git/remote/branch (credentials live in on-disk git
    ///     config; swapping instances is safe).
    ///   - <see cref="LoggingProvider"/> — toggle JsonOutput via the
    ///     <c>format</c> key (json|text).
    /// Content + schema-registry + provenance providers require graph
    /// downtime to reconfigure so they stay edit-YAML-only.
    /// </summary>
    public EditResult UpdateProviderConfig(string name, Dictionary<string, string> config)
    {
        var provider = _globalCtx.GetProvider(name);
        if (provider is null) return EditResult.Fail($"provider '{name}' not found");
        config ??= new Dictionary<string, string>();

        switch (provider)
        {
            case VersionControlProvider oldVc:
            {
                var repo = config.GetValueOrDefault("repo", oldVc.Repo);
                var git = config.GetValueOrDefault("git", oldVc.GitBinary);
                var remote = config.GetValueOrDefault("remote", oldVc.Remote);
                var branch = config.GetValueOrDefault("branch", oldVc.Branch);
                var newVc = new VersionControlProvider(repo, git, remote, branch);
                if (oldVc.IsEnabled) newVc.Enable();
                _globalCtx.AddProvider(newVc);
                return EditResult.Success();
            }
            case LoggingProvider logging:
            {
                if (config.TryGetValue("format", out var fmt))
                {
                    logging.JsonOutput = string.Equals(fmt, "json", StringComparison.OrdinalIgnoreCase);
                }
                return EditResult.Success();
            }
            default:
                return EditResult.Fail(
                    $"provider '{name}' ({provider.ProviderType}) does not support runtime config edits — edit config.yaml and POST /api/reload");
        }
    }

    /// <summary>
    /// Rebuild a running processor with a new config. Keeps its
    /// connections, name, and lifecycle state; replaces the instance
    /// atomically. <paramref name="typeName"/> may be null to reuse
    /// the processor's current type.
    /// </summary>
    public EditResult UpdateProcessorConfig(string name, string? typeName, Dictionary<string, string> config)
    {
        if (string.IsNullOrEmpty(name)) return EditResult.Fail("name must not be blank");
        var graph = _graph;
        if (!graph.Processors.ContainsKey(name)) return EditResult.Fail($"processor '{name}' not found");

        var currentDef = graph.ProcessorDefs.GetValueOrDefault(name);
        var effectiveType = string.IsNullOrEmpty(typeName) ? currentDef.Type : typeName;
        if (string.IsNullOrEmpty(effectiveType) || effectiveType == "unknown")
            return EditResult.Fail($"cannot determine type for '{name}' — pass 'type' explicitly");
        if (!_reg.Has(effectiveType))
            return EditResult.Fail($"unknown processor type '{effectiveType}'");

        config ??= new Dictionary<string, string>();
        var requires = currentDef.Requires ?? new List<string>();
        var ctx = BuildScopedContext(requires);
        IProcessor rebuilt;
        try { rebuilt = _reg.Create(effectiveType, ctx, config); }
        catch (Exception ex) { return EditResult.Fail($"factory failed: {ex.Message}"); }

        var newProcessors = new Dictionary<string, IProcessor>(graph.Processors) { [name] = rebuilt };
        var newDefs = new Dictionary<string, (string, Dictionary<string, string>, List<string>)>(graph.ProcessorDefs)
        {
            [name] = (effectiveType, new Dictionary<string, string>(config), new List<string>(requires))
        };
        _graph = new PipelineGraph(newProcessors, graph.Connections, graph.EntryPoints,
            graph.ProcessorNames, graph.ProcessorStates, newDefs);
        return EditResult.Success();
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
        if (TryGetConfig<int>(config, "defaults.maxHops", out var mh)) _maxHops = mh;

        // Parse new processor defs from config. Connections live in the
        // sibling flow.connections: block (canonical). Fall back to
        // per-processor inlined connections when the sibling block is
        // absent — transition compat for in-memory test fixtures.
        var newDefs = new Dictionary<string, (string Type, Dictionary<string, string> Config, List<string> Requires, Dictionary<string, List<string>> Connections)>();
        var flowDict = AsStringDict(config.GetValueOrDefault("flow"));
        var flowConnsParsed = flowDict is not null
            ? ParseFlowConnections(flowDict)
            : new Dictionary<string, Dictionary<string, List<string>>>();
        bool useFlowConnections = flowConnsParsed.Count > 0;
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
                    var connections = useFlowConnections
                        ? flowConnsParsed.GetValueOrDefault(name) ?? new Dictionary<string, List<string>>()
                        : ParseInlineConnections(def);
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

        // Explicit entryPoints override; fall back to DAG inference.
        var reloadExplicitEntries = flowDict is not null ? GetStringList(flowDict, "entryPoints") : null;
        var reloadEffectiveEntries = (reloadExplicitEntries is not null && reloadExplicitEntries.Count > 0)
            ? reloadExplicitEntries
            : dagResult.EntryPoints;

        // Atomic swap
        _graph = new PipelineGraph(newProcessors, newConnections, reloadEffectiveEntries, newNames, newStates, newProcessorDefs);

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

    /// <summary>
    /// Legacy per-processor inlined-connections parser. Reads the
    /// <c>connections:</c> field directly off a single processor def.
    /// Used only by the in-memory test-fixture compat path; user-facing
    /// YAML uses the sibling <c>flow.connections:</c> block.
    /// </summary>
    internal static Dictionary<string, List<string>> ParseInlineConnections(Dictionary<string, object?> def)
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
                var single = GetStr(connDefs, rel);
                if (!string.IsNullOrEmpty(single))
                    connections[rel] = new List<string> { single };
            }
        }
        return connections;
    }

    /// <summary>
    /// Parse the sibling <c>flow.connections:</c> block into a
    /// {fromProcessor: {relationship: [targets]}} map. Entries are
    /// absent when the processor has no outgoing connections — callers
    /// that need a full map (one entry per defined processor) should
    /// pre-populate empty-connection entries themselves (LoadFlow does
    /// this before handing the map to DagValidator).
    /// </summary>
    internal static Dictionary<string, Dictionary<string, List<string>>> ParseFlowConnections(Dictionary<string, object?> flowDict)
    {
        var result = new Dictionary<string, Dictionary<string, List<string>>>();
        var connsRaw = AsStringDict(flowDict.GetValueOrDefault("connections"));
        if (connsRaw is null) return result;
        foreach (var (fromProc, relObj) in connsRaw)
        {
            var rels = AsStringDict(relObj);
            if (rels is null) continue;
            var perRel = new Dictionary<string, List<string>>();
            foreach (var (rel, _) in rels)
            {
                var dests = GetStringList(rels, rel);
                if (dests is not null && dests.Count > 0)
                    perRel[rel] = dests;
                else
                {
                    var single = GetStr(rels, rel);
                    if (!string.IsNullOrEmpty(single))
                        perRel[rel] = new List<string> { single };
                }
            }
            if (perRel.Count > 0)
                result[fromProc] = perRel;
        }
        return result;
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
