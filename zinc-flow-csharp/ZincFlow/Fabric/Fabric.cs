using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using ZincFlow.Core;

namespace ZincFlow.Fabric;

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
    // processor names. ConcurrentDictionary because API threads mutate while
    // source threads read during ingest — a plain Dictionary would throw
    // KeyNotFoundException or silently return stale data. The inner map is
    // still a plain Dictionary but we lock on the source-name slot for
    // read-modify-write in AddSourceConnection / RemoveSourceConnection.
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, Dictionary<string, List<string>>> _sourceConnections = new();
    // Source config snapshots so the drawer can display (and eventually
    // edit) what was passed at AddSource time. IConnectorSource doesn't
    // expose its config; we hold the raw map here instead.
    private readonly Dictionary<string, Dictionary<string, string>> _sourceConfigs = new();
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
    // Per-edge processed counter keyed by "from|rel|to". Incremented in
    // PushDownstream. UI polls this to animate edges in proportion to
    // their recent delta throughput (zinc-in-motion).
    private readonly ConcurrentDictionary<string, long> _edgeCounts = new();
    private int _activeExecutions;
    private long _totalProcessed;

    // Per-processor sample ring — a small bounded buffer of recent output
    // previews. Powers the Peek sample tab in the drawer and the field
    // picker every wizard relies on. Default-on in standalone; suppressed
    // by sampling.enabled = false in performance-sensitive deployments.
    private readonly ConcurrentDictionary<string, SampleRing> _samples = new();
    private bool _samplingEnabled = true;
    private const int SamplesPerProcessor = 5;
    private const int SamplePreviewBytes = 4096;

    // Dirty tracking — monotonic counter bumped on every runtime mutation
    // and on successful config.yaml write. UI polls GetDirtyState() to
    // show whether in-memory graph matches what's on disk.
    private long _mutationCounter;
    private long _lastSavedCounter;
    private long _lastSavedTick;

    public void MarkDirty() => Interlocked.Increment(ref _mutationCounter);
    public void MarkSaved()
    {
        _lastSavedCounter = Interlocked.Read(ref _mutationCounter);
        _lastSavedTick = Environment.TickCount64;
    }

    public (bool Dirty, long MutationCounter, long LastSavedCounter, long LastSavedTick) GetDirtyState()
        => (Interlocked.Read(ref _mutationCounter) > Interlocked.Read(ref _lastSavedCounter),
            Interlocked.Read(ref _mutationCounter),
            Interlocked.Read(ref _lastSavedCounter),
            _lastSavedTick);

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
        // (matches zinc-flow-java's separated layout). If that block
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

        // entryPoints is gone. A processor receives data iff a source or
        // another processor wires an outbound connection to it. We still
        // keep an empty list in PipelineGraph so the internal ctor stays
        // compatible; it isn't read by any routing code.
        var effectiveEntries = new List<string>();
        if (flowDict is not null && GetStringList(flowDict, "entryPoints") is { Count: > 0 } legacy)
        {
            Console.WriteLine($"[fabric] config.yaml 'entryPoints: {string.Join(", ", legacy)}' is obsolete — sources now own their outbound connections. Drop the key; connect your source to the target processor instead.");
        }

        // Initialize per-processor counters
        foreach (var name in processorNames)
        {
            _processorCounts.TryAdd(name, 0);
            _processorErrors.TryAdd(name, 0);
        }

        // Build and swap graph
        _graph = new PipelineGraph(processors, connections, effectiveEntries, processorNames, processorStates, processorDefs);
        // LoadFlow reflects disk truth, so the in-memory graph is clean
        // relative to the config file we just parsed.
        MarkSaved();
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

            // Processor lookup. If the target vanished between when we
            // enqueued the work item and now (hot reload removed it),
            // record a FAILED provenance event so operators can see the
            // data loss in /api/provenance/failures rather than only in
            // the log stream. The upstream "from" processor is already
            // gone from context at this point, so we attribute to the
            // missing processor itself.
            if (!graph.Processors.TryGetValue(procName, out var processor))
            {
                _log?.Log("ERROR", "fabric", $"processor '{procName}' removed by reload, dropping ff-{currentFf.NumericId}");
                _provenance?.Record(currentFf.NumericId, ProvenanceEventType.Failed, procName,
                    "target processor removed by reload");
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
                    SampleFromFlowFile(procName, outFf);
                    SingleResult.Return(single);
                    PushDownstream(work, graph, outFf, procName, "success", hops + 1);
                    break;
                }

                case MultipleResult multiple:
                {
                    if (multiple.FlowFiles.Count > 0)
                        SampleFromFlowFile(procName, multiple.FlowFiles[0]);
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
                    SampleFromFlowFile(procName, outFf);
                    RoutedResult.Return(routed);
                    PushDownstream(work, graph, outFf, procName, route, hops + 1);
                    break;
                }

                case MultiRoutedResult multiRouted:
                {
                    if (multiRouted.Outputs.Count > 0)
                        SampleFromFlowFile(procName, multiRouted.Outputs[0].FlowFile);
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
            BumpEdge(fromProcessor, relationship, targets[i]);
        }

        _provenance?.Record(ff.NumericId, ProvenanceEventType.Routed, fromProcessor, targets[0]);
        work.Push((ff, targets[0], hops));
        BumpEdge(fromProcessor, relationship, targets[0]);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void BumpEdge(string from, string rel, string to)
        => _edgeCounts.AddOrUpdate($"{from}|{rel}|{to}", 1, (_, v) => v + 1);

    public Dictionary<string, long> GetEdgeCounts() => new(_edgeCounts);

    // --- Sample ring (Peek) ---

    public void SetSamplingEnabled(bool enabled) => _samplingEnabled = enabled;
    public bool IsSamplingEnabled() => _samplingEnabled;

    private void SampleFromFlowFile(string procName, FlowFile ff)
    {
        if (!_samplingEnabled) return;
        var ring = _samples.GetOrAdd(procName, _ => new SampleRing(SamplesPerProcessor));

        string contentType;
        byte[] preview;
        switch (ff.Content)
        {
            case Raw raw:
                contentType = "bytes";
                var len = Math.Min(raw.Size, SamplePreviewBytes);
                preview = raw.Data.Slice(0, len).ToArray();
                break;
            case RecordContent rc:
                contentType = "records";
                preview = SerializeRecordPreview(rc);
                break;
            case ClaimContent claim:
                // Don't read from disk on the hot path. Operator can open the
                // content store browser if they need the bytes.
                contentType = "claim";
                preview = System.Text.Encoding.UTF8.GetBytes($"(claim {claim.ClaimId}, {claim.Size} bytes)");
                break;
            default:
                contentType = "unknown";
                preview = Array.Empty<byte>();
                break;
        }

        // Snapshot attributes. Flatten the overlay chain so the UI doesn't
        // need to understand AttributeMap internals.
        var attrs = ff.Attributes.ToDictionary();

        ring.Push(new SampleEntry(
            Timestamp: Environment.TickCount64,
            FlowFileId: ff.NumericId,
            ContentType: contentType,
            Preview: preview,
            Attributes: attrs));
    }

    private static byte[] SerializeRecordPreview(RecordContent rc)
    {
        // First record as JSON, capped at SamplePreviewBytes. Cheap enough
        // for a drawer peek; not optimized for throughput-heavy flows
        // which can switch off sampling via SetSamplingEnabled(false).
        if (rc.Records.Count == 0) return "[]"u8.ToArray();
        var first = rc.Records[0].ToDictionary();
        var json = ZincJson.SerializeToString(first);
        var bytes = System.Text.Encoding.UTF8.GetBytes(json);
        return bytes.Length <= SamplePreviewBytes
            ? bytes
            : bytes.AsSpan(0, SamplePreviewBytes).ToArray();
    }

    public List<SampleEntry> GetSamples(string procName)
    {
        if (!_samples.TryGetValue(procName, out var ring)) return new List<SampleEntry>();
        return ring.Snapshot();
    }

    public sealed record SampleEntry(
        long Timestamp,
        long FlowFileId,
        string ContentType,
        byte[] Preview,
        Dictionary<string, string> Attributes);

    /// Small thread-safe ring buffer. Sized once at construction; Push is
    /// O(1) amortized under lock; Snapshot copies oldest-first → newest.
    public sealed class SampleRing
    {
        private readonly SampleEntry?[] _buf;
        private int _head;
        private int _count;
        private readonly object _lock = new();

        public SampleRing(int capacity) { _buf = new SampleEntry?[capacity]; }

        public void Push(SampleEntry entry)
        {
            lock (_lock)
            {
                _buf[_head] = entry;
                _head = (_head + 1) % _buf.Length;
                if (_count < _buf.Length) _count++;
            }
        }

        /// Newest first — that's what the UI wants to render.
        public List<SampleEntry> Snapshot()
        {
            lock (_lock)
            {
                var result = new List<SampleEntry>(_count);
                // Walk backwards from head to produce newest-first order.
                int idx = _head == 0 ? _buf.Length - 1 : _head - 1;
                for (int i = 0; i < _count; i++)
                {
                    if (_buf[idx] is { } e) result.Add(e);
                    idx = idx == 0 ? _buf.Length - 1 : idx - 1;
                }
                return result;
            }
        }
    }

    // --- Ingest callbacks for sources ---

    /// Route a FlowFile out of a named source to its declared outbound
    /// connections. Strict: a source without connections is a no-op — no
    /// hidden entryPoints fallback. The DAG you see on the canvas is the
    /// DAG data flows through.
    public bool IngestFromSource(string sourceName, FlowFile ff)
    {
        // Strict model: a source's FlowFile flows only along its declared
        // outbound connections. No legacy entryPoints fallback — the graph
        // is the program, and a source with no wired edges is a no-op until
        // the operator draws one. Previously we fell back to the graph's
        // entryPoints list, which made FFs appear to flow through
        // processors the operator never connected to the source — visually
        // confusing and semantically hidden.
        if (!_sourceConnections.TryGetValue(sourceName, out var connsByRel))
        {
            FlowFile.Return(ff);
            return false;
        }
        List<string> targets;
        lock (connsByRel)
        {
            if (!connsByRel.TryGetValue("success", out var live) || live.Count == 0)
            {
                FlowFile.Return(ff);
                return false;
            }
            targets = new List<string>(live);
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

    // --- Per-source connection management ---

    public Dictionary<string, Dictionary<string, List<string>>> GetSourceConnections()
    {
        // Snapshot the outer map + inner maps so callers see a stable view.
        var snapshot = new Dictionary<string, Dictionary<string, List<string>>>();
        foreach (var (name, byRel) in _sourceConnections)
        {
            lock (byRel)
            {
                var copy = new Dictionary<string, List<string>>(byRel.Count);
                foreach (var (rel, targets) in byRel) copy[rel] = new List<string>(targets);
                snapshot[name] = copy;
            }
        }
        return snapshot;
    }

    public EditResult AddSourceConnection(string sourceName, string relationship, string target)
    {
        if (string.IsNullOrEmpty(sourceName)) return EditResult.Fail("from must not be blank");
        if (string.IsNullOrEmpty(relationship)) return EditResult.Fail("relationship must not be blank");
        if (string.IsNullOrEmpty(target)) return EditResult.Fail("to must not be blank");
        if (!_sources.ContainsKey(sourceName)) return EditResult.Fail($"source '{sourceName}' not found");
        if (!_graph.Processors.ContainsKey(target)) return EditResult.Fail($"processor '{target}' not found");
        var byRel = _sourceConnections.GetOrAdd(sourceName, _ => new Dictionary<string, List<string>>());
        lock (byRel)
        {
            var targets = byRel.GetValueOrDefault(relationship) ?? new List<string>();
            if (!targets.Contains(target)) targets.Add(target);
            byRel[relationship] = targets;
        }
        MarkDirty();
        return EditResult.Success();
    }

    public EditResult RemoveSourceConnection(string sourceName, string relationship, string target)
    {
        if (!_sourceConnections.TryGetValue(sourceName, out var byRel))
            return EditResult.Fail($"source '{sourceName}' has no connections");
        lock (byRel)
        {
            if (!byRel.TryGetValue(relationship, out var targets))
                return EditResult.Fail($"no '{relationship}' connection");
            if (targets.Remove(target))
            {
                MarkDirty();
                return EditResult.Success();
            }
            return EditResult.Fail($"'{target}' not in '{relationship}' connections");
        }
    }

    public void SetSourceConnections(string sourceName, Dictionary<string, List<string>> connections)
    {
        _sourceConnections[sourceName] = connections;
        MarkDirty();
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

        // Supervisor: every 5s check for dead sources (e.g. ListenHTTP bind
        // failure, HttpListener dispose cascade, unrecoverable poll) and
        // restart them with exponential backoff per source. Runs for the
        // lifetime of the fabric via _cts.
        _ = Task.Run(() => SourceSupervisorLoop(_cts.Token));

        _log?.Log("INFO", "fabric", $"started: {_graph.ProcessorNames.Count} processors, {_sources.Count} sources");
        Console.WriteLine($"[fabric] started ({_graph.ProcessorNames.Count} processors, {_sources.Count} sources)");
    }

    private readonly ConcurrentDictionary<string, int> _sourceRestartBackoffMs = new();
    private readonly ConcurrentDictionary<string, long> _sourceRestartCounts = new();

    public long GetSourceRestartCount(string name) => _sourceRestartCounts.GetValueOrDefault(name);

    private async Task SourceSupervisorLoop(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested && _running)
        {
            try { await Task.Delay(5_000, ct); }
            catch (OperationCanceledException) { break; }

            foreach (var (name, source) in _sources)
            {
                if (source.IsRunning)
                {
                    _sourceRestartBackoffMs[name] = 0;
                    continue;
                }
                var backoff = _sourceRestartBackoffMs.GetValueOrDefault(name, 0);
                if (backoff > 0)
                {
                    // Wait one tick; supervisor runs every 5s, that's the
                    // minimum restart cadence. We scale backoff upward from
                    // consecutive failures.
                    _sourceRestartBackoffMs[name] = Math.Max(0, backoff - 5_000);
                    continue;
                }
                try
                {
                    var sourceName = name;
                    source.Start(ff => IngestFromSource(sourceName, ff), _cts.Token);
                    if (source.IsRunning)
                    {
                        _sourceRestartCounts.AddOrUpdate(name, 1, (_, v) => v + 1);
                        _log?.Log("INFO", "fabric", $"source '{name}' restarted by supervisor");
                        _sourceRestartBackoffMs[name] = 0;
                    }
                    else
                    {
                        var next = _sourceRestartBackoffMs.GetValueOrDefault(name, 0);
                        _sourceRestartBackoffMs[name] = next == 0 ? 5_000 : Math.Min(next * 2, 60_000);
                    }
                }
                catch (Exception ex)
                {
                    _log?.Log("ERROR", "fabric", $"source '{name}' restart threw: {ex.Message}");
                    var next = _sourceRestartBackoffMs.GetValueOrDefault(name, 0);
                    _sourceRestartBackoffMs[name] = next == 0 ? 5_000 : Math.Min(next * 2, 60_000);
                }
            }
        }
    }

    public void StopAsync()
    {
        _running = false;
        foreach (var source in _sources.Values)
            source.Stop();
        _cts.Cancel();
    }

    /// Stop only the connector sources so no new FlowFiles enter the
    /// pipeline, while leaving the execution loop running to drain the
    /// in-flight work. Used by Program.cs's graceful-shutdown phase
    /// before the full StopAsync().
    public void StopSources()
    {
        foreach (var source in _sources.Values)
        {
            try { source.Stop(); } catch { /* tolerate broken source on shutdown */ }
        }
    }

    public int GetActiveExecutions() => Volatile.Read(ref _activeExecutions);

    // --- Connector source management ---

    public void AddSource(IConnectorSource source)
    {
        AddSource(source, new Dictionary<string, string>());
    }

    /// Overload that also stores the source's configuration map so the UI
    /// can render it on the drawer's config tab. Config parity with
    /// processors — without this, a dropped source is a black box.
    public void AddSource(IConnectorSource source, Dictionary<string, string> config)
    {
        _sources[source.Name] = source;
        _sourceConfigs[source.Name] = new Dictionary<string, string>(config);
        if (_running)
        {
            var sourceName = source.Name;
            source.Start(ff => IngestFromSource(sourceName, ff), _cts.Token);
        }
        MarkDirty();
    }

    public Dictionary<string, string>? GetSourceConfig(string name)
        => _sourceConfigs.TryGetValue(name, out var c) ? new Dictionary<string, string>(c) : null;

    /// Remove a source from the fabric — stops it first so no lingering
    /// inflight ingests, then drops its config + connections. Returns
    /// false when the source isn't registered.
    public bool RemoveSource(string name)
    {
        if (!_sources.TryGetValue(name, out var source)) return false;
        try { source.Stop(); } catch { /* best-effort */ }
        _sources.Remove(name);
        _sourceConfigs.Remove(name);
        _sourceConnections.TryRemove(name, out _);
        MarkDirty();
        return true;
    }

    /// Replace a running source's config without losing its outbound
    /// connections. Caller supplies the freshly-constructed source (the
    /// registry factory lives at the API layer); we stop the old instance,
    /// swap it in, stash the new config, and restart if the fabric is
    /// running. Returns false when the name isn't registered.
    public bool ReplaceSource(IConnectorSource source, Dictionary<string, string> config)
    {
        var name = source.Name;
        if (!_sources.TryGetValue(name, out var old)) return false;
        try { old.Stop(); } catch { /* best-effort */ }
        _sources[name] = source;
        _sourceConfigs[name] = new Dictionary<string, string>(config);
        if (_running)
        {
            var sourceName = name;
            source.Start(ff => IngestFromSource(sourceName, ff), _cts.Token);
        }
        MarkDirty();
        return true;
    }

    public bool StartSource(string name)
    {
        if (!_sources.TryGetValue(name, out var source)) return false;
        if (!source.IsRunning)
        {
            var sourceName = name;
            source.Start(ff => IngestFromSource(sourceName, ff), _cts.Token);
        }
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
    // EntryPoints removed from the public surface. Sources own their own
    // outbound connections now; processors with no inbound connections are
    // implicit entries only if a source wires to them.
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
            ["processors"] = processors,
        };
        if (connections.Count > 0) flow["connections"] = connections;

        // Top-level sources: map of name → { type, config?, connections? }.
        // Sources are first-class graph nodes — the canvas IS the
        // program, so they must round-trip through YAML. They live at
        // the config root (matching Program.cs's load path), not under
        // flow.*.
        var sources = new Dictionary<string, object?>();
        foreach (var (sname, src) in _sources)
        {
            var srcEntry = new Dictionary<string, object?> { ["type"] = src.SourceType };
            if (_sourceConfigs.TryGetValue(sname, out var scfg) && scfg.Count > 0)
            {
                var cfgCopy = new Dictionary<string, object?>(scfg.Count);
                foreach (var (k, v) in scfg) cfgCopy[k] = v;
                srcEntry["config"] = cfgCopy;
            }
            if (_sourceConnections.TryGetValue(sname, out var srels))
            {
                lock (srels)
                {
                    var relMap = new Dictionary<string, object?>();
                    foreach (var (rel, targets) in srels)
                        if (targets.Count > 0) relMap[rel] = new List<string>(targets);
                    if (relMap.Count > 0) srcEntry["connections"] = relMap;
                }
            }
            sources[sname] = srcEntry;
        }

        var root = new Dictionary<string, object?> { ["flow"] = flow };
        if (sources.Count > 0) root["sources"] = sources;
        return root;
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
        MarkDirty();
        return true;
    }

    public bool DisableProcessor(string name)
    {
        var graph = _graph;
        if (!graph.Processors.ContainsKey(name)) return false;
        graph.ProcessorStates[name] = ComponentState.Disabled;
        MarkDirty();
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
        return TryAddProcessor(name, typeName, config, requires, connections, out _);
    }

    /// Same as AddProcessor but surfaces factory-exception messages to
    /// the caller. Palette drops route through here so the UI can turn
    /// "Access to path denied" into a usable toast instead of a bare 500.
    public bool TryAddProcessor(string name, string typeName, Dictionary<string, string> config,
        List<string>? requires, Dictionary<string, List<string>>? connections, out string? error)
    {
        error = null;
        var graph = _graph;
        if (graph.Processors.ContainsKey(name))
        {
            error = $"processor '{name}' already exists";
            return false;
        }
        if (!_reg.Has(typeName))
        {
            error = $"unknown processor type '{typeName}'";
            return false;
        }
        requires ??= [];
        connections ??= new();

        foreach (var pn in requires)
            _globalCtx.RegisterDependent(pn, name);

        var ctx = BuildScopedContext(requires);
        IProcessor proc;
        try { proc = _reg.Create(typeName, ctx, config); }
        catch (Exception ex)
        {
            error = $"factory failed: {ex.Message}";
            return false;
        }

        // Build new graph with the additional processor
        var newProcessors = new Dictionary<string, IProcessor>(graph.Processors) { [name] = proc };
        var newConnections = new Dictionary<string, Dictionary<string, List<string>>>(graph.Connections) { [name] = connections };
        var newNames = new List<string>(graph.ProcessorNames) { name };
        var newStates = new Dictionary<string, ComponentState>(graph.ProcessorStates) { [name] = ComponentState.Enabled };
        var newDefs = new Dictionary<string, (string, Dictionary<string, string>, List<string>)>(graph.ProcessorDefs)
            { [name] = (typeName, new Dictionary<string, string>(config), new List<string>(requires)) };

        // EntryPoints: empty list. Explicit sources wire to named processors.
        _graph = new PipelineGraph(newProcessors, newConnections, new List<string>(), newNames, newStates, newDefs);

        _processorCounts.TryAdd(name, 0);
        _processorErrors.TryAdd(name, 0);
        MarkDirty();
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

        _graph = new PipelineGraph(newProcessors, newConnections, new List<string>(), newNames, newStates, newDefs);
        MarkDirty();
        return true;
    }

    /// <summary>
    /// Result of a graph-edit call. <c>Ok == false</c> means the edit
    /// was rejected (unknown processor, duplicate edge, etc.);
    /// <c>Reason</c> carries a short human-readable explanation the
    /// admin API echoes to the operator. Mirrors zinc-flow-java's
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

        // Sources own entry into the graph; no entry-points bookkeeping.
        _graph = new PipelineGraph(graph.Processors, newConns, new List<string>(),
            graph.ProcessorNames, graph.ProcessorStates, graph.ProcessorDefs);
        MarkDirty();
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

        // Sources own entry into the graph; no entry-points bookkeeping.
        _graph = new PipelineGraph(graph.Processors, newConns, new List<string>(),
            graph.ProcessorNames, graph.ProcessorStates, graph.ProcessorDefs);
        MarkDirty();
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

        // Sources own entry into the graph; no entry-points bookkeeping.
        _graph = new PipelineGraph(graph.Processors, newConns, new List<string>(),
            graph.ProcessorNames, graph.ProcessorStates, graph.ProcessorDefs);
        MarkDirty();
        return EditResult.Success();
    }

    // SetEntryPoints removed — sources on the graph are the only entry.

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
        _graph = new PipelineGraph(newProcessors, graph.Connections, new List<string>(),
            graph.ProcessorNames, graph.ProcessorStates, newDefs);
        MarkDirty();
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

        DagValidator.Validate(newConnections);

        if (flowDict is not null && GetStringList(flowDict, "entryPoints") is { Count: > 0 } legacyEntries)
            Console.WriteLine($"[hot-reload] ignoring obsolete entryPoints: [{string.Join(", ", legacyEntries)}]");

        // Atomic swap
        _graph = new PipelineGraph(newProcessors, newConnections, new List<string>(), newNames, newStates, newProcessorDefs);

        var total = added + removed + updated + connectionsChanged;
        if (total > 0)
            Console.WriteLine($"[hot-reload] applied: +{added} -{removed} ~{updated} processors, {connectionsChanged} connections");
        else
            Console.WriteLine("[hot-reload] no changes detected");

        // Hot reload syncs the in-memory graph to disk truth; clean.
        MarkSaved();
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
