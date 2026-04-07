using System.Runtime.CompilerServices;
using ZincFlow.Fabric;

namespace ZincFlow.Core;

/// <summary>
/// Transaction boundary: claim → process → route → ack.
/// All-or-nothing fan-out with backpressure.
/// Returns pooled objects after consumption to minimize GC pressure.
/// </summary>
public sealed class ProcessSession
{
    private readonly FlowQueue _source;
    private readonly IProcessor _processor;
    private readonly string _processorName;
    private readonly RulesEngine _engine;
    private readonly Dictionary<string, FlowQueue> _destQueues;
    private readonly DLQ _dlq;
    private readonly int _maxRetries;
    private readonly int _maxHops;
    private readonly ProvenanceProvider? _provenance;

    // Reusable destination list — never reallocated
    private readonly List<string> _destBuffer = new();

    public ProcessSession(
        FlowQueue source,
        IProcessor processor,
        string processorName,
        RulesEngine engine,
        Dictionary<string, FlowQueue> destQueues,
        DLQ dlq,
        int maxRetries,
        ProvenanceProvider? provenance = null,
        int maxHops = 50)
    {
        _source = source;
        _processor = processor;
        _processorName = processorName;
        _engine = engine;
        _destQueues = destQueues;
        _dlq = dlq;
        _maxRetries = maxRetries;
        _provenance = provenance;
        _maxHops = maxHops;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Execute()
    {
        var entry = _source.Claim();
        if (entry is null)
            return false;

        // Max hop protection — detect routing cycles at runtime
        if (entry.FlowFile.HopCount >= _maxHops)
        {
            _provenance?.Record(entry.FlowFile.NumericId, ProvenanceEventType.DLQ, _processorName, $"max hops exceeded ({_maxHops})");
            _dlq.Add(entry.FlowFile, _processorName, _source.Name, entry.AttemptCount, $"routing cycle detected: {_maxHops} hops exceeded");
            _source.Ack(entry.Id);
            return true;
        }

        // Retry exhaustion → DLQ
        if (entry.AttemptCount >= _maxRetries)
        {
            _provenance?.Record(entry.FlowFile.NumericId, ProvenanceEventType.DLQ, _processorName, "max retries exceeded");
            _dlq.Add(entry.FlowFile, _processorName, _source.Name, entry.AttemptCount, "max retries exceeded");
            _source.Ack(entry.Id);
            return true;
        }

        var result = _processor.Process(entry.FlowFile);

        if (result is SingleResult single)
        {
            var outFf = single.FlowFile;
            _provenance?.Record(outFf.NumericId, ProvenanceEventType.Processed, _processorName);
            SingleResult.Return(single);

            if (RouteResult(outFf, entry))
            {
                // Route succeeded — return the input FlowFile to pool
                var inputFf = entry.FlowFile;
                _source.Ack(entry.Id); // returns QueueEntry to pool
                if (!ReferenceEquals(inputFf, outFf))
                {
                    FlowFile.Return(inputFf);
                    // If no destinations matched, output FF is orphaned — return it too
                    if (_destBuffer.Count == 0)
                        FlowFile.Return(outFf);
                }
            }
            // else: nacked — entry goes back to visible, don't return anything
            return true;
        }
        else if (result is MultipleResult multiple)
        {
            // Route each output FlowFile — all-or-nothing
            bool allRouted = true;
            foreach (var ff in multiple.FlowFiles)
            {
                if (!RouteResult(ff, entry))
                {
                    allRouted = false;
                    break;
                }
            }
            MultipleResult.Return(multiple);
            if (allRouted)
                _source.Ack(entry.Id);
            return true;
        }
        else if (result is RoutedResult routed)
        {
            var routedFf = routed.FlowFile;
            RoutedResult.Return(routed);
            if (RouteResult(routedFf, entry))
                _source.Ack(entry.Id);
            return true;
        }
        else if (result is DroppedResult)
        {
            _provenance?.Record(entry.FlowFile.NumericId, ProvenanceEventType.Dropped, _processorName);
            var inputFf = entry.FlowFile;
            _source.Ack(entry.Id);
            FlowFile.Return(inputFf);
            return true;
        }
        else if (result is FailureResult failure)
        {
            _provenance?.Record(failure.FlowFile.NumericId, ProvenanceEventType.DLQ, _processorName, failure.Reason);
            _dlq.Add(failure.FlowFile, _processorName, _source.Name, entry.AttemptCount, failure.Reason);
            FailureResult.Return(failure);
            _source.Ack(entry.Id);
            return true;
        }

        _source.Ack(entry.Id);
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool RouteResult(FlowFile ff, QueueEntry entry)
    {
        _engine.GetDestinations(ff.Attributes, _destBuffer);

        if (_destBuffer.Count == 0)
            return true;

        // Pre-check all destinations (all-or-nothing)
        foreach (var dest in _destBuffer)
        {
            if (_destQueues.TryGetValue(dest, out var q) && !q.HasCapacity())
            {
                _source.Nack(entry.Id);
                return false;
            }
        }

        // All clear — commit to all destinations
        foreach (var dest in _destBuffer)
        {
            if (_destQueues.TryGetValue(dest, out var q))
            {
                ff.HopCount++;
                _provenance?.Record(ff.NumericId, ProvenanceEventType.Routed, _processorName, dest);
                q.Offer(ff);
            }
        }
        return true;
    }
}
