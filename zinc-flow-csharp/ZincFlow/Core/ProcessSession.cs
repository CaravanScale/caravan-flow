using System.Runtime.CompilerServices;
using ZincFlow.Fabric;

namespace ZincFlow.Core;

/// <summary>
/// Transaction boundary: claim → process → route via connections → ack.
/// All-or-nothing fan-out with backpressure.
/// Connections map relationship names (success, failure, custom) to destination queues.
/// </summary>
public sealed class ProcessSession
{
    private readonly FlowQueue _source;
    private readonly IProcessor _processor;
    private readonly string _processorName;
    private readonly Dictionary<string, List<string>> _connections;
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
        Dictionary<string, List<string>> connections,
        Dictionary<string, FlowQueue> destQueues,
        DLQ dlq,
        int maxRetries,
        ProvenanceProvider? provenance = null,
        int maxHops = 50)
    {
        _source = source;
        _processor = processor;
        _processorName = processorName;
        _connections = connections;
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
                var inputFf = entry.FlowFile;
                _source.Ack(entry.Id);
                if (!ReferenceEquals(inputFf, outFf))
                {
                    FlowFile.Return(inputFf);
                    if (_destBuffer.Count == 0)
                        FlowFile.Return(outFf);
                }
            }
            return true;
        }
        else if (result is MultipleResult multiple)
        {
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
            var relationship = routed.Route;
            RoutedResult.Return(routed);
            if (RouteResult(routedFf, entry, relationship))
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
            if (_connections.ContainsKey("failure"))
            {
                // Route to failure connection targets
                var failFf = failure.FlowFile;
                FailureResult.Return(failure);
                if (RouteResult(failFf, entry, "failure"))
                    _source.Ack(entry.Id);
            }
            else
            {
                // No failure connection — DLQ
                _provenance?.Record(failure.FlowFile.NumericId, ProvenanceEventType.DLQ, _processorName, failure.Reason);
                _dlq.Add(failure.FlowFile, _processorName, _source.Name, entry.AttemptCount, failure.Reason);
                FailureResult.Return(failure);
                _source.Ack(entry.Id);
            }
            return true;
        }

        _source.Ack(entry.Id);
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool RouteResult(FlowFile ff, QueueEntry entry, string relationship = "success")
    {
        _destBuffer.Clear();

        if (_connections.TryGetValue(relationship, out var targets))
        {
            foreach (var t in targets)
                _destBuffer.Add(t);
        }

        if (_destBuffer.Count == 0)
            return true; // No connections = terminal/sink

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
