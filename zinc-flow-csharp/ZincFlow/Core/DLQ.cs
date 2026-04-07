namespace ZincFlow.Core;

public sealed class DLQ
{
    private readonly Dictionary<string, DLQEntry> _entries = new();
    private readonly object _lock = new();
    private long _idCounter;

    public void Add(FlowFile ff, string sourceProc, string sourceQueue, int attempts, string error)
    {
        lock (_lock)
        {
            var id = $"dlq-{++_idCounter}";
            var entry = new DLQEntry(id, ff, sourceProc, sourceQueue, attempts, error, Environment.TickCount64);
            _entries[id] = entry;
        }
    }

    public DLQEntry? Get(string entryId)
    {
        lock (_lock)
        {
            return _entries.GetValueOrDefault(entryId);
        }
    }

    public List<DLQEntry> ListEntries()
    {
        lock (_lock)
        {
            return new List<DLQEntry>(_entries.Values);
        }
    }

    public void Remove(string entryId)
    {
        lock (_lock) { _entries.Remove(entryId); }
    }

    public FlowFile? Replay(string entryId)
    {
        lock (_lock)
        {
            if (_entries.Remove(entryId, out var entry))
                return entry.FlowFile;
            return null;
        }
    }

    public int Count
    {
        get { lock (_lock) return _entries.Count; }
    }
}
