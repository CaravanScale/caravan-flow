using System.Buffers;
using System.Runtime.CompilerServices;

namespace ZincFlow.Core;

/// <summary>
/// Transactional bounded queue with claim/ack/nack semantics.
/// Hot paths use ArrayPool-backed storage, pooled QueueEntry objects, and numeric IDs.
/// Head-index dequeue with amortized compaction.
/// </summary>
public sealed class FlowQueue
{
    public string Name { get; }
    private readonly int _maxCount;
    private readonly long _maxBytes;
    private readonly long _visibilityTimeoutMs;

    // Visible items — ArrayPool-rented backing array
    private QueueEntry?[] _items;
    private bool _itemsPooled;
    private int _head;
    private int _tail;

    // Claimed items waiting for ack/nack — long key avoids string hashing
    private readonly Dictionary<long, QueueEntry> _invisible;

    private readonly object _lock = new();
    private long _currentBytes;
    private long _idCounter;

    public FlowQueue(string name, int maxCount, long maxBytes, long visibilityTimeoutMs)
    {
        Name = name;
        _maxCount = maxCount;
        _maxBytes = maxBytes;
        _visibilityTimeoutMs = visibilityTimeoutMs;
        int initialSize = Math.Min(maxCount, 1024);
        _items = ArrayPool<QueueEntry?>.Shared.Rent(initialSize);
        _itemsPooled = true;
        _head = 0;
        _tail = 0;
        _invisible = new Dictionary<long, QueueEntry>();
        _currentBytes = 0;
        _idCounter = 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Offer(FlowFile ff)
    {
        int ffBytes = ff.Content.Size;
        lock (_lock)
        {
            int visibleCount = _tail - _head;
            int total = visibleCount + _invisible.Count;
            if (total >= _maxCount)
                return false;
            if (_maxBytes > 0 && _currentBytes + ffBytes > _maxBytes)
                return false;

            var entry = QueueEntry.Rent(++_idCounter, ff, 0, 0, "");

            EnsureCapacity();
            _items[_tail++] = entry;
            _currentBytes += ffBytes;
            return true;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool OfferWithSource(FlowFile ff, string sourceProc)
    {
        int ffBytes = ff.Content.Size;
        lock (_lock)
        {
            int visibleCount = _tail - _head;
            int total = visibleCount + _invisible.Count;
            if (total >= _maxCount)
                return false;
            if (_maxBytes > 0 && _currentBytes + ffBytes > _maxBytes)
                return false;

            var entry = QueueEntry.Rent(++_idCounter, ff, 0, 0, sourceProc);

            EnsureCapacity();
            _items[_tail++] = entry;
            _currentBytes += ffBytes;
            return true;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public QueueEntry? Claim()
    {
        lock (_lock)
        {
            if (_head >= _tail)
                return null;

            var entry = _items[_head]!;
            _items[_head] = null;
            _head++;

            MaybeCompact();

            // Mutate in-place — zero allocation
            entry.ClaimedAt = Environment.TickCount64;
            _invisible[entry.Id] = entry;
            return entry;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Ack(long entryId)
    {
        lock (_lock)
        {
            if (_invisible.Remove(entryId, out var entry))
            {
                _currentBytes -= entry.FlowFile.Content.Size;
                QueueEntry.Return(entry);
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Nack(long entryId)
    {
        lock (_lock)
        {
            if (_invisible.Remove(entryId, out var entry))
            {
                entry.ClaimedAt = 0;
                entry.AttemptCount++;
                EnsureCapacity();
                _items[_tail++] = entry;
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool HasCapacity()
    {
        lock (_lock)
        {
            int total = (_tail - _head) + _invisible.Count;
            return total < _maxCount && (_maxBytes == 0 || _currentBytes < _maxBytes);
        }
    }

    public int VisibleCount
    {
        get { lock (_lock) return _tail - _head; }
    }

    public int InvisibleCount
    {
        get { lock (_lock) return _invisible.Count; }
    }

    public void ReapExpired()
    {
        lock (_lock)
        {
            if (_invisible.Count == 0) return;

            long now = Environment.TickCount64;
            List<long>? expired = null;
            foreach (var (id, entry) in _invisible)
            {
                if (now - entry.ClaimedAt > _visibilityTimeoutMs)
                {
                    expired ??= new List<long>();
                    expired.Add(id);
                }
            }

            if (expired is null) return;
            foreach (var id in expired)
            {
                if (_invisible.Remove(id, out var entry))
                {
                    entry.ClaimedAt = 0;
                    entry.AttemptCount++;
                    EnsureCapacity();
                    _items[_tail++] = entry;
                }
            }
        }
    }

    public void StartReaper(CancellationToken ct = default)
    {
        _ = Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(1000, ct).ConfigureAwait(false);
                ReapExpired();
            }
        }, ct);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void MaybeCompact()
    {
        int liveCount = _tail - _head;
        if (_head > 0 && _head >= liveCount)
        {
            Array.Copy(_items, _head, _items, 0, liveCount);
            Array.Clear(_items, liveCount, _head);
            _tail = liveCount;
            _head = 0;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureCapacity()
    {
        if (_tail >= _items.Length)
        {
            int liveCount = _tail - _head;
            if (_head > _items.Length / 4)
            {
                Array.Copy(_items, _head, _items, 0, liveCount);
                Array.Clear(_items, liveCount, _head);
                _tail = liveCount;
                _head = 0;
            }
            else
            {
                int newSize = Math.Min(_items.Length * 2, _maxCount + 16);
                if (newSize <= _items.Length) newSize = _items.Length + 256;
                var newItems = ArrayPool<QueueEntry?>.Shared.Rent(newSize);
                Array.Copy(_items, _head, newItems, 0, liveCount);

                Array.Clear(_items, 0, _tail);
                if (_itemsPooled)
                    ArrayPool<QueueEntry?>.Shared.Return(_items, clearArray: false);

                _items = newItems;
                _itemsPooled = true;
                _tail = liveCount;
                _head = 0;
            }
        }
    }
}
