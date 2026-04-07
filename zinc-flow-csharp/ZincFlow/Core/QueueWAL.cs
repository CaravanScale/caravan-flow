using System.Buffers.Binary;

namespace ZincFlow.Core;

/// <summary>
/// Write-ahead log for FlowQueue persistence. Append-only binary file.
/// On startup, replays the WAL to restore queue state. Compacts periodically
/// and enforces a max file size.
///
/// Record format: [type:1][id:8][payloadLen:4][payload:N]
///   type: 0x01=offer, 0x02=ack
///   payload: V3-encoded FlowFile (for offer) or empty (for ack)
/// </summary>
public sealed class QueueWAL : IDisposable
{
    private const byte TypeOffer = 0x01;
    private const byte TypeAck = 0x02;
    private const int HeaderSize = 1 + 8 + 4; // type + id + payloadLen

    private readonly string _path;
    private readonly long _maxSizeBytes;
    private readonly int _compactIntervalMs;
    private FileStream? _stream;
    private readonly object _lock = new();
    private long _fileSize;
    private long _offerCount;
    private long _ackCount;
    private CancellationTokenSource? _compactCts;

    /// <param name="path">WAL file path.</param>
    /// <param name="maxSizeMb">Max WAL file size in MB. 0 = unlimited. When exceeded, compacts immediately.</param>
    /// <param name="compactIntervalMs">Periodic compaction interval. 0 = no periodic compaction.</param>
    public QueueWAL(string path, int maxSizeMb = 100, int compactIntervalMs = 60_000)
    {
        _path = path;
        _maxSizeBytes = maxSizeMb > 0 ? maxSizeMb * 1024L * 1024L : 0;
        _compactIntervalMs = compactIntervalMs;
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir))
            Directory.CreateDirectory(dir);
    }

    public void Open()
    {
        _stream = new FileStream(_path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, 4096, FileOptions.WriteThrough);
        _fileSize = _stream.Length;
        _stream.Seek(0, SeekOrigin.End);
    }

    public void AppendOffer(long entryId, byte[] v3Payload)
    {
        lock (_lock)
        {
            if (_stream is null) return;
            WriteRecord(TypeOffer, entryId, v3Payload);
            _offerCount++;
            CheckSize();
        }
    }

    public void AppendAck(long entryId)
    {
        lock (_lock)
        {
            if (_stream is null) return;
            WriteRecord(TypeAck, entryId, []);
            _ackCount++;
            CheckSize();
        }
    }

    /// <summary>
    /// Replay WAL from beginning. Returns list of FlowFiles that were offered but not acked.
    /// </summary>
    public List<(long Id, byte[] V3Payload)> Replay()
    {
        if (_stream is null) return [];

        var offered = new Dictionary<long, byte[]>();
        _stream.Seek(0, SeekOrigin.Begin);

        var headerBuf = new byte[HeaderSize];
        while (true)
        {
            int read = _stream.Read(headerBuf, 0, HeaderSize);
            if (read < HeaderSize) break;

            byte type = headerBuf[0];
            long id = BinaryPrimitives.ReadInt64LittleEndian(headerBuf.AsSpan(1));
            int payloadLen = BinaryPrimitives.ReadInt32LittleEndian(headerBuf.AsSpan(9));

            byte[] payload = [];
            if (payloadLen > 0)
            {
                payload = new byte[payloadLen];
                int payloadRead = _stream.Read(payload, 0, payloadLen);
                if (payloadRead < payloadLen) break;
            }

            if (type == TypeOffer)
                offered[id] = payload;
            else if (type == TypeAck)
                offered.Remove(id);
        }

        _stream.Seek(0, SeekOrigin.End);
        return offered.Select(kv => (kv.Key, kv.Value)).ToList();
    }

    /// <summary>
    /// Compact: rewrite WAL with only the live entries (offered but not acked).
    /// </summary>
    public void Compact(List<(long Id, byte[] V3Payload)>? liveEntries = null)
    {
        lock (_lock)
        {
            // If no entries provided, replay to find them
            if (liveEntries is null)
            {
                _stream?.Seek(0, SeekOrigin.Begin);
                var offered = new Dictionary<long, byte[]>();
                var headerBuf = new byte[HeaderSize];
                while (_stream is not null)
                {
                    int read = _stream.Read(headerBuf, 0, HeaderSize);
                    if (read < HeaderSize) break;
                    byte type = headerBuf[0];
                    long id = BinaryPrimitives.ReadInt64LittleEndian(headerBuf.AsSpan(1));
                    int payloadLen = BinaryPrimitives.ReadInt32LittleEndian(headerBuf.AsSpan(9));
                    byte[] payload = [];
                    if (payloadLen > 0)
                    {
                        payload = new byte[payloadLen];
                        if (_stream.Read(payload, 0, payloadLen) < payloadLen) break;
                    }
                    if (type == TypeOffer) offered[id] = payload;
                    else if (type == TypeAck) offered.Remove(id);
                }
                liveEntries = offered.Select(kv => (kv.Key, kv.Value)).ToList();
            }

            _stream?.Close();

            var tmpPath = _path + ".tmp";
            long newSize = 0;
            using (var tmp = new FileStream(tmpPath, FileMode.Create, FileAccess.Write, FileShare.None, 4096, FileOptions.WriteThrough))
            {
                var headerBuf = new byte[HeaderSize];
                foreach (var (id, payload) in liveEntries)
                {
                    headerBuf[0] = TypeOffer;
                    BinaryPrimitives.WriteInt64LittleEndian(headerBuf.AsSpan(1), id);
                    BinaryPrimitives.WriteInt32LittleEndian(headerBuf.AsSpan(9), payload.Length);
                    tmp.Write(headerBuf);
                    if (payload.Length > 0)
                        tmp.Write(payload);
                    newSize += HeaderSize + payload.Length;
                }
            }

            File.Move(tmpPath, _path, overwrite: true);
            _stream = new FileStream(_path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, 4096, FileOptions.WriteThrough);
            _stream.Seek(0, SeekOrigin.End);
            _fileSize = newSize;
            _offerCount = liveEntries.Count;
            _ackCount = 0;
        }
    }

    /// <summary>Start periodic compaction on a background thread.</summary>
    public void StartPeriodicCompaction(CancellationToken ct)
    {
        if (_compactIntervalMs <= 0) return;
        _compactCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _ = Task.Run(async () =>
        {
            while (!_compactCts.Token.IsCancellationRequested)
            {
                try { await Task.Delay(_compactIntervalMs, _compactCts.Token); }
                catch (OperationCanceledException) { break; }

                // Only compact if there are acked entries to remove
                if (_ackCount > 0)
                    Compact();
            }
        }, _compactCts.Token);
    }

    public long FileSize { get { lock (_lock) return _fileSize; } }
    public long OfferCount => _offerCount;
    public long AckCount => _ackCount;
    public long LiveCount => _offerCount - _ackCount;

    private void WriteRecord(byte type, long id, byte[] payload)
    {
        Span<byte> header = stackalloc byte[HeaderSize];
        header[0] = type;
        BinaryPrimitives.WriteInt64LittleEndian(header[1..], id);
        BinaryPrimitives.WriteInt32LittleEndian(header[9..], payload.Length);
        _stream!.Write(header);
        if (payload.Length > 0)
            _stream.Write(payload);
        _stream.Flush();
        _fileSize += HeaderSize + payload.Length;
    }

    /// <summary>If WAL exceeds max size, compact immediately.</summary>
    private void CheckSize()
    {
        if (_maxSizeBytes > 0 && _fileSize > _maxSizeBytes && _ackCount > 0)
        {
            // Release lock temporarily for compact (it re-acquires)
            // Safe because we're already in the lock — compact uses the same lock
            // Do inline compact by replaying and rewriting
            CompactInline();
        }
    }

    private void CompactInline()
    {
        // Already holding _lock from AppendOffer/AppendAck
        // Replay in-place to find live entries
        _stream!.Seek(0, SeekOrigin.Begin);
        var offered = new Dictionary<long, byte[]>();
        var headerBuf = new byte[HeaderSize];
        while (true)
        {
            int read = _stream.Read(headerBuf, 0, HeaderSize);
            if (read < HeaderSize) break;
            byte type = headerBuf[0];
            long id = BinaryPrimitives.ReadInt64LittleEndian(headerBuf.AsSpan(1));
            int payloadLen = BinaryPrimitives.ReadInt32LittleEndian(headerBuf.AsSpan(9));
            byte[] payload = [];
            if (payloadLen > 0)
            {
                payload = new byte[payloadLen];
                if (_stream.Read(payload, 0, payloadLen) < payloadLen) break;
            }
            if (type == TypeOffer) offered[id] = payload;
            else if (type == TypeAck) offered.Remove(id);
        }

        // Rewrite in place
        _stream.Close();
        var tmpPath = _path + ".tmp";
        long newSize = 0;
        using (var tmp = new FileStream(tmpPath, FileMode.Create, FileAccess.Write, FileShare.None, 4096, FileOptions.WriteThrough))
        {
            foreach (var (id, payload) in offered)
            {
                Span<byte> h = stackalloc byte[HeaderSize];
                h[0] = TypeOffer;
                BinaryPrimitives.WriteInt64LittleEndian(h[1..], id);
                BinaryPrimitives.WriteInt32LittleEndian(h[9..], payload.Length);
                tmp.Write(h);
                if (payload.Length > 0) tmp.Write(payload);
                newSize += HeaderSize + payload.Length;
            }
        }
        File.Move(tmpPath, _path, overwrite: true);
        _stream = new FileStream(_path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, 4096, FileOptions.WriteThrough);
        _stream.Seek(0, SeekOrigin.End);
        _fileSize = newSize;
        _offerCount = offered.Count;
        _ackCount = 0;
    }

    public void Dispose()
    {
        _compactCts?.Cancel();
        lock (_lock)
        {
            _stream?.Dispose();
            _stream = null;
        }
    }
}
