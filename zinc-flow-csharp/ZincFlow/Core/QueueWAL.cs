using System.Buffers.Binary;
using System.Text;

namespace ZincFlow.Core;

/// <summary>
/// Write-ahead log for FlowQueue persistence. Append-only binary file.
/// On startup, replays the WAL to restore queue state. Periodically compacts
/// by rewriting only live entries.
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
    private FileStream? _stream;
    private readonly object _lock = new();
    private long _entryCount;

    public QueueWAL(string path)
    {
        _path = path;
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir))
            Directory.CreateDirectory(dir);
    }

    public void Open()
    {
        _stream = new FileStream(_path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, 4096, FileOptions.WriteThrough);
        _stream.Seek(0, SeekOrigin.End);
    }

    public void AppendOffer(long entryId, byte[] v3Payload)
    {
        lock (_lock)
        {
            if (_stream is null) return;
            WriteRecord(TypeOffer, entryId, v3Payload);
            _entryCount++;
        }
    }

    public void AppendAck(long entryId)
    {
        lock (_lock)
        {
            if (_stream is null) return;
            WriteRecord(TypeAck, entryId, []);
            _entryCount++;
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

        // Seek back to end for appending
        _stream.Seek(0, SeekOrigin.End);

        return offered.Select(kv => (kv.Key, kv.Value)).ToList();
    }

    /// <summary>
    /// Compact: rewrite WAL with only the live entries (offered but not acked).
    /// </summary>
    public void Compact(List<(long Id, byte[] V3Payload)> liveEntries)
    {
        lock (_lock)
        {
            _stream?.Close();

            var tmpPath = _path + ".tmp";
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
                }
            }

            File.Move(tmpPath, _path, overwrite: true);
            _stream = new FileStream(_path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, 4096, FileOptions.WriteThrough);
            _stream.Seek(0, SeekOrigin.End);
            _entryCount = liveEntries.Count;
        }
    }

    public long EntryCount => _entryCount;

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
    }

    public void Dispose()
    {
        lock (_lock)
        {
            _stream?.Dispose();
            _stream = null;
        }
    }
}
