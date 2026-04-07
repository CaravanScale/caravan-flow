using System.Net.Http.Headers;
using System.Text;
using ZincFlow.Core;
using ZincFlow.Fabric;

namespace ZincFlow.StdLib;

// --- UpdateAttribute: zero-alloc via overlay chain + pooled result ---

public sealed class UpdateAttribute : IProcessor
{
    private readonly string _key;
    private readonly string _value;

    public UpdateAttribute(string key, string value) { _key = key; _value = value; }

    public ProcessorResult Process(FlowFile ff)
        => SingleResult.Rent(FlowFile.WithAttribute(ff, _key, _value));
}

// --- LogAttribute: logs FlowFile and passes through ---

public sealed class LogAttribute : IProcessor
{
    private readonly string _prefix;

    public LogAttribute(string prefix) => _prefix = prefix;

    public ProcessorResult Process(FlowFile ff)
    {
        return SingleResult.Rent(ff);
    }
}

// --- ConvertJSONToRecord: parses Raw/Claim JSON content → Records ---

public sealed class ConvertJSONToRecord : IProcessor
{
    private readonly string _schemaName;
    private readonly IContentStore _store;
    private readonly JsonRecordReader _reader = new();

    public ConvertJSONToRecord(string schemaName, IContentStore store)
    {
        _schemaName = schemaName;
        _store = store;
    }

    public ProcessorResult Process(FlowFile ff)
    {
        byte[] data;
        if (ff.Content is Raw raw)
        {
            data = raw.Data.ToArray();
        }
        else if (ff.Content is ClaimContent)
        {
            var (resolved, error) = ContentHelpers.Resolve(_store, ff.Content);
            if (error != "") return FailureResult.Rent(error, ff);
            data = resolved;
        }
        else
        {
            return SingleResult.Rent(ff);
        }

        var schema = new Schema(_schemaName, []);
        var records = _reader.Read(data, schema);
        if (records.Count == 0)
            return FailureResult.Rent("no records parsed from JSON", ff);

        var updated = FlowFile.WithContent(ff, new RecordContent(
            new Dictionary<string, string> { ["name"] = records[0].RecordSchema.Name },
            records.Select(r =>
            {
                var dict = new Dictionary<string, object?>();
                foreach (var f in r.RecordSchema.Fields)
                    dict[f.Name] = r.GetField(f.Name);
                return dict;
            }).ToList()));
        return SingleResult.Rent(updated);
    }
}

// --- ConvertRecordToJSON: Records → JSON bytes ---

public sealed class ConvertRecordToJSON : IProcessor
{
    public ProcessorResult Process(FlowFile ff)
    {
        if (ff.Content is RecordContent rc)
        {
            byte[] bytes;
            try
            {
                bytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(rc.Records);
            }
            catch
            {
                return FailureResult.Rent("failed to serialize records to JSON", ff);
            }
            var updated = FlowFile.WithContent(ff, Raw.Rent(bytes));
            return SingleResult.Rent(updated);
        }
        return SingleResult.Rent(ff);
    }
}

// --- PutHTTP: POST FlowFile content to downstream HTTP endpoint ---

public sealed class PutHTTP : IProcessor
{
    private readonly string _endpoint;
    private readonly string _format;
    private readonly IContentStore _store;
    private readonly HttpClient _client;

    public PutHTTP(string endpoint, string format, IContentStore store)
    {
        _endpoint = endpoint;
        _format = format;
        _store = store;
        _client = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
    }

    public ProcessorResult Process(FlowFile ff)
    {
        var (data, error) = ContentHelpers.Resolve(_store, ff.Content);
        if (error != "")
            return FailureResult.Rent(error, ff);

        try
        {
            HttpContent httpContent;
            if (_format == "v3")
            {
                var packed = FlowFileV3.Pack(ff, data);
                httpContent = new ByteArrayContent(packed);
                httpContent.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
            }
            else
            {
                httpContent = new ByteArrayContent(data);
                var ct = ff.Attributes.TryGetValue("http.content.type", out var contentType)
                    ? contentType : "application/octet-stream";
                httpContent.Headers.ContentType = MediaTypeHeaderValue.Parse(ct);
            }

            // Forward flow attributes as X-Flow headers
            var current = ff.Attributes;
            while (current is not null)
            {
                if (current._key is not null && !current._key.StartsWith("http."))
                    httpContent.Headers.TryAddWithoutValidation($"X-Flow-{current._key}", current._value);
                current = current._key is not null ? current._parent : null;
            }
            if (current?._base is not null)
            {
                foreach (var (k, v) in current._base)
                {
                    if (!k.StartsWith("http."))
                        httpContent.Headers.TryAddWithoutValidation($"X-Flow-{k}", v);
                }
            }

            var response = _client.PostAsync(_endpoint, httpContent).GetAwaiter().GetResult();

            if (!response.IsSuccessStatusCode)
            {
                if ((int)response.StatusCode == 429)
                    return FailureResult.Rent($"backpressure: {response.StatusCode}", ff);
                return FailureResult.Rent($"HTTP {(int)response.StatusCode}: {response.ReasonPhrase}", ff);
            }

            var delivered = FlowFile.WithAttribute(ff, "delivery.status", ((int)response.StatusCode).ToString());
            delivered = FlowFile.WithAttribute(delivered, "delivery.endpoint", _endpoint);
            return SingleResult.Rent(delivered);
        }
        catch (TaskCanceledException)
        {
            return FailureResult.Rent($"timeout delivering to {_endpoint}", ff);
        }
        catch (HttpRequestException ex)
        {
            return FailureResult.Rent($"delivery failed: {ex.Message}", ff);
        }
    }
}

// --- PutFile: write FlowFile content to directory with configurable naming ---

public sealed class PutFile : IProcessor
{
    private readonly string _outputDir;
    private readonly string _namingAttr;
    private readonly string _prefix;
    private readonly string _suffix;
    private readonly IContentStore _store;
    private long _counter;

    public PutFile(string outputDir, string namingAttr, string prefix, string suffix, IContentStore store)
    {
        _outputDir = outputDir;
        _namingAttr = namingAttr;
        _prefix = prefix;
        _suffix = string.IsNullOrEmpty(suffix) ? ".dat" : suffix;
        _store = store;
        Directory.CreateDirectory(outputDir);
    }

    public ProcessorResult Process(FlowFile ff)
    {
        var (data, error) = ContentHelpers.Resolve(_store, ff.Content);
        if (error != "")
            return FailureResult.Rent(error, ff);

        string fileName;
        if (ff.Attributes.TryGetValue(_namingAttr, out var attrName) && !string.IsNullOrEmpty(attrName))
            fileName = $"{_prefix}{attrName}";
        else
        {
            var id = Interlocked.Increment(ref _counter);
            fileName = $"{_prefix}{id}{_suffix}";
        }

        fileName = Path.GetFileName(fileName);
        var path = Path.Combine(_outputDir, fileName);
        File.WriteAllBytes(path, data);

        var updated = FlowFile.WithAttribute(ff, "output.path", path);
        updated = FlowFile.WithAttribute(updated, "output.size", data.Length.ToString());
        return SingleResult.Rent(updated);
    }
}

// --- PutStdout: write FlowFile content to stdout ---

public sealed class PutStdout : IProcessor
{
    private readonly string _format;
    private readonly IContentStore _store;

    public PutStdout(string format, IContentStore store)
    {
        _format = format;
        _store = store;
    }

    public ProcessorResult Process(FlowFile ff)
    {
        if (_format == "attrs")
        {
            var sb = new StringBuilder();
            sb.Append($"[ff-{ff.NumericId}]");
            var current = ff.Attributes;
            while (current is not null)
            {
                if (current._key is not null)
                {
                    sb.Append($" {current._key}={current._value}");
                    current = current._parent;
                }
                else
                {
                    if (current._base is not null)
                        foreach (var (k, v) in current._base)
                            sb.Append($" {k}={v}");
                    break;
                }
            }
            Console.WriteLine(sb.ToString());
            return SingleResult.Rent(ff);
        }

        var (data, error) = ContentHelpers.Resolve(_store, ff.Content);
        if (error != "")
            return FailureResult.Rent(error, ff);

        if (_format == "hex")
            Console.WriteLine($"[ff-{ff.NumericId}] ({data.Length} bytes) {Convert.ToHexString(data[..Math.Min(data.Length, 128)])}");
        else
            Console.WriteLine($"[ff-{ff.NumericId}] {Encoding.UTF8.GetString(data)}");

        return SingleResult.Rent(ff);
    }
}
