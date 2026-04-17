using System.Net.Http.Headers;
using System.Text;
using CaravanFlow.Core;
using CaravanFlow.Fabric;

namespace CaravanFlow.StdLib;

// --- RouteOnAttribute: route FlowFiles based on attribute predicates ---

public sealed class RouteOnAttribute : IProcessor
{
    private readonly List<(string Route, RuleCondition Condition)> _routes;

    public RouteOnAttribute(string routes)
    {
        _routes = new List<(string, RuleCondition)>();
        if (string.IsNullOrWhiteSpace(routes)) return;
        var entries = routes.Split(';', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        for (int i = 0; i < entries.Length; i++)
        {
            var entry = entries[i];
            var colonIdx = entry.IndexOf(':');
            if (colonIdx <= 0)
                throw new ConfigException($"RouteOnAttribute: malformed route at index {i}: '{entry}' — expected 'name: attr OP value'");
            var routeName = entry[..colonIdx].Trim();
            var conditionStr = entry[(colonIdx + 1)..].Trim();
            var parts = conditionStr.Split(' ', 3, StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length < 2)
                throw new ConfigException($"RouteOnAttribute: route '{routeName}' has malformed condition: '{conditionStr}' — expected 'attr OP value'");
            var attribute = parts[0];
            var op = ParseOperator(parts[1], routeName);
            var value = parts.Length >= 3 ? parts[2] : "";
            _routes.Add((routeName, new BaseRule(attribute, op, value)));
        }
    }

    public ProcessorResult Process(FlowFile ff)
    {
        foreach (var (route, condition) in _routes)
            if (condition.Evaluate(ff.Attributes))
                return RoutedResult.Rent(route, ff);
        return RoutedResult.Rent("unmatched", ff);
    }

    private static Operator ParseOperator(string op, string routeName) => op.ToUpperInvariant() switch
    {
        "EQ" => Operator.Eq,
        "NEQ" => Operator.Neq,
        "CONTAINS" => Operator.Contains,
        "STARTSWITH" => Operator.StartsWith,
        "ENDSWITH" => Operator.EndsWith,
        "EXISTS" => Operator.Exists,
        "GT" => Operator.Gt,
        "LT" => Operator.Lt,
        _ => throw new ConfigException(
            $"RouteOnAttribute: route '{routeName}' has unknown operator '{op}' — valid: EQ, NEQ, CONTAINS, STARTSWITH, ENDSWITH, EXISTS, GT, LT")
    };
}

// --- FilterAttribute: remove or keep specific FlowFile attributes ---

public sealed class FilterAttribute : IProcessor
{
    private readonly bool _remove;
    private readonly HashSet<string> _attrSet;

    public FilterAttribute(string mode, string attributes)
    {
        _remove = !string.Equals(mode, "keep", StringComparison.OrdinalIgnoreCase);
        _attrSet = new HashSet<string>(
            attributes.Split(';', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries));
    }

    public ProcessorResult Process(FlowFile ff)
    {
        var allAttrs = ff.Attributes.ToDictionary();

        Dictionary<string, string> filtered;
        if (_remove)
            filtered = allAttrs.Where(kv => !_attrSet.Contains(kv.Key))
                .ToDictionary(kv => kv.Key, kv => kv.Value);
        else
            filtered = allAttrs.Where(kv => _attrSet.Contains(kv.Key))
                .ToDictionary(kv => kv.Key, kv => kv.Value);

        ff.Content.AddRef(); // new FlowFile shell shares same content
        var newFf = FlowFile.Rent(ff.NumericId, AttributeMap.FromDict(filtered), ff.Content, ff.Timestamp, ff.HopCount);
        return SingleResult.Rent(newFf);
    }
}

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
    private readonly LoggingProvider? _log;

    public LogAttribute(string prefix, LoggingProvider? log = null)
    {
        _prefix = prefix;
        _log = log;
    }

    public ProcessorResult Process(FlowFile ff)
    {
        if (_log is not null)
        {
            var extras = new Dictionary<string, string> { ["ff"] = $"ff-{ff.NumericId}", ["size"] = ff.Content.Size.ToString() };
            // Walk attributes for logging
            var current = ff.Attributes;
            while (current is not null)
            {
                if (current._key is not null)
                {
                    extras[current._key] = current._value!;
                    current = current._parent;
                }
                else
                {
                    if (current._base is not null)
                        foreach (var (k, v) in current._base)
                            extras[k] = v;
                    break;
                }
            }
            _log.Log("INFO", _prefix, $"ff-{ff.NumericId}", extras);
        }
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

        var effectiveSchema = records[0].RecordSchema;
        var updated = FlowFile.WithContent(ff, new RecordContent(effectiveSchema, records));
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
                var writer = new JsonRecordWriter();
                bytes = writer.Write(rc.Records, rc.Schema);
            }
            catch (Exception ex)
            {
                return FailureResult.Rent($"failed to serialize records to JSON: {ex.Message}", ff);
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

            // Forward flow attributes as X-Flow-* headers. Walk the overlay
            // chain bottom-up (base dict first, then each overlay) so newer
            // values override older ones — matches AttributeMap read semantics.
            var attrs = new Dictionary<string, string>();
            var stack = new Stack<AttributeMap>();
            for (var node = ff.Attributes; node is not null; node = node._key is not null ? node._parent : null)
                stack.Push(node);
            while (stack.Count > 0)
            {
                var node = stack.Pop();
                if (node._base is not null)
                {
                    foreach (var (k, v) in node._base) attrs[k] = v;
                }
                else if (node._key is not null)
                {
                    attrs[node._key] = node._value!;
                }
            }
            foreach (var (k, v) in attrs)
            {
                if (k.StartsWith("http.")) continue;
                httpContent.Headers.TryAddWithoutValidation($"X-Flow-{k}", v);
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
    private readonly string _format;
    private readonly IContentStore _store;
    private long _counter;

    public PutFile(string outputDir, string namingAttr, string prefix, string suffix, IContentStore store,
        string format = "raw")
    {
        _outputDir = outputDir;
        _namingAttr = namingAttr;
        _prefix = prefix;
        _suffix = string.IsNullOrEmpty(suffix) ? ".dat" : suffix;
        _format = format;
        _store = store;
        Directory.CreateDirectory(outputDir);
    }

    public ProcessorResult Process(FlowFile ff)
    {
        var (data, error) = ContentHelpers.Resolve(_store, ff.Content);
        if (error != "")
            return FailureResult.Rent(error, ff);

        // V3 framing wraps attributes + content. The on-disk file is then a
        // self-describing NiFi FlowFile that GetFile (with unpackV3=true)
        // can ingest losslessly on the other side.
        if (_format == "v3")
            data = FlowFileV3.Pack(ff, data);

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

        if (_format == "v3")
        {
            var packed = FlowFileV3.Pack(ff, data);
            Console.WriteLine($"[ff-{ff.NumericId}] v3 ({packed.Length} bytes) {Convert.ToHexString(packed[..Math.Min(packed.Length, 128)])}");
        }
        else if (_format == "hex")
            Console.WriteLine($"[ff-{ff.NumericId}] ({data.Length} bytes) {Convert.ToHexString(data[..Math.Min(data.Length, 128)])}");
        else
            Console.WriteLine($"[ff-{ff.NumericId}] {Encoding.UTF8.GetString(data)}");

        return SingleResult.Rent(ff);
    }
}

// --- PackageFlowFileV3: encode (attributes + content) as a V3 binary blob ---

/// <summary>
/// Wraps a FlowFile's attributes + content into a single NiFi FlowFile V3 binary
/// blob. The output FlowFile carries that binary as its content; downstream sinks
/// (PutFile, PutHTTP, PutStdout) can write it verbatim and another caravan-flow or
/// NiFi instance with a V3-aware reader will round-trip the original attributes.
///
/// Lets V3 framing be a pipeline step rather than a sink-only concern.
/// </summary>
public sealed class PackageFlowFileV3 : IProcessor
{
    private readonly IContentStore _store;

    public PackageFlowFileV3(IContentStore store) => _store = store;

    public ProcessorResult Process(FlowFile ff)
    {
        var (data, error) = ContentHelpers.Resolve(_store, ff.Content);
        if (error != "")
            return FailureResult.Rent(error, ff);

        var packed = FlowFileV3.Pack(ff, data);
        var packedContent = ContentHelpers.MaybeOffload(_store, packed);
        var updated = FlowFile.WithContent(ff, packedContent);
        // Mark MIME type so HTTP sinks can advertise it correctly.
        updated = FlowFile.WithAttribute(updated, "http.content.type", "application/flowfile-v3");
        updated = FlowFile.WithAttribute(updated, "v3.packaged", "true");
        return SingleResult.Rent(updated);
    }
}

// --- UnpackageFlowFileV3: decode a V3-binary content blob into one or more FlowFiles ---

/// <summary>
/// Inverse of PackageFlowFileV3. Treats the FlowFile's content as V3-framed bytes
/// (which can hold N FlowFiles concatenated) and emits each unpacked FlowFile
/// with its original attributes restored.
///
/// On non-V3 input (no magic header), emits a FailureResult so the caller can
/// route to an error handler instead of silently discarding.
/// </summary>
public sealed class UnpackageFlowFileV3 : IProcessor
{
    private static readonly byte[] V3Magic = "NiFiFF3"u8.ToArray();
    private readonly IContentStore _store;

    public UnpackageFlowFileV3(IContentStore store) => _store = store;

    public ProcessorResult Process(FlowFile ff)
    {
        var (data, error) = ContentHelpers.Resolve(_store, ff.Content);
        if (error != "")
            return FailureResult.Rent(error, ff);

        if (data.Length < V3Magic.Length || !data.AsSpan(0, V3Magic.Length).SequenceEqual(V3Magic))
            return FailureResult.Rent("not a V3-framed FlowFile (missing NiFiFF3 magic)", ff);

        var unpacked = FlowFileV3.UnpackAll(data);
        if (unpacked.Count == 0)
            return FailureResult.Rent("V3 stream contained no FlowFiles", ff);

        if (unpacked.Count == 1)
            return SingleResult.Rent(unpacked[0]);

        var result = MultipleResult.Rent();
        result.FlowFiles.AddRange(unpacked);
        return result;
    }
}
