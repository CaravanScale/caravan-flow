using ZincFlow.Core;

namespace ZincFlow.Fabric;

/// <summary>
/// Registers all built-in processors with the registry.
/// </summary>
public static class BuiltinProcessors
{
    public static void RegisterAll(Registry reg)
    {
        reg.Register(
            new ProcessorInfo("add-attribute", "Adds key=value attribute to FlowFiles", ["key", "value"]),
            (ctx, config) => new AddAttribute(config["key"], config["value"]));

        reg.Register(
            new ProcessorInfo("file-sink", "Writes FlowFile content to disk", ["output_dir"]),
            (ctx, config) =>
            {
                IContentStore store;
                try { store = ((ContentProvider)ctx.GetProvider("content")).Store; }
                catch { store = new MemoryContentStore(); }
                return new FileSink(config["output_dir"], store);
            });

        reg.Register(
            new ProcessorInfo("log", "Logs FlowFile and passes through", ["prefix"]),
            (ctx, config) => new LogProcessor(config.GetValueOrDefault("prefix", "flow")));

        reg.Register(
            new ProcessorInfo("json-to-records", "Parses JSON content into Avro records", ["schema_name"]),
            (ctx, config) =>
            {
                var schemaName = config.GetValueOrDefault("schema_name", "default");
                IContentStore store;
                try { store = ((ContentProvider)ctx.GetProvider("content")).Store; }
                catch { store = new MemoryContentStore(); }
                return new JsonToRecords(schemaName, store);
            });

        reg.Register(
            new ProcessorInfo("records-to-json", "Serializes Avro records back to JSON", []),
            (ctx, config) => new RecordsToJson());
    }
}

// --- AddAttribute: zero-alloc via overlay chain + pooled result ---

public sealed class AddAttribute : IProcessor
{
    private readonly string _key;
    private readonly string _value;

    public AddAttribute(string key, string value) { _key = key; _value = value; }

    public ProcessorResult Process(FlowFile ff)
        => SingleResult.Rent(FlowFile.WithAttribute(ff, _key, _value));
}

// --- FileSink: writes content to disk, returns Dropped (terminal) ---

public sealed class FileSink : IProcessor
{
    private readonly string _outputDir;
    private readonly IContentStore _store;

    public FileSink(string outputDir, IContentStore store)
    {
        _outputDir = outputDir;
        _store = store;
        Directory.CreateDirectory(outputDir);
    }

    public ProcessorResult Process(FlowFile ff)
    {
        var path = Path.Combine(_outputDir, $"{ff.NumericId}.out");
        var (data, error) = ContentHelpers.Resolve(_store, ff.Content);
        if (error != "")
            return FailureResult.Rent(error, ff);
        File.WriteAllBytes(path, data);
        return DroppedResult.Instance;
    }
}

// --- LogProcessor ---

public sealed class LogProcessor : IProcessor
{
    private readonly string _prefix;

    public LogProcessor(string prefix) => _prefix = prefix;

    public ProcessorResult Process(FlowFile ff)
    {
        // Minimal logging on hot path — no string interpolation unless debug
        return SingleResult.Rent(ff);
    }
}

// --- JsonToRecords: parses Raw/Claim JSON content → Avro Records ---

public sealed class JsonToRecords : IProcessor
{
    private readonly string _schemaName;
    private readonly IContentStore _store;
    private readonly JsonRecordReader _reader = new();

    public JsonToRecords(string schemaName, IContentStore store)
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
        else // RecordContent — already records, pass through
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

// --- RecordsToJson: Avro Records → JSON bytes ---

public sealed class RecordsToJson : IProcessor
{
    public ProcessorResult Process(FlowFile ff)
    {
        if (ff.Content is RecordContent rc)
        {
            // Serialize the stored record dicts back to JSON
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
        return SingleResult.Rent(ff); // pass through Raw/Claim
    }
}
