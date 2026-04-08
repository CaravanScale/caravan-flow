using ZincFlow.Core;

namespace ZincFlow.StdLib;

/// <summary>
/// ConvertAvroToRecord: Avro binary content → RecordContent.
/// Requires a schema — reads from avro.schema attribute (JSON field defs) or config.
/// Config: schema_name, fields (comma-separated name:type pairs, e.g. "name:string,age:int,score:double").
/// </summary>
public sealed class ConvertAvroToRecord : IProcessor
{
    private readonly string _schemaName;
    private readonly Schema? _configSchema;
    private readonly IContentStore _store;
    private readonly AvroRecordReader _reader = new();

    public ConvertAvroToRecord(string schemaName, string fieldDefs, IContentStore store)
    {
        _schemaName = schemaName;
        _store = store;
        _configSchema = ParseFieldDefs(schemaName, fieldDefs);
    }

    public ProcessorResult Process(FlowFile ff)
    {
        byte[] data;
        if (ff.Content is Raw raw)
            data = raw.Data.ToArray();
        else if (ff.Content is ClaimContent)
        {
            var (resolved, error) = ContentHelpers.Resolve(_store, ff.Content);
            if (error != "") return FailureResult.Rent(error, ff);
            data = resolved;
        }
        else
            return SingleResult.Rent(ff);

        // Resolve schema: config fields take priority, then avro.schema attribute
        var schema = _configSchema;
        if (schema is null && ff.Attributes.TryGetValue("avro.schema", out var attrFields))
            schema = ParseFieldDefs(_schemaName, attrFields);
        if (schema is null || schema.Fields.Count == 0)
            return FailureResult.Rent("no schema: set 'fields' config or 'avro.schema' attribute", ff);

        var records = _reader.Read(data, schema);
        if (records.Count == 0)
            return FailureResult.Rent("no records decoded from Avro binary", ff);

        var updated = FlowFile.WithContent(ff, new RecordContent(
            new Dictionary<string, string> { ["name"] = schema.Name },
            records.Select(r =>
            {
                var dict = new Dictionary<string, object?>();
                foreach (var f in r.RecordSchema.Fields)
                    dict[f.Name] = r.GetField(f.Name);
                return dict;
            }).ToList()));
        return SingleResult.Rent(updated);
    }

    internal static Schema? ParseFieldDefs(string name, string fieldDefs)
    {
        if (string.IsNullOrWhiteSpace(fieldDefs)) return null;
        var fields = new List<Field>();
        foreach (var part in fieldDefs.Split(',', StringSplitOptions.TrimEntries))
        {
            var kv = part.Split(':', 2);
            if (kv.Length != 2) continue;
            var ft = kv[1].ToLowerInvariant() switch
            {
                "boolean" or "bool" => FieldType.Boolean,
                "int" or "int32" => FieldType.Int,
                "long" or "int64" => FieldType.Long,
                "float" or "float32" => FieldType.Float,
                "double" or "float64" => FieldType.Double,
                "bytes" => FieldType.Bytes,
                _ => FieldType.String
            };
            fields.Add(new Field(kv[0], ft));
        }
        return fields.Count > 0 ? new Schema(name, fields) : null;
    }
}

/// <summary>
/// ConvertRecordToAvro: RecordContent → Avro binary content.
/// Rebuilds Schema from RecordContent field types.
/// </summary>
public sealed class ConvertRecordToAvro : IProcessor
{
    private readonly AvroRecordWriter _writer = new();

    public ProcessorResult Process(FlowFile ff)
    {
        if (ff.Content is not RecordContent rc || rc.Records.Count == 0)
            return SingleResult.Rent(ff);

        // Build schema from first record's keys and value types
        var schemaName = rc.Schema.GetValueOrDefault("name") ?? "record";
        var fields = new List<Field>();
        var first = rc.Records[0];
        foreach (var (key, val) in first)
            fields.Add(new Field(key, InferFieldType(val)));
        var schema = new Schema(schemaName, fields);

        // Convert record dicts → GenericRecords
        var records = new List<GenericRecord>(rc.Records.Count);
        foreach (var dict in rc.Records)
        {
            var record = new GenericRecord(schema);
            foreach (var f in schema.Fields)
                record.SetField(f.Name, dict.GetValueOrDefault(f.Name));
            records.Add(record);
        }

        var bytes = _writer.Write(records, schema);
        var updated = FlowFile.WithContent(ff, Raw.Rent(bytes));
        updated = FlowFile.WithAttribute(updated, "avro.schema",
            string.Join(",", schema.Fields.Select(f => $"{f.Name}:{f.FieldType.ToString().ToLowerInvariant()}")));
        return SingleResult.Rent(updated);
    }

    private static FieldType InferFieldType(object? val) => val switch
    {
        null => FieldType.String,
        bool => FieldType.Boolean,
        int => FieldType.Int,
        long => FieldType.Long,
        float => FieldType.Float,
        double => FieldType.Double,
        byte[] => FieldType.Bytes,
        _ => FieldType.String
    };
}

/// <summary>
/// ConvertCSVToRecord: CSV content → RecordContent.
/// Config: delimiter, has_header.
/// </summary>
public sealed class ConvertCSVToRecord : IProcessor
{
    private readonly string _schemaName;
    private readonly CsvRecordReader _reader;
    private readonly IContentStore _store;

    public ConvertCSVToRecord(string schemaName, char delimiter, bool hasHeader, IContentStore store)
    {
        _schemaName = schemaName;
        _reader = new CsvRecordReader(delimiter, hasHeader);
        _store = store;
    }

    public ProcessorResult Process(FlowFile ff)
    {
        byte[] data;
        if (ff.Content is Raw raw)
            data = raw.Data.ToArray();
        else if (ff.Content is ClaimContent)
        {
            var (resolved, error) = ContentHelpers.Resolve(_store, ff.Content);
            if (error != "") return FailureResult.Rent(error, ff);
            data = resolved;
        }
        else
            return SingleResult.Rent(ff);

        var schema = new Schema(_schemaName, []);
        var records = _reader.Read(data, schema);
        if (records.Count == 0)
            return FailureResult.Rent("no records parsed from CSV", ff);

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

/// <summary>
/// ConvertRecordToCSV: RecordContent → CSV content.
/// Config: delimiter, include_header.
/// </summary>
public sealed class ConvertRecordToCSV : IProcessor
{
    private readonly CsvRecordWriter _writer;

    public ConvertRecordToCSV(char delimiter, bool includeHeader)
    {
        _writer = new CsvRecordWriter(delimiter, includeHeader);
    }

    public ProcessorResult Process(FlowFile ff)
    {
        if (ff.Content is not RecordContent rc || rc.Records.Count == 0)
            return SingleResult.Rent(ff);

        var schemaName = rc.Schema.GetValueOrDefault("name") ?? "record";
        var fields = rc.Records[0].Keys.Select(k => new Field(k, FieldType.String)).ToList();
        var schema = new Schema(schemaName, fields);

        var records = new List<GenericRecord>(rc.Records.Count);
        foreach (var dict in rc.Records)
        {
            var record = new GenericRecord(schema);
            foreach (var f in schema.Fields)
                record.SetField(f.Name, dict.GetValueOrDefault(f.Name));
            records.Add(record);
        }

        var bytes = _writer.Write(records, schema);
        var updated = FlowFile.WithContent(ff, Raw.Rent(bytes));
        return SingleResult.Rent(updated);
    }
}
