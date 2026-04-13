using ZincFlow.Core;
using ZincFlow.Fabric;

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

        var updated = FlowFile.WithContent(ff, new RecordContent(schema, records));
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

        var bytes = _writer.Write(rc.Records, rc.Schema);
        var updated = FlowFile.WithContent(ff, Raw.Rent(bytes));
        updated = FlowFile.WithAttribute(updated, "avro.schema",
            string.Join(",", rc.Schema.Fields.Select(f => $"{f.Name}:{f.FieldType.ToString().ToLowerInvariant()}")));
        return SingleResult.Rent(updated);
    }
}

/// <summary>
/// ConvertOCFToRecord: Avro Object Container File → RecordContent.
/// Schema is embedded in the OCF header — no config needed.
/// Supports null and deflate codecs.
/// </summary>
public sealed class ConvertOCFToRecord : IProcessor
{
    private readonly IContentStore _store;
    private readonly OCFReader _reader = new();

    public ConvertOCFToRecord(IContentStore store) => _store = store;

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

        Schema schema;
        List<GenericRecord> records;
        try
        {
            (schema, records) = _reader.Read(data);
        }
        catch (Exception ex)
        {
            return FailureResult.Rent($"OCF decode failed: {ex.Message}", ff);
        }

        if (records.Count == 0)
            return FailureResult.Rent("no records in OCF", ff);

        var updated = FlowFile.WithContent(ff, new RecordContent(schema, records));
        return SingleResult.Rent(updated);
    }
}

/// <summary>
/// ConvertRecordToOCF: RecordContent → Avro Object Container File.
/// Emits a single-block OCF with embedded JSON schema. Optional deflate codec.
/// </summary>
public sealed class ConvertRecordToOCF : IProcessor
{
    private readonly OCFWriter _writer;

    public ConvertRecordToOCF(string codec = AvroOCF.CodecNull) => _writer = new OCFWriter(codec);

    public ProcessorResult Process(FlowFile ff)
    {
        if (ff.Content is not RecordContent rc || rc.Records.Count == 0)
            return SingleResult.Rent(ff);

        byte[] bytes;
        try
        {
            bytes = _writer.Write(rc.Records, rc.Schema);
        }
        catch (Exception ex)
        {
            return FailureResult.Rent($"OCF encode failed: {ex.Message}", ff);
        }

        var updated = FlowFile.WithContent(ff, Raw.Rent(bytes));
        return SingleResult.Rent(updated);
    }
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

        var effectiveSchema = records[0].RecordSchema;
        var updated = FlowFile.WithContent(ff, new RecordContent(effectiveSchema, records));
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

        var bytes = _writer.Write(rc.Records, rc.Schema);
        var updated = FlowFile.WithContent(ff, Raw.Rent(bytes));
        return SingleResult.Rent(updated);
    }
}

/// <summary>
/// ExtractRecordField: extract field values from GenericRecord into FlowFile attributes.
/// Config: fields (format "fieldName:attrName;fieldName2:attrName2"), record_index (default 0).
/// </summary>
public sealed class ExtractRecordField : IProcessor
{
    private readonly List<(string Field, string Attr)> _fields;
    private readonly int _recordIndex;

    public ExtractRecordField(string fields, int recordIndex = 0)
    {
        _recordIndex = recordIndex;
        _fields = new List<(string, string)>();
        if (string.IsNullOrWhiteSpace(fields)) return;
        foreach (var entry in fields.Split(';', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
        {
            var parts = entry.Split(':', 2, StringSplitOptions.TrimEntries);
            if (parts.Length != 2) continue;
            _fields.Add((parts[0], parts[1]));
        }
    }

    public ProcessorResult Process(FlowFile ff)
    {
        if (ff.Content is not RecordContent rc || rc.Records.Count == 0)
            return SingleResult.Rent(ff);
        if (_recordIndex >= rc.Records.Count)
            return SingleResult.Rent(ff);
        var record = rc.Records[_recordIndex];
        var result = ff;
        foreach (var (field, attr) in _fields)
        {
            var val = record.GetField(field);
            if (val is not null)
                result = FlowFile.WithAttribute(result, attr, val.ToString()!);
        }
        return SingleResult.Rent(result);
    }
}

/// <summary>
/// QueryRecord: filter records using a simple predicate expression.
/// Config: where (format "field operator value").
/// Operators: =, !=, &gt;, &lt;, &gt;=, &lt;=, contains, startsWith, endsWith.
/// </summary>
public sealed class QueryRecord : IProcessor
{
    private readonly string _field;
    private readonly string _operator;
    private readonly string _value;

    public QueryRecord(string where)
    {
        _field = "";
        _operator = "=";
        _value = "";
        if (string.IsNullOrWhiteSpace(where)) return;

        // Parse "field operator value" — operator may be multi-char (>=, <=, !=, contains, startsWith, endsWith)
        var trimmed = where.Trim();
        // Find field name (first token)
        var spaceIdx = trimmed.IndexOf(' ');
        if (spaceIdx <= 0) { _field = trimmed; return; }
        _field = trimmed[..spaceIdx];
        var rest = trimmed[(spaceIdx + 1)..].TrimStart();

        // Try multi-char operators first
        string[] multiOps = [">=", "<=", "!=", "contains", "startsWith", "endsWith"];
        foreach (var op in multiOps)
        {
            if (rest.StartsWith(op, StringComparison.OrdinalIgnoreCase) &&
                (rest.Length == op.Length || rest[op.Length] == ' '))
            {
                _operator = op;
                _value = rest.Length > op.Length ? rest[(op.Length + 1)..].Trim() : "";
                return;
            }
        }
        // Single-char operators: =, >, <
        if (rest.Length > 0 && (rest[0] == '=' || rest[0] == '>' || rest[0] == '<'))
        {
            _operator = rest[0].ToString();
            _value = rest.Length > 1 ? rest[1..].Trim() : "";
        }
        else
        {
            // Unknown — treat rest as operator + value
            var opEnd = rest.IndexOf(' ');
            if (opEnd > 0)
            {
                _operator = rest[..opEnd];
                _value = rest[(opEnd + 1)..].Trim();
            }
            else
            {
                _operator = rest;
            }
        }
    }

    public ProcessorResult Process(FlowFile ff)
    {
        if (ff.Content is not RecordContent rc || rc.Records.Count == 0)
            return SingleResult.Rent(ff);

        var filtered = new List<GenericRecord>();
        foreach (var record in rc.Records)
        {
            if (EvaluatePredicate(record))
                filtered.Add(record);
        }

        if (filtered.Count == 0)
            return DroppedResult.Instance;

        var updated = FlowFile.WithContent(ff, new RecordContent(rc.Schema, filtered));
        return SingleResult.Rent(updated);
    }

    private bool EvaluatePredicate(GenericRecord record)
    {
        var fieldVal = record.GetField(_field);
        if (fieldVal is null) return false;
        var fieldStr = fieldVal.ToString()!;

        return _operator.ToLowerInvariant() switch
        {
            "=" => CompareValues(fieldStr, _value) == 0,
            "!=" => CompareValues(fieldStr, _value) != 0,
            ">" => CompareValues(fieldStr, _value) > 0,
            "<" => CompareValues(fieldStr, _value) < 0,
            ">=" => CompareValues(fieldStr, _value) >= 0,
            "<=" => CompareValues(fieldStr, _value) <= 0,
            "contains" => fieldStr.Contains(_value, StringComparison.Ordinal),
            "startswith" => fieldStr.StartsWith(_value, StringComparison.Ordinal),
            "endswith" => fieldStr.EndsWith(_value, StringComparison.Ordinal),
            _ => false
        };
    }

    private static int CompareValues(string a, string b)
    {
        if (double.TryParse(a, out var da) && double.TryParse(b, out var db))
            return da.CompareTo(db);
        return string.Compare(a, b, StringComparison.Ordinal);
    }
}
