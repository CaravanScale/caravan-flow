using CaravanFlow.Core;
using CaravanFlow.Fabric;

namespace CaravanFlow.StdLib;

/// <summary>
/// ConvertAvroToRecord: Avro binary content → RecordContent.
/// Requires a schema — reads from avro.schema attribute (JSON field defs) or config.
/// Config: schemaName, fields (comma-separated name:type pairs, e.g. "name:string,age:int,score:double").
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
            if (kv.Length != 2 || string.IsNullOrEmpty(kv[0]) || string.IsNullOrEmpty(kv[1]))
                throw new ConfigException(
                    $"ConvertAvroToRecord: malformed field def '{part}' — expected 'name:type'");
            var typeLower = kv[1].ToLowerInvariant();
            // "string" is the default; other names map to typed variants. An
            // unrecognized name silently became String before — that made typos
            // (e.g. "stirng") silently lose type info. Now we reject.
            var ft = typeLower switch
            {
                "boolean" or "bool" => FieldType.Boolean,
                "int" or "int32" => FieldType.Int,
                "long" or "int64" => FieldType.Long,
                "float" or "float32" => FieldType.Float,
                "double" or "float64" => FieldType.Double,
                "bytes" => FieldType.Bytes,
                "string" => FieldType.String,
                _ => throw new ConfigException(
                    $"ConvertAvroToRecord: unknown field type '{kv[1]}' in '{part}' — valid: boolean, int, long, float, double, bytes, string")
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
/// Schema is embedded in the OCF header — no config needed for the simple case.
///
/// Optional reader-schema sources (checked in order):
///   1. <paramref name="staticReaderSchema"/> — set directly, e.g. from inline
///      JSON in config.
///   2. <paramref name="registryProvider"/> + subject/version — fetched lazily
///      on the first Process() call. Cached unless version is "latest".
///
/// Optional <paramref name="autoRegisterSubject"/>: after a successful decode,
/// the writer schema embedded in the OCF is registered under this subject in
/// the embedded registry. Identical-schema dedup means re-reading a known file
/// is a no-op; a never-before-seen schema becomes a new version. Auto-register
/// failures are logged but never block the FlowFile (advisory side effect).
///
/// When a reader schema is in play, decoded records are projected onto it via
/// Avro schema-evolution rules (type promotion, defaults). Incompatible
/// writer/reader pairs surface as a FailureResult, not an unhandled throw.
/// </summary>
public sealed class ConvertOCFToRecord : IProcessor
{
    private readonly IContentStore _store;
    private readonly OCFReader _reader = new();
    private readonly Schema? _staticReaderSchema;
    private readonly SchemaRegistryProvider? _registryProvider;
    private readonly string? _registrySubject;
    private readonly string _registryVersion;
    private readonly string? _autoRegisterSubject;
    private Schema? _resolvedReaderSchema;
    private readonly object _resolveLock = new();

    public ConvertOCFToRecord(
        IContentStore store,
        Schema? staticReaderSchema = null,
        SchemaRegistryProvider? registryProvider = null,
        string? registrySubject = null,
        string registryVersion = "latest",
        string? autoRegisterSubject = null)
    {
        _store = store;
        _staticReaderSchema = staticReaderSchema;
        _registryProvider = registryProvider;
        _registrySubject = registrySubject;
        _registryVersion = registryVersion;
        _autoRegisterSubject = autoRegisterSubject;
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

        // Resolve the reader schema lazily. Static wins; otherwise hit the registry
        // (cached after the first call). Registry failures become FailureResult so
        // the running server doesn't crash on a transient registry outage.
        Schema? readerSchema = _staticReaderSchema;
        if (readerSchema is null && _registryProvider is not null && _registrySubject is not null)
        {
            try
            {
                readerSchema = ResolveFromRegistry();
            }
            catch (Exception ex)
            {
                return FailureResult.Rent($"registry lookup failed for {_registrySubject}@{_registryVersion}: {ex.Message}", ff);
            }
        }

        // First decode pass: always with the writer schema (no projection) so we
        // can capture the writer schema for auto-registration. If a reader schema
        // is in play, project as a second step.
        Schema writerSchema;
        List<GenericRecord> records;
        try
        {
            (writerSchema, records) = _reader.Read(data, readerSchema: null);
        }
        catch (Exception ex)
        {
            return FailureResult.Rent($"OCF decode failed: {ex.Message}", ff);
        }

        if (records.Count == 0)
            return FailureResult.Rent("no records in OCF", ff);

        // Auto-register the writer schema if configured. Fire-and-forget on failure —
        // the advisory side effect must never block the data flow.
        if (_autoRegisterSubject is not null && _registryProvider is not null)
        {
            try
            {
                _registryProvider.Registry
                    .RegisterAsync(_autoRegisterSubject, writerSchema)
                    .GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[ocf] auto-register failed for subject '{_autoRegisterSubject}': {ex.Message}");
            }
        }

        // If a reader schema was supplied, project the writer-decoded records onto it.
        Schema effectiveSchema = writerSchema;
        if (readerSchema is not null)
        {
            var compat = SchemaResolver.Check(readerSchema, writerSchema);
            if (!compat.IsCompatible)
                return FailureResult.Rent("OCF reader schema incompatible: " + string.Join("; ", compat.Errors), ff);
            var projected = new List<GenericRecord>(records.Count);
            foreach (var r in records)
                projected.Add(SchemaResolver.Project(r, readerSchema, writerSchema));
            records = projected;
            effectiveSchema = readerSchema;
        }

        var updated = FlowFile.WithContent(ff, new RecordContent(effectiveSchema, records));
        return SingleResult.Rent(updated);
    }

    private Schema ResolveFromRegistry()
    {
        // Cache the registry-fetched schema unless the caller asked for "latest" —
        // in that case re-query so newly promoted versions get picked up.
        if (_registryVersion != "latest" && _resolvedReaderSchema is not null)
            return _resolvedReaderSchema;
        lock (_resolveLock)
        {
            if (_registryVersion != "latest" && _resolvedReaderSchema is not null)
                return _resolvedReaderSchema;
            var (_, schema) = _registryProvider!.Registry
                .GetSubjectVersionAsync(_registrySubject!, _registryVersion)
                .GetAwaiter().GetResult();
            _resolvedReaderSchema = schema;
            return schema;
        }
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
///
/// Config:
///   delimiter    — single character separator, default ","
///   hasHeader   — first row is the header, default true
///   fields       — optional explicit schema as "name:type,name:type" pairs
///                  (same syntax as ConvertAvroToRecord). When supplied, CSV
///                  cells are parsed into the declared CLR types (Long, Double,
///                  Boolean, Int, Float, Bytes) — the reader otherwise treats
///                  every cell as a string, which loses type fidelity when
///                  forwarding to schema-rich sinks like Avro/OCF.
///
/// When `fields` is set and `hasHeader` is true, the header row is still
/// consumed (and ignored). Schema field order wins.
/// </summary>
public sealed class ConvertCSVToRecord : IProcessor
{
    private readonly string _schemaName;
    private readonly CsvRecordReader _reader;
    private readonly Schema? _explicitSchema;
    private readonly IContentStore _store;

    public ConvertCSVToRecord(string schemaName, char delimiter, bool hasHeader, IContentStore store,
        string fieldDefs = "")
    {
        _schemaName = schemaName;
        _reader = new CsvRecordReader(delimiter, hasHeader);
        _explicitSchema = ConvertAvroToRecord.ParseFieldDefs(schemaName, fieldDefs);
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

        var schema = _explicitSchema ?? new Schema(_schemaName, []);
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
/// Config: delimiter, includeHeader.
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
/// Config: fields (format "fieldName:attrName;fieldName2:attrName2"), recordIndex (default 0).
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
            if (parts.Length != 2 || string.IsNullOrEmpty(parts[0]) || string.IsNullOrEmpty(parts[1]))
                throw new ConfigException(
                    $"ExtractRecordField: malformed entry '{entry}' — expected 'fieldName:attrName'");
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
            // Supports dotted paths (e.g., "address.city") via RecordHelpers.GetByPath.
            var val = RecordHelpers.GetByPath(record, field);
            if (val is not null)
                result = FlowFile.WithAttribute(result, attr, val.ToString()!);
        }
        return SingleResult.Rent(result);
    }
}

/// <summary>
/// QueryRecord: filter records using a JsonPath query (RFC-9535).
/// Backed by JsonCons.JsonPath over System.Text.Json — AOT-friendly,
/// no reflection, ~200 KB of library overhead vs Newtonsoft's ~7 MB.
///
/// Config: query (JsonPath expression).
///
/// Examples:
///   $[?(@.amount > 100)]                     — rows where amount exceeds 100
///   $[?(@.priority == 'high')]               — rows with a specific string value
///   $[?(@.address.country == 'US')]          — nested predicate
///   $[?(@.qty >= 2 &amp;&amp; @.qty &lt;= 10)]           — compound filter
///   $[*]                                      — match everything (trivial pass)
///
/// We mirror each incoming record into a System.Text.Json JsonElement
/// wrapped in a root JSON array, ask JsonSelector for matching
/// locations, then project those matches back to the original
/// GenericRecord list by array index. Schema and original types are
/// preserved — no re-materialization from JSON.
///
/// Mirrors caravan-flow-java's QueryRecord — both tracks accept the
/// same JsonPath strings in YAML, so configs are portable.
/// </summary>
public sealed class QueryRecord : IProcessor
{
    private readonly string _query;
    private readonly JsonCons.JsonPath.JsonSelector? _selector;
    private readonly string? _parseError;

    public QueryRecord(string query)
    {
        _query = string.IsNullOrWhiteSpace(query) ? "$" : query.Trim();
        try
        {
            _selector = JsonCons.JsonPath.JsonSelector.Parse(_query);
            _parseError = null;
        }
        catch (Exception ex)
        {
            _selector = null;
            _parseError = ex.Message;
        }
    }

    public ProcessorResult Process(FlowFile ff)
    {
        if (ff.Content is not RecordContent rc || rc.Records.Count == 0)
            return SingleResult.Rent(ff);

        if (_selector is null)
            return FailureResult.Rent($"QueryRecord: bad JsonPath '{_query}' — {_parseError}", ff);

        // Render the records as a top-level JSON array so a filter like
        // $[?(@.x > 1)] matches against each record in turn. We serialize
        // via System.Text.Json's Utf8JsonWriter (no reflection, AOT-safe)
        // then parse back into a JsonDocument for the selector.
        byte[] json;
        try
        {
            using var stream = new MemoryStream();
            using (var writer = new System.Text.Json.Utf8JsonWriter(stream))
            {
                writer.WriteStartArray();
                foreach (var record in rc.Records)
                    WriteValue(writer, record.ToDictionary());
                writer.WriteEndArray();
            }
            json = stream.ToArray();
        }
        catch (Exception ex)
        {
            return FailureResult.Rent($"QueryRecord: serialize failed — {ex.Message}", ff);
        }

        using var doc = System.Text.Json.JsonDocument.Parse(json);

        // JsonSelector.SelectPaths returns normalized paths to each
        // matched location. For our top-level array root, first path
        // segment is the array index — we strip it back out and use it
        // to index rc.Records directly so original typed values + schema
        // survive the round-trip untouched.
        List<JsonCons.JsonPath.NormalizedPath> paths;
        try
        {
            paths = _selector.SelectPaths(doc.RootElement).ToList();
        }
        catch (Exception ex)
        {
            return FailureResult.Rent($"QueryRecord: evaluate failed — {ex.Message}", ff);
        }

        var kept = new HashSet<int>();
        foreach (var path in paths)
        {
            // First hop beyond root is the array index. JsonCons exposes
            // each path component via enumeration; we take the first
            // ArrayIndex-ish segment and use it as the record index.
            int? idx = FirstArrayIndex(path);
            if (idx is null) continue;
            if (idx.Value >= 0 && idx.Value < rc.Records.Count) kept.Add(idx.Value);
        }

        if (kept.Count == 0)
            return DroppedResult.Instance;

        var filtered = new List<GenericRecord>(kept.Count);
        for (int i = 0; i < rc.Records.Count; i++)
            if (kept.Contains(i)) filtered.Add(rc.Records[i]);

        var updated = FlowFile.WithContent(ff, new RecordContent(rc.Schema, filtered));
        return SingleResult.Rent(updated);
    }

    private static int? FirstArrayIndex(JsonCons.JsonPath.NormalizedPath path)
    {
        // NormalizedPath is enumerable over its NormalizedPathNode segments.
        // First node is the root ($); the next Index-kind segment is our
        // top-level array index (since records are mirrored as a root array).
        foreach (var node in path)
        {
            if (node.ComponentKind == JsonCons.JsonPath.NormalizedPathNodeKind.Index)
                return node.GetIndex();
        }
        return null;
    }

    /// <summary>
    /// Serialize a record value into a Utf8JsonWriter without touching
    /// reflection. Handles nested GenericRecord (recurse into its
    /// ToDictionary view), dictionaries, lists, and primitive scalars.
    /// </summary>
    private static void WriteValue(System.Text.Json.Utf8JsonWriter writer, object? value)
    {
        switch (value)
        {
            case null:
                writer.WriteNullValue();
                break;
            case GenericRecord gr:
                WriteValue(writer, gr.ToDictionary());
                break;
            case Dictionary<string, object?> dict:
                writer.WriteStartObject();
                foreach (var (k, v) in dict)
                {
                    writer.WritePropertyName(k);
                    WriteValue(writer, v);
                }
                writer.WriteEndObject();
                break;
            case IDictionary<string, object?> idict:
                writer.WriteStartObject();
                foreach (var (k, v) in idict)
                {
                    writer.WritePropertyName(k);
                    WriteValue(writer, v);
                }
                writer.WriteEndObject();
                break;
            case List<object?> list:
                writer.WriteStartArray();
                foreach (var v in list) WriteValue(writer, v);
                writer.WriteEndArray();
                break;
            case string s:    writer.WriteStringValue(s); break;
            case bool b:      writer.WriteBooleanValue(b); break;
            case int i:       writer.WriteNumberValue(i); break;
            case long l:      writer.WriteNumberValue(l); break;
            case float f:     writer.WriteNumberValue(f); break;
            case double d:    writer.WriteNumberValue(d); break;
            case decimal m:   writer.WriteNumberValue(m); break;
            case byte[] bytes: writer.WriteBase64StringValue(bytes); break;
            case System.Collections.IEnumerable enumerable:
                writer.WriteStartArray();
                foreach (var v in enumerable) WriteValue(writer, v);
                writer.WriteEndArray();
                break;
            default:
                writer.WriteStringValue(value.ToString());
                break;
        }
    }

}
