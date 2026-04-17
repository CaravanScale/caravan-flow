namespace CaravanFlow.Core;

// --- Avro field types ---

public enum FieldType
{
    Null, Boolean, Int, Long, Float, Double, String, Bytes,
    Array, Map, Record, Enum, Union
}

// --- Logical types: semantic annotations on top of primitives.
// Storage stays on the underlying primitive (long for timestamps, int for dates,
// string for uuid, bytes for decimal); the tag travels with the schema so OCF
// roundtrips preserve intent. Use LogicalTypeHelpers for conversions.
public enum LogicalType
{
    None,
    TimestampMillis,   // long: millis since Unix epoch (UTC)
    TimestampMicros,   // long: micros since Unix epoch (UTC)
    Date,              // int: days since Unix epoch
    TimeMillis,        // int: millis past midnight
    TimeMicros,        // long: micros past midnight
    Uuid,              // string: RFC 4122
    Decimal            // bytes: two's complement big-endian; uses Precision/Scale
}

// --- Field: named, typed column ---

public sealed class Field
{
    public string Name { get; }
    public FieldType FieldType { get; }
    public LogicalType LogicalType { get; }
    public int Precision { get; }       // Decimal only
    public int Scale { get; }           // Decimal only
    public object? DefaultValue { get; }

    public Field(string name, FieldType fieldType, object? defaultValue = null,
                 LogicalType logicalType = LogicalType.None, int precision = 0, int scale = 0)
    {
        Name = name;
        FieldType = fieldType;
        DefaultValue = defaultValue;
        LogicalType = logicalType;
        Precision = precision;
        Scale = scale;
    }
}

// --- Schema: ordered list of fields ---

public sealed class Schema
{
    public string Name { get; }
    public List<Field> Fields { get; }

    public Schema(string name, List<Field> fields)
    {
        Name = name;
        Fields = fields;
    }
}

// --- GenericRecord: schema-aware row of data ---

public sealed class GenericRecord
{
    public Schema RecordSchema { get; }
    internal readonly Dictionary<string, object?> _values = new();

    public GenericRecord(Schema schema) => RecordSchema = schema;

    public void SetField(string key, object? value) => _values[key] = value;

    public object? GetField(string key) => _values.GetValueOrDefault(key);

    public Schema GetSchema() => RecordSchema;

    public Dictionary<string, object?> ToDictionary()
    {
        var dict = new Dictionary<string, object?>(_values.Count);
        foreach (var (k, v) in _values)
            dict[k] = v;
        return dict;
    }

    public GenericRecord Clone()
    {
        var copy = new GenericRecord(RecordSchema);
        foreach (var (k, v) in _values)
            copy._values[k] = v;
        return copy;
    }
}

// --- Record interfaces ---

public interface IRecordReader
{
    List<GenericRecord> Read(byte[] data, Schema schema);
}

public interface IRecordWriter
{
    byte[] Write(List<GenericRecord> records, Schema schema);
}

public interface IRecordProcessor
{
    GenericRecord ProcessRecord(GenericRecord record, Schema schema);
}

// --- Helpers ---

public static class RecordHelpers
{
    public static Schema SchemaFromFields(string name, List<Field> fields) => new(name, fields);

    public static GenericRecord NewRecord(Schema schema, Dictionary<string, object?> values)
    {
        var record = new GenericRecord(schema);
        foreach (var (key, value) in values)
            record.SetField(key, value);
        return record;
    }

    public static string GetFieldString(GenericRecord record, string fieldName)
    {
        var val = record.GetField(fieldName);
        return val?.ToString() ?? "";
    }

    public static GenericRecord WithField(GenericRecord record, string fieldName, object? value)
    {
        var schema = record.GetSchema();
        var copy = new GenericRecord(schema);
        foreach (var f in schema.Fields)
            copy.SetField(f.Name, record.GetField(f.Name));
        copy.SetField(fieldName, value);
        return copy;
    }

    /// <summary>
    /// Reads a field value via dotted path (e.g., "address.city"). Walks GenericRecord
    /// values and Dictionary&lt;string, object?&gt; values transparently. Returns null
    /// if any intermediate segment is missing or not a navigable container.
    /// </summary>
    public static object? GetByPath(GenericRecord record, string path)
    {
        if (string.IsNullOrEmpty(path)) return null;
        if (!path.Contains('.')) return record.GetField(path);

        var parts = path.Split('.');
        object? cur = record.GetField(parts[0]);
        for (int i = 1; i < parts.Length; i++)
        {
            switch (cur)
            {
                case null: return null;
                case GenericRecord gr: cur = gr.GetField(parts[i]); break;
                case IDictionary<string, object?> dict:
                    cur = dict.TryGetValue(parts[i], out var v) ? v : null;
                    break;
                default: return null;
            }
        }
        return cur;
    }

    /// <summary>
    /// Writes a field value via dotted path. Walks existing GenericRecord intermediates;
    /// missing intermediates are created as empty-schema GenericRecords. Returns true
    /// if the write reached its target. Callers should not rely on the schema being
    /// updated for newly-created intermediate records — use this for ad-hoc nested set,
    /// not for cases where strict schema fidelity matters.
    /// </summary>
    public static bool SetByPath(GenericRecord record, string path, object? value)
    {
        if (string.IsNullOrEmpty(path)) return false;
        if (!path.Contains('.')) { record.SetField(path, value); return true; }

        var parts = path.Split('.');
        var cur = record;
        for (int i = 0; i < parts.Length - 1; i++)
        {
            var next = cur.GetField(parts[i]);
            if (next is GenericRecord gr) { cur = gr; continue; }
            // Create a new empty sub-record and attach it.
            var sub = new GenericRecord(new Schema(parts[i], []));
            cur.SetField(parts[i], sub);
            cur = sub;
        }
        cur.SetField(parts[^1], value);
        return true;
    }
}
