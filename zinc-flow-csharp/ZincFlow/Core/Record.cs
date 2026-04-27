namespace ZincFlow.Core;

// Record model for in-process dataflow. The field types and logical types here
// are structurally Avro-compatible (so converters to/from Avro OCF, Kafka, etc.
// stay trivial) but this is NOT an Avro runtime — nothing in this file knows
// how to serialize. It is a dict-with-schema that flows between processors.

public enum FieldType
{
    Null, Boolean, Int, Long, Float, Double, String, Bytes,
    Array, Map, Record, Enum, Union
}

// Logical types: semantic annotations on top of primitives. Storage stays on
// the underlying primitive (long for timestamps, int for dates, string for
// uuid, bytes for decimal); the tag travels with the schema so OCF roundtrips
// preserve intent. Use LogicalTypeHelpers for conversions.
public enum LogicalType
{
    None,
    TimestampMillis,
    TimestampMicros,
    Date,
    TimeMillis,
    TimeMicros,
    Uuid,
    Decimal
}

public sealed class Field
{
    public string Name { get; }
    public FieldType FieldType { get; }
    public LogicalType LogicalType { get; }
    public int Precision { get; }
    public int Scale { get; }
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

// Record: dict of named fields, optionally annotated with a Schema.
// Semantically immutable — callers should use RecordHelpers.WithField rather
// than SetField on shared records. SetField remains for hot-path builders
// (e.g. format decoders populating a freshly-allocated Record).
//
// Schema is optional: a schemaless Record is a bag of fields with no type
// declarations. Converters to wire formats (Avro, OCF) require schema;
// schemaless converters (JSON without declared types) do not. Processors
// that don't care about types (UpdateRecord, RouteRecord, SplitRecord)
// work either way.
public sealed class Record
{
    public Schema? RecordSchema { get; }
    internal readonly Dictionary<string, object?> _values = new();

    public Record() => RecordSchema = null;

    public Record(Schema? schema) => RecordSchema = schema;

    public void SetField(string key, object? value) => _values[key] = value;

    public object? GetField(string key) => _values.GetValueOrDefault(key);

    public Schema? GetSchema() => RecordSchema;

    public Dictionary<string, object?> ToDictionary()
    {
        var dict = new Dictionary<string, object?>(_values.Count);
        foreach (var (k, v) in _values)
            dict[k] = v;
        return dict;
    }

    public Record Clone()
    {
        var copy = new Record(RecordSchema);
        foreach (var (k, v) in _values)
            copy._values[k] = v;
        return copy;
    }
}

public interface IRecordReader
{
    List<Record> Read(byte[] data, Schema schema);
}

public interface IRecordWriter
{
    byte[] Write(List<Record> records, Schema schema);
}

public interface IRecordProcessor
{
    Record ProcessRecord(Record record, Schema schema);
}

public static class RecordHelpers
{
    public static Schema SchemaFromFields(string name, List<Field> fields) => new(name, fields);

    public static Record NewRecord(Schema schema, Dictionary<string, object?> values)
    {
        var record = new Record(schema);
        foreach (var (key, value) in values)
            record.SetField(key, value);
        return record;
    }

    public static string GetFieldString(Record record, string fieldName)
    {
        var val = record.GetField(fieldName);
        return val?.ToString() ?? "";
    }

    public static Record WithField(Record record, string fieldName, object? value)
    {
        var schema = record.GetSchema();
        var copy = new Record(schema);
        // When a schema is attached, iterate it so the schema's declared field
        // order is preserved. Schemaless records just carry over whatever is
        // in the value dict.
        if (schema is not null)
        {
            foreach (var f in schema.Fields)
                copy.SetField(f.Name, record.GetField(f.Name));
        }
        else
        {
            foreach (var (k, v) in record._values)
                copy._values[k] = v;
        }
        copy.SetField(fieldName, value);
        return copy;
    }

    /// <summary>
    /// Reads a field value via dotted path (e.g., "address.city"). Walks Record
    /// values and Dictionary&lt;string, object?&gt; values transparently. Returns null
    /// if any intermediate segment is missing or not a navigable container.
    /// </summary>
    public static object? GetByPath(Record record, string path)
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
                case Record gr: cur = gr.GetField(parts[i]); break;
                case IDictionary<string, object?> dict:
                    cur = dict.TryGetValue(parts[i], out var v) ? v : null;
                    break;
                default: return null;
            }
        }
        return cur;
    }

    /// <summary>
    /// Writes a field value via dotted path. Walks existing Record intermediates;
    /// missing intermediates are created as empty-schema Records. Returns true
    /// if the write reached its target.
    /// </summary>
    public static bool SetByPath(Record record, string path, object? value)
    {
        if (string.IsNullOrEmpty(path)) return false;
        if (!path.Contains('.')) { record.SetField(path, value); return true; }

        var parts = path.Split('.');
        var cur = record;
        for (int i = 0; i < parts.Length - 1; i++)
        {
            var next = cur.GetField(parts[i]);
            if (next is Record gr) { cur = gr; continue; }
            var sub = new Record(new Schema(parts[i], []));
            cur.SetField(parts[i], sub);
            cur = sub;
        }
        cur.SetField(parts[^1], value);
        return true;
    }
}
