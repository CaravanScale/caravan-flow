namespace ZincFlow.Core;

// --- Avro field types ---

public enum FieldType
{
    Null, Boolean, Int, Long, Float, Double, String, Bytes,
    Array, Map, Record, Enum, Union
}

// --- Field: named, typed column ---

public sealed class Field
{
    public string Name { get; }
    public FieldType FieldType { get; }
    public object? DefaultValue { get; }

    public Field(string name, FieldType fieldType, object? defaultValue = null)
    {
        Name = name;
        FieldType = fieldType;
        DefaultValue = defaultValue;
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
}
