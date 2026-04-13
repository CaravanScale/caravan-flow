using System.Text;
using System.Text.Json;
using ZincFlow.Core;

namespace ZincFlow.StdLib;

/// <summary>
/// Parses and emits Avro JSON schema (spec 1.11) into/from zinc-flow's Schema.
/// Supports primitive types and nullable-primitive unions (["null", T]).
/// Hand-rolled with Utf8JsonReader/Writer — no reflection, AOT-safe.
///
/// Limitations: records embedded in records, arrays, maps, enums, fixed, and
/// logical types are not yet mapped to Schema (they'd need Schema extensions).
/// </summary>
public static class AvroSchemaJson
{
    public static Schema Parse(string json)
    {
        var bytes = Encoding.UTF8.GetBytes(json);
        var reader = new Utf8JsonReader(bytes, isFinalBlock: true, state: default);
        return ParseSchema(ref reader, defaultName: "root");
    }

    private static Schema ParseSchema(ref Utf8JsonReader reader, string defaultName)
    {
        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
            throw new InvalidOperationException("expected schema object");

        string? name = null;
        string? type = null;
        List<Field>? fields = null;

        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) continue;
            var prop = reader.GetString();
            reader.Read();

            if (prop == "type") type = reader.GetString();
            else if (prop == "name") name = reader.GetString();
            else if (prop == "fields") fields = ParseFieldList(ref reader);
            else SkipValue(ref reader);
        }

        if (type != "record")
            throw new InvalidOperationException($"top-level Avro schema must be a record, got: {type}");
        if (fields is null || fields.Count == 0)
            throw new InvalidOperationException("record schema missing 'fields' array");

        return new Schema(name ?? defaultName, fields);
    }

    private static List<Field> ParseFieldList(ref Utf8JsonReader reader)
    {
        if (reader.TokenType != JsonTokenType.StartArray)
            throw new InvalidOperationException("expected 'fields' to be an array");

        var result = new List<Field>();
        while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
        {
            if (reader.TokenType != JsonTokenType.StartObject)
                throw new InvalidOperationException("expected field object");

            string? fieldName = null;
            FieldType? fieldType = null;

            while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
            {
                if (reader.TokenType != JsonTokenType.PropertyName) continue;
                var prop = reader.GetString();
                reader.Read();

                if (prop == "name") fieldName = reader.GetString();
                else if (prop == "type") fieldType = ParseTypeToken(ref reader);
                else SkipValue(ref reader);
            }

            if (fieldName is null || fieldType is null)
                throw new InvalidOperationException("field missing name or type");
            result.Add(new Field(fieldName, fieldType.Value));
        }
        return result;
    }

    // Parse a "type" token: primitive string, nullable-union array ["null", T], or nested type object.
    private static FieldType ParseTypeToken(ref Utf8JsonReader reader)
    {
        if (reader.TokenType == JsonTokenType.String)
            return MapPrimitive(reader.GetString() ?? "");

        if (reader.TokenType == JsonTokenType.StartArray)
        {
            // Union: take first non-null branch as the effective type (nullability erased).
            FieldType? resolved = null;
            while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
            {
                if (reader.TokenType == JsonTokenType.String)
                {
                    var s = reader.GetString();
                    if (s == "null") continue;
                    resolved ??= MapPrimitive(s ?? "");
                }
                else
                {
                    SkipValue(ref reader);
                    resolved ??= FieldType.String;
                }
            }
            return resolved ?? FieldType.String;
        }

        if (reader.TokenType == JsonTokenType.StartObject)
        {
            // Nested type object — extract its "type" field; fall back to String for unsupported shapes.
            FieldType result = FieldType.String;
            while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
            {
                if (reader.TokenType == JsonTokenType.PropertyName && reader.GetString() == "type")
                {
                    reader.Read();
                    if (reader.TokenType == JsonTokenType.String)
                        result = MapPrimitive(reader.GetString() ?? "");
                    else SkipValue(ref reader);
                }
                else SkipValue(ref reader);
            }
            return result;
        }

        throw new InvalidOperationException($"unsupported type token: {reader.TokenType}");
    }

    private static FieldType MapPrimitive(string typeName) => typeName switch
    {
        "null" => FieldType.Null,
        "boolean" => FieldType.Boolean,
        "int" => FieldType.Int,
        "long" => FieldType.Long,
        "float" => FieldType.Float,
        "double" => FieldType.Double,
        "string" => FieldType.String,
        "bytes" => FieldType.Bytes,
        "enum" => FieldType.Enum,
        "array" => FieldType.Array,
        "map" => FieldType.Map,
        "record" => FieldType.Record,
        _ => FieldType.String
    };

    private static void SkipValue(ref Utf8JsonReader reader)
    {
        if (reader.TokenType == JsonTokenType.StartObject || reader.TokenType == JsonTokenType.StartArray)
            reader.Skip();
    }

    public static string Emit(Schema schema)
    {
        using var ms = new MemoryStream();
        using (var writer = new Utf8JsonWriter(ms))
        {
            writer.WriteStartObject();
            writer.WriteString("type", "record");
            writer.WriteString("name", schema.Name);
            writer.WriteStartArray("fields");
            foreach (var f in schema.Fields)
            {
                writer.WriteStartObject();
                writer.WriteString("name", f.Name);
                writer.WriteString("type", PrimitiveName(f.FieldType));
                writer.WriteEndObject();
            }
            writer.WriteEndArray();
            writer.WriteEndObject();
        }
        return Encoding.UTF8.GetString(ms.ToArray());
    }

    private static string PrimitiveName(FieldType type) => type switch
    {
        FieldType.Null => "null",
        FieldType.Boolean => "boolean",
        FieldType.Int => "int",
        FieldType.Long => "long",
        FieldType.Float => "float",
        FieldType.Double => "double",
        FieldType.String => "string",
        FieldType.Bytes => "bytes",
        _ => "string"
    };
}
