using System.Collections;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace ZincFlow.Core;

// AOT-safe serialization metadata. Every type DESERIALIZED by System.Text.Json
// in this codebase is registered below — the source generator produces
// reflection-free JsonTypeInfo for each. Serialization of open-ended shapes
// (Dictionary<string, object?>, etc.) goes through ZincJson.Serialize below,
// which uses Utf8JsonWriter directly to avoid STJ's polymorphic object path
// (which is not AOT-safe without reflection fallback).
[JsonSerializable(typeof(Dictionary<string, string>))]
[JsonSerializable(typeof(Dictionary<string, object?>))]
[JsonSerializable(typeof(List<Dictionary<string, object?>>))]
internal partial class ZincJsonContext : JsonSerializerContext { }

// AOT-safe JSON writer for open shapes: recursively serializes anything the
// codebase actually hands to JSON output — primitives, IDictionary, IEnumerable,
// and JsonElement. Uses runtime interface dispatch only, no reflection.
internal static class ZincJson
{
    public static byte[] Serialize(object? value)
    {
        using var ms = new MemoryStream();
        using (var writer = new Utf8JsonWriter(ms))
            WriteValue(writer, value);
        return ms.ToArray();
    }

    public static string SerializeToString(object? value) => Encoding.UTF8.GetString(Serialize(value));

    public static void WriteValue(Utf8JsonWriter writer, object? value)
    {
        switch (value)
        {
            case null: writer.WriteNullValue(); return;
            case string s: writer.WriteStringValue(s); return;
            case bool b: writer.WriteBooleanValue(b); return;
            case int i: writer.WriteNumberValue(i); return;
            case long l: writer.WriteNumberValue(l); return;
            case double d: writer.WriteNumberValue(d); return;
            case float f: writer.WriteNumberValue(f); return;
            case decimal m: writer.WriteNumberValue(m); return;
            case short sh: writer.WriteNumberValue(sh); return;
            case byte by: writer.WriteNumberValue(by); return;
            case uint u: writer.WriteNumberValue(u); return;
            case ulong ul: writer.WriteNumberValue(ul); return;
            case DateTime dt: writer.WriteStringValue(dt); return;
            case DateTimeOffset dto: writer.WriteStringValue(dto); return;
            case Guid g: writer.WriteStringValue(g); return;
            case JsonElement je: je.WriteTo(writer); return;
            case IDictionary dict:
                writer.WriteStartObject();
                foreach (DictionaryEntry entry in dict)
                {
                    writer.WritePropertyName(entry.Key?.ToString() ?? "");
                    WriteValue(writer, entry.Value);
                }
                writer.WriteEndObject();
                return;
            case IEnumerable enumerable:
                writer.WriteStartArray();
                foreach (var item in enumerable)
                    WriteValue(writer, item);
                writer.WriteEndArray();
                return;
            default:
                writer.WriteStringValue(value.ToString() ?? "");
                return;
        }
    }
}
