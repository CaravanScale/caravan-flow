# crystal-avro

Apache Avro 1.11 for Crystal. Hand-rolled, no reflection, AOT-safe,
works under `preview_mt`. Intended as the missing "Avro for Crystal"
shard so applications targeting AOT'd static binaries don't have to
build their own.

Current status: **0.1.0 — JSON schema + binary codec.** OCF container
and schema-evolution resolver are coming in 0.2.

## Install

In your `shard.yml`:

```yaml
dependencies:
  avro:
    github: CaravanScale/crystal-avro
    version: ~> 0.1
```

Then `shards install`.

## Schema

Parse and emit the JSON schema form described in Avro 1.11 §2:

```crystal
require "avro"

schema = Avro::SchemaJson.parse(<<-JSON)
  {
    "type": "record",
    "name": "User",
    "fields": [
      {"name": "id",    "type": "long"},
      {"name": "name",  "type": "string"},
      {"name": "email", "type": ["null", "string"]},
      {"name": "ts",    "type": {"type": "long", "logicalType": "timestamp-millis"}}
    ]
  }
  JSON

schema.fields.map(&.name)     # => ["id", "name", "email", "ts"]
schema.field("email").try(&.nullable)              # => true
schema.field("ts").try(&.logical)                  # => Avro::LogicalType::TimestampMillis

Avro::SchemaJson.emit(schema) # => "{\"type\":\"record\",...}"
```

Supported:

- Primitives: `null`, `boolean`, `int`, `long`, `float`, `double`,
  `bytes`, `string`
- Complex: `record` (nesting), `array`, `map`, `union`
  - `["null", T]` and `[T, "null"]` lift to `Field#nullable = true`
    for ergonomic code
  - Other unions keep their branch list in `Field#union_branches`
- Logical types: `decimal` (with precision/scale), `uuid`, `date`,
  `time-millis`, `time-micros`, `timestamp-millis`, `timestamp-micros`
- Aliases, doc strings, default values

Not yet: enums, fixed, nested unions that aren't `[null, T]`. PRs welcome.

## Binary codec

Avro's positional binary format (1.11 §3) as `IO`-based read/write:

```crystal
record : Avro::Record = {
  "id"   => 42_i64.as(Avro::Value),
  "name" => "zeus".as(Avro::Value),
}

io = IO::Memory.new
Avro::BinaryCodec.write_record(io, record, schema)

io.rewind
back = Avro::BinaryCodec.read_record(io, schema)
# back["id"]   == 42_i64
# back["name"] == "zeus"
```

Avro value types:

| Avro type    | Crystal type         |
|--------------|----------------------|
| `null`       | `Nil`                |
| `boolean`    | `Bool`               |
| `int`        | `Int32`              |
| `long`       | `Int64`              |
| `float`      | `Float32`            |
| `double`     | `Float64`            |
| `bytes`      | `Bytes`              |
| `string`     | `String`             |
| `record`     | `Record` (alias)     |
| `array<T>`   | `Array(Value)`       |
| `map<T>`     | `Hash(String, Value)`|

The codec accepts loose types on write (coerces `String`→`Int64`,
etc. where there's no ambiguity) and returns strict types on read.

## What's next (0.2)

- **OCF reader / writer** — magic `Obj\x01`, metadata map, sync
  markers, block framing; codecs: `null`, `deflate`, `zstandard`.
- **Schema resolver** — writer-schema → reader-schema projection for
  Avro evolution (type promotion, field add with default, field
  removal, aliases).
- **Confluent wire format** — 1-byte magic + 4-byte schema ID prefix,
  plus an `HTTP::Client`-backed schema registry adapter.

## Design notes

- **Pure `IO`**, no `Bytes.new(capacity)` hand-rolled buffers. Works
  with `IO::Memory`, sockets, files, gzip/zstd streams — all the same.
- **Static methods on modules, not classes with state.** Encoding is
  positional and stateless; state would just be a place for bugs.
- **Positional field resolution.** No reflection, no string keys in
  the hot path. Write/read iterate `schema.fields`; the codec looks
  up by index, not name, at encode/decode time.
- **Coercion is explicit, narrow, and errors loudly.** We accept
  `String → Int32` because caravan-flow hands us config values as
  strings, but we never guess `Int32 → Bool` or anything weirder.

## License

MIT. See `LICENSE`.
