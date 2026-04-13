# Getting started with zinc-flow (C#)

A NiFi-style data flow engine that ships as a single 27 MB Native AOT binary.
Records flow through a directed graph of processors; the runtime handles
backpressure, hot reload, provenance, and failure routing.

This guide covers the C# build (`zinc-flow-csharp`). For the Go and Python
runtimes, see the project root README.

## Install + build

The build is driven by `zinc-csharp` (the C# build wrapper that lives in the
sibling `zinc` repo). It generates a `.csproj` from `zinc.toml` and shells out
to `dotnet`.

```bash
# Install the build tool (one-time)
git clone https://github.com/ZincScale/zinc.git
/path/to/zinc/zinc-csharp/install.sh   # installs .NET 10 SDK + zinc-csharp

# Build
cd zinc-flow-csharp
zinc-csharp build              # AOT (default) — produces build/ZincFlow (27 MB)
zinc-csharp build --jit        # JIT — fast iteration during dev
zinc-csharp test               # JIT test run
zinc-csharp test --aot         # publish tests as AOT binary, then run (nightly)
```

## Run

```bash
./build/ZincFlow                       # serve, using ./config.yaml
./build/ZincFlow validate              # check config, exit 0 valid / 1 errors / 2 usage
./build/ZincFlow validate path.yaml    # explicit path
./build/ZincFlow bench                 # pipeline throughput micro-bench
./build/ZincFlow help
```

The validator catches the things that would otherwise crash the running server
or fire after the first FlowFile arrives: unknown processor types, missing
required config keys, malformed regex, dangling connections, DAG cycles.

## Config structure

Every config has up to six top-level sections. Only `flow` is required.

```yaml
server:                      # management API + dashboard
  port: 9091

logging:
  format: text               # or "json"

content:
  dir: /tmp/zinc-flow-csharp/content
  offload_threshold_kb: 256  # FlowFiles bigger than this offload to disk
  sweep_interval_ms: 300000  # orphan content cleanup interval

defaults:
  max_hops: 50               # max processor hops per FlowFile (cycle guard)
  max_concurrent_executions: 100  # backpressure semaphore

schema_registry:             # optional — auto-wires SchemaRegistryProvider
  url: http://schema-registry:8081
  auth: user:pass            # optional Basic auth

sources:                     # ingest connectors
  listen_http: { port: 9092, path: / }
  file:        { input_dir: /in, pattern: "*.avro", poll_interval_ms: 1000 }
  generate:    { content: "tick", poll_interval_ms: 5000 }   # for testing

flow:
  processors:
    name:
      type: ProcessorType
      requires: [content, schema_registry]   # provider deps
      config: { key: value, ... }
      connections:
        success: [next-processor]
        failure: [error-handler]
```

## Walkthrough — JSON pipeline

`examples/02-json-pipeline.yaml`:

```yaml
flow:
  processors:
    parse:
      type: ConvertJSONToRecord
      requires: [content]
      config: { schema_name: orders }
      connections: { success: [filter], failure: [errors] }

    filter:
      type: QueryRecord
      config: { where: "amount > 100" }
      connections: { success: [transform] }

    transform:
      type: TransformRecord
      config:
        operations: >
          compute:total:amount * qty;
          compute:tax:total * if(region == "US", 0.07, 0.15);
          compute:label:upper(region) + "-" + string(round(total + tax))
      connections: { success: [encode] }

    encode:
      type: ConvertRecordToJSON
      connections: { success: [write] }

    write:
      type: PutFile
      requires: [content]
      config: { output_dir: /tmp/zinc-out, suffix: .json }

    errors:
      type: LogAttribute
      config: { prefix: parse-error }
```

Then:

```bash
./build/ZincFlow validate examples/02-json-pipeline.yaml
cp examples/02-json-pipeline.yaml config.yaml && ./build/ZincFlow

curl -X POST http://localhost:9092/ \
  -H 'Content-Type: application/json' \
  -d '[{"id":1,"amount":120,"qty":2,"region":"US"},
       {"id":2,"amount":40,"qty":1,"region":"EU"},
       {"id":3,"amount":250,"qty":5,"region":"US"}]'
```

Two records survive the `amount > 100` filter; both get `total`/`tax`/`label`
computed and land in `/tmp/zinc-out/`.

## Processor catalog

| Type | Purpose | Required config |
|---|---|---|
| **Attributes** | | |
| `UpdateAttribute` | Set key=value | `key`, `value` |
| `FilterAttribute` | Keep or remove attributes | `mode`, `attributes` |
| `EvaluateExpression` | String templating on attributes | `expressions` |
| `LogAttribute` | Log all attributes | `prefix` |
| **Routing** | | |
| `RouteOnAttribute` | Branch by predicate on attributes | `routes` |
| `QueryRecord` | Filter records by predicate (supports nested paths) | `where` |
| **Records — codecs** | | |
| `ConvertJSONToRecord` / `ConvertRecordToJSON` | JSON ↔ records | `schema_name` (read) |
| `ConvertCSVToRecord` / `ConvertRecordToCSV` | CSV ↔ records | optional `delimiter`, `has_header` |
| `ConvertAvroToRecord` / `ConvertRecordToAvro` | Raw Avro binary ↔ records | `fields` (read) |
| `ConvertOCFToRecord` / `ConvertRecordToOCF` | Avro `.avro` files ↔ records | optional `reader_schema` / `reader_schema_subject`; codec ∈ `null`, `deflate`, `zstandard` |
| **Records — query/transform** | | |
| `ExtractRecordField` | Lift record fields to attributes (dotted paths) | `fields`, optional `record_index` |
| `TransformRecord` | Rename/remove/add/copy/upper/lower/default + `compute:expr` | `operations` |
| **Text** | | |
| `ReplaceText` | Regex find/replace on content | `pattern`, optional `replacement` |
| `ExtractText` | Regex capture → attributes | `pattern`, optional `group_names` |
| `SplitText` | Split content into multiple FlowFiles | `delimiter` |
| **Sinks** | | |
| `PutFile` | Write content to disk | `output_dir` |
| `PutHTTP` | POST content to URL | `endpoint` |
| `PutStdout` | Write content to stdout | optional `format` |

## Expression language (used by `TransformRecord compute:` and tests)

Tagged-value evaluator with type promotion. Identifiers resolve against record
fields (or attribute maps); dotted paths walk nested records.

| Category | Examples |
|---|---|
| Literals | `42`, `3.14`, `"hello"`, `'world'`, `true`, `false`, `null` |
| Arithmetic | `+ - * / %` (long+long → long; anything+double → double; anything+string → concat) |
| Comparison | `== != < > <= >=` (numeric across long/double; string is lex) |
| Logical | `&& || !` |
| Math | `abs`, `min`, `max`, `floor`, `ceil`, `round`, `pow`, `sqrt` |
| String | `upper`, `lower`, `trim`, `length`, `substring`, `replace`, `concat`, `contains`, `startsWith`, `endsWith` |
| Casts | `int`, `long`, `double`, `string`, `bool` |
| Logic | `coalesce(a, b, c)`, `if(cond, then, else)`, `isNull`, `isEmpty` |

Edge cases: division by zero → `0` for long, `NaN` for double (no throw —
data-engineering ergonomic). Missing identifiers → `null`, propagating.

`TransformRecord` directives:

```
rename:oldName:newName
remove:fieldName
add:fieldName:literal-value
copy:source:target
toUpper:fieldName  /  toLower:fieldName
default:fieldName:fallback   (only if missing/null)
compute:targetField:expression
```

## Avro schema evolution

When the writer schema in a `.avro` file differs from what the reader wants,
supply a `reader_schema` (inline JSON) or `reader_schema_subject` (registry).
The reader applies Avro 1.11 resolution rules:

- **Type promotion**: `int → long, float, double`; `long → float, double`;
  `float → double`; `string ↔ bytes`. Other promotions error.
- **Field added in reader**: filled from the reader's default; error if no default.
- **Field removed in reader**: silently dropped on read.

Incompatible reader schemas fail loudly at decode time, listing the offending
field.

## Logical types

`Field` carries `LogicalType` and (for decimal) `Precision`/`Scale`. Storage
stays on the underlying primitive — `LogicalTypeHelpers` converts:

```csharp
new Field("created_at", FieldType.Long,  logicalType: LogicalType.TimestampMillis)
new Field("event_date", FieldType.Int,   logicalType: LogicalType.Date)
new Field("user_id",    FieldType.String, logicalType: LogicalType.Uuid)
new Field("amount",     FieldType.Bytes, logicalType: LogicalType.Decimal,
          precision: 12, scale: 2)

LogicalTypeHelpers.ToTimestampMillis(DateTime.UtcNow);    // → long
LogicalTypeHelpers.FromUuid(rec.GetField("user_id")!);    // → Guid
LogicalTypeHelpers.FromDecimalBytes(bytes, scale: 2);     // → decimal
```

## Schema registry

```yaml
schema_registry:
  url: http://schema-registry:8081
  auth: user:pass            # optional
```

The `SchemaRegistryProvider` is auto-wired at startup. Processors request it via
`requires: [schema_registry]` and configure `reader_schema_subject` (+ optional
`reader_schema_version`, defaults to `latest`). Schemas cache by id and by
(subject, version); `latest` lookups always hit the wire.

## Management API + dashboard

Running on the management port (default 9091):

```bash
GET  /                          # read-only DAG dashboard (HTML)
GET  /health                    # liveness + source status
GET  /metrics                   # Prometheus
GET  /api/flow                  # full graph + per-processor stats
GET  /api/processors            # processor names
GET  /api/processor-stats       # per-processor counters
GET  /api/registry              # available processor types
GET  /api/sources               # source connector list + state
GET  /api/providers             # provider list + state
GET  /api/provenance?n=50       # recent provenance events

# Mutations (idempotent where possible)
POST   /api/processors/add        {"name":"x","type":"...","config":{...},"connections":{...}}
DELETE /api/processors/remove     {"name":"x"}
POST   /api/processors/enable     {"name":"x"}
POST   /api/processors/disable    {"name":"x"}
POST   /api/sources/start         {"name":"x"}
POST   /api/sources/stop          {"name":"x"}
POST   /api/providers/enable      {"name":"x"}
POST   /api/providers/disable     {"name":"x"}
POST   /api/reload                # re-read config.yaml + atomic graph swap
```

Editing `config.yaml` while the server is running also triggers a hot-reload
(file-watcher with 500 ms debounce; atomic graph swap; in-flight FlowFiles are
unaffected).

## Examples

The `examples/` directory ships five validated configs:

1. `01-hello-flow.yaml` — minimal HTTP → log → file.
2. `02-json-pipeline.yaml` — parse, filter, typed transforms, write.
3. `03-avro-ocf.yaml` — read real `.avro` files, transform, write back zstd-compressed.
4. `04-schema-evolution.yaml` — inline reader schema with type promotion + defaults.
5. `05-schema-registry.yaml` — registry-backed reader schemas.

Validate any of them before deploying:

```bash
for f in examples/*.yaml; do ./build/ZincFlow validate "$f"; done
```
