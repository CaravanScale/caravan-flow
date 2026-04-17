# Getting started with caravan-flow (C#)

A NiFi-style data flow engine that ships as a single 27 MB Native AOT binary.
Records flow through a directed graph of processors; the runtime handles
backpressure, hot reload, provenance, and failure routing.

This guide covers the C# build (`caravan-flow-csharp`). For the Go and Python
runtimes, see the project root README.

## Install + build

The build is driven by `caravan-csharp` (the C# build wrapper that lives in the
sibling `caravan` repo). It generates a `.csproj` from `caravan.toml` and shells out
to `dotnet`.

```bash
# Install the build tool (one-time)
git clone https://github.com/CaravanScale/caravan.git
/path/to/caravan/caravan-csharp/install.sh   # installs .NET 10 SDK + caravan-csharp

# Build
cd caravan-flow-csharp
caravan-csharp build              # AOT (default) — produces build/CaravanFlow (27 MB)
caravan-csharp build --jit        # JIT — fast iteration during dev
caravan-csharp test               # JIT test run
caravan-csharp test --aot         # publish tests as AOT binary, then run (nightly)
```

## Run

```bash
./build/CaravanFlow                       # serve, using ./config.yaml
./build/CaravanFlow validate              # check config, exit 0 valid / 1 errors / 2 usage
./build/CaravanFlow validate path.yaml    # explicit path
./build/CaravanFlow bench                 # pipeline throughput micro-bench
./build/CaravanFlow help
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
  dir: /tmp/caravan-flow-csharp/content
  offload_threshold_kb: 256  # FlowFiles bigger than this offload to disk
  sweep_interval_ms: 300000  # orphan content cleanup interval

defaults:
  max_hops: 50               # max processor hops per FlowFile (cycle guard)
  max_concurrent_executions: 100  # backpressure semaphore

schemas:                     # optional — pre-load the embedded schema registry
  orders:
    file: schemas/orders.avsc       # path relative to this config file
  events:
    inline: |                       # or inline JSON
      {"type":"record","name":"Event","fields":[
        {"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}]}

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

`examples/02-json-pipeline.yaml`. Note: `schema_name` is just a *label* used in
provenance/log output. JSON is schema-on-read — `ConvertJSONToRecord` infers
the field types from the first object in the array (Long, Double, String, etc.).
You only need a `schemas:` section if a downstream processor wants to
*project* records onto a registered reader schema (see example 05 for that
pattern).

```yaml
flow:
  processors:
    parse:
      type: ConvertJSONToRecord
      requires: [content]
      config: { schema_name: orders }     # label only; types inferred from JSON
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
      config: { output_dir: /tmp/caravan-out, suffix: .json }

    errors:
      type: LogAttribute
      config: { prefix: parse-error }
```

Then:

```bash
./build/CaravanFlow validate examples/02-json-pipeline.yaml
cp examples/02-json-pipeline.yaml config.yaml && ./build/CaravanFlow

curl -X POST http://localhost:9092/ \
  -H 'Content-Type: application/json' \
  -d '[{"id":1,"amount":120,"qty":2,"region":"US"},
       {"id":2,"amount":40,"qty":1,"region":"EU"},
       {"id":3,"amount":250,"qty":5,"region":"US"}]'
```

Two records survive the `amount > 100` filter; both get `total`/`tax`/`label`
computed and land in `/tmp/caravan-out/`.

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
| `ConvertCSVToRecord` / `ConvertRecordToCSV` | CSV ↔ records | optional `delimiter`, `has_header`, `fields` (typed columns) |
| `ConvertAvroToRecord` / `ConvertRecordToAvro` | Raw Avro binary ↔ records | `fields` (read) |
| `ConvertOCFToRecord` / `ConvertRecordToOCF` | Avro `.avro` files ↔ records | optional `reader_schema` / `reader_schema_subject`; codec ∈ `null`, `deflate`, `zstandard` |
| **Records — query/transform** | | |
| `ExtractRecordField` | Lift record fields to attributes (dotted paths) | `fields`, optional `record_index` |
| `TransformRecord` | Rename/remove/add/copy/upper/lower/default + `compute:expr` | `operations` |
| **Text** | | |
| `ReplaceText` | Regex find/replace on content | `pattern`, optional `replacement` |
| `ExtractText` | Regex capture → attributes | `pattern`, optional `group_names` |
| `SplitText` | Split content into multiple FlowFiles | `delimiter` |
| **NiFi V3 framing** (let V3 wrapping be a pipeline step) | | |
| `PackageFlowFileV3` | Wrap (attributes + content) into V3 binary content; output content is the V3 frame | none |
| `UnpackageFlowFileV3` | Decode V3 binary content into one or more original FlowFiles (MultipleResult on multi-frame) | none |
| **Sinks** | | |
| `PutFile` | Write content to disk. `format: v3` writes V3-framed bytes (lossless attributes); default is raw. | `output_dir`, optional `format` |
| `PutHTTP` | POST content to URL. `format: v3` sends V3-framed body; default sends raw with `X-Flow-*` headers carrying attributes. | `endpoint`, optional `format` |
| `PutStdout` | Write content to stdout. `format` ∈ `text` (default), `hex`, `attrs`, `v3` | optional `format` |

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

## Schema registry (embedded)

caravan-flow ships with an in-process schema registry. No external service — fits
airgapped deployments. Three population paths:

**1. Config (`schemas:` section).** Loaded at startup, re-applied on hot reload:

```yaml
schemas:
  orders:
    file: schemas/orders.avsc       # path relative to the config file
  users:
    inline: |                       # or inline JSON
      {"type":"record","name":"User","fields":[
        {"name":"id","type":"long"},
        {"name":"name","type":"string"}]}
```

**2. REST.** Confluent-shape endpoints under `/api/schema-registry/*` on the
management port. Existing tooling that targets the Confluent API works against
us:

| Endpoint | Purpose |
|---|---|
| `GET /api/schema-registry/subjects` | List all subjects |
| `GET /api/schema-registry/subjects/{s}/versions` | List versions for a subject |
| `GET /api/schema-registry/subjects/{s}/versions/{v\|latest}` | Fetch a specific version |
| `GET /api/schema-registry/schemas/ids/{id}` | Fetch by global id |
| `POST /api/schema-registry/subjects/{s}/versions` | Register (`{"schema":"..."}`); returns `{"id":N}`. Identical schema = same id (dedup). |
| `DELETE /api/schema-registry/subjects/{s}` | Drop a subject |
| `DELETE /api/schema-registry/subjects/{s}/versions/{v}` | Drop one version |

**3. Auto-capture from incoming OCF files.** Set `auto_register_subject` on
`ConvertOCFToRecord` and every `.avro` file's writer schema is registered under
that subject. Identical schemas are no-ops; new schemas become new versions.
This is the airgapped magic — the first file teaches the registry:

```yaml
parse:
  type: ConvertOCFToRecord
  requires: [content, schema_registry]
  config: { auto_register_subject: discovered }
```

**Reading with a registered schema.** Processors that need a reader schema
reference it by subject:

```yaml
parse:
  type: ConvertOCFToRecord
  requires: [content, schema_registry]
  config:
    reader_schema_subject: orders
    reader_schema_version: latest    # or "3" to pin
```

Lookup is lazy (first FlowFile triggers it) and cached. `latest` always
re-resolves so newly-promoted versions get picked up.

**Caveats.** Storage is in-memory. Restart loses runtime registrations;
config-loaded ones come back on the next boot. Disk persistence is a future
follow-up. Hot-reload of `config.yaml` re-applies the `schemas:` section as an
additive upsert (changes create new versions; missing subjects are left alone —
DELETE via REST or restart to remove).

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

The `examples/` directory ships six validated configs:

1. `01-hello-flow.yaml` — minimal HTTP → log → file.
2. `02-json-pipeline.yaml` — parse, filter, typed transforms, write.
3. `03-avro-ocf.yaml` — read real `.avro` files, transform, write back zstd-compressed.
4. `04-schema-evolution.yaml` — inline reader schema with type promotion + defaults.
5. `05-schema-registry.yaml` — embedded registry pre-loaded from a `.avsc` file.
6. `06-embedded-schema-registry.yaml` — registry REST API + auto-capture from incoming files.

Validate any of them before deploying:

```bash
for f in examples/*.yaml; do ./build/CaravanFlow validate "$f"; done
```
