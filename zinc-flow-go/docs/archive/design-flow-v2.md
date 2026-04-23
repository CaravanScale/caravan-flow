# Design: Zinc Flow v2 — Fabric + Sidecar Architecture

> **Status**: DESIGN — supersedes design-flow-runtime.md for production architecture
>
> The original design (v1) compiled processors into a Go binary with channel-based wiring.
> v2 decouples processors from the fabric, enabling instant deployment without rebuild cycles.

---

## The Problem with v1

v1 baked processors into the Go binary. This meant:

- **Adding a processor requires a rebuild and redeploy.** Production can't wait for build cycles.
- **Processors are coupled to the graph.** Wiring is channel pointers, changed by swapping references.
- **No auditability.** No record of what changed, when, or why.
- **No fan-out.** Direct channel wiring is point-to-point.

These are the same problems NiFi solved — but NiFi's solution (monolithic 2GB JVM with all processors baked in) is too heavy. Scalable-NiFi decomposed NiFi into microservices but introduced too many infrastructure dependencies (K8s, ArgoCD, git, Cartographer).

zinc-flow v2 takes the core pattern from scalable-niFi's IRS (Internal Routing Service) and embeds it directly into the runtime, eliminating the external dependency chain.

---

## Architecture: Two Layers

### The Fabric (slow-moving, deliberate lifecycle)

zinc-flow IS the fabric. A single Go binary that provides:

| Component | Purpose |
|-----------|---------|
| **IRS Router** | Rule-based routing with live updates, no restart |
| **Endpoint Registry** | Tracks where each processor lives (HTTP endpoints) |
| **Audit Log** | All graph/rule changes tracked with full history |
| **Content Store** | Large payloads stored by reference |
| **Cross-Cutting Services** | Secrets, lookups, security — shared infrastructure for processors |
| **Stats/Observability** | Metrics collection from all processors |
| **Process Manager** | Start/stop/health-check processor sidecars (local dev mode) |

The fabric changes deliberately. It's infrastructure.

### Processors on the Fabric (fast-moving, instant deploy)

Each processor is a **tiny HTTP server** implementing one endpoint:

```
POST /process
Content-Type: application/json

Request:  FlowFile JSON
Response: ProcessorResult JSON
```

- Any language, any framework
- Deploy independently — no fabric rebuild needed
- Processors call fabric services for cross-cutting concerns (secrets, lookups, content)
- Each processor is a separate process/container with only its own dependencies

This is the scalable-NiFi model (one processor = one deployment) but with the fabric replacing the K8s/ArgoCD/git/Cartographer stack.

---

## The IRS Router

The core differentiator. Derived from scalable-NiFi's IRS backbone.

### How It Works

1. Processor finishes processing a FlowFile, returns result to fabric
2. Fabric evaluates routing rules against FlowFile attributes
3. Rules determine destination(s) — which processor(s) receive the FlowFile next
4. Fabric delivers via the appropriate adapter (HTTP POST to processor endpoint)
5. If no rules match → route to failure/dead-letter queue

**Processors never know their downstream.** They process and emit. The IRS handles all routing.

### What This Enables

| Capability | How |
|------------|-----|
| **Live rerouting** | Update a rule, immediate effect, no restart |
| **Fan-out** | Multiple rules match → FlowFile delivered to multiple processors |
| **A/B testing** | Rule: 10% of traffic → experimental processor |
| **Debugging** | Toggle a logging rule ON, see traffic, toggle OFF |
| **Feature flags** | Enable/disable rules per processor |
| **Auditability** | Every rule change logged with who/when/what |

### Rule Model

Adapted from scalable-NiFi's `EasyRulesEngine`, simplified to pure Go (no MVEL expression language).

```
Ruleset: "order-processing"
  Rule: "high-priority-orders"
    condition: attributes["priority"] == "high" AND attributes["type"] == "order"
    destination: http://enrichment-service:8081/process
    enabled: true

  Rule: "audit-all"
    condition: EXISTS attributes["order_id"]
    destination: http://audit-logger:8082/process
    enabled: true

  Rule: "default"
    condition: true (catch-all)
    destination: http://standard-processing:8083/process
    enabled: true
```

#### Rule Types

```
BaseRule:
  left: String          (attribute name)
  operator: Operator    (EQ, NEQ, GT, LT, CONTAINS, STARTSWITH, ENDSWITH, EXISTS)
  right: Any            (value to compare)

CompositeRule:
  left: Rule
  joiner: AND | OR
  right: Rule
```

Rules are evaluated top-to-bottom within a ruleset. **All matching rules fire** (fan-out), not just the first match. This matches scalable-NiFi's behavior where `getDestinations()` returns a list.

#### Rule Operations (live, no restart)

| Operation | Effect |
|-----------|--------|
| `addOrReplaceRuleset(name, rules)` | Create or replace an entire ruleset |
| `toggleRule(ruleset, ruleName)` | Enable/disable a specific rule |
| `toggleRuleset(ruleset)` | Enable/disable all rules in a ruleset |
| `removeRuleset(ruleset)` | Remove a ruleset entirely |
| `getRules(ruleset)` | Inspect current rules |
| `getDestinations(flowFile)` | Evaluate rules, return matching destinations |

### Thread Safety

Rules stored in concurrent map. Mutations use write lock, reads are lock-free:

```
RulesEngine:
  registry: ConcurrentMap<String, ConcurrentMap<String, RoutingRule>>
  lock: RWMutex

  getDestinations():  read path, no lock (concurrent map is safe for reads)
  addOrReplace():     write path, write lock
  toggle():           write path, write lock
```

Mutations are rare (operator-initiated). Reads happen per-FlowFile at high throughput. The lock contention is effectively zero.

---

## Processor Contract

### FlowFile (JSON)

```json
{
  "id": "ff-1719856200123456789",
  "attributes": {
    "type": "order",
    "source": "api",
    "priority": "high"
  },
  "content": "aGVsbG8gd29ybGQ=",
  "timestamp": 1719856200
}
```

The `content` field is always base64-encoded to handle binary data safely over JSON. Processors decode on receipt, encode on response.

For large content (above configurable threshold), content is stored in the content store and replaced with a reference:

```json
{
  "id": "ff-123",
  "attributes": {
    "content.ref": "store://abc-def-123",
    "content.length": "10485760"
  },
  "content": null,
  "timestamp": 1719856200
}
```

The processor calls `GET /content/{ref}` on the fabric to retrieve the actual content.

### ProcessorResult (response)

```json
{"type": "single",   "flowFile": {...}}
{"type": "multiple", "flowFiles": [{...}, {...}]}
{"type": "dropped",  "reason": "filtered out"}
```

Note: `Routed` is removed from the processor contract. Processors don't decide routing — the IRS does, based on attributes. If a processor wants to influence routing, it sets attributes on the FlowFile (e.g., `attributes["route"] = "high-priority"`), and the IRS rules match on those attributes.

### Error Handling

| HTTP Status | Meaning |
|-------------|---------|
| 200 | Success, result in body |
| 429 | Back-pressure — processor is overloaded, fabric should retry/backoff |
| 500 | Processing error — route FlowFile to DLQ with error metadata |

---

## Cross-Cutting Services

The fabric exposes infrastructure services to processors via HTTP. Processors stay simple — no database drivers, credential management, or state.

### Secrets Provider

```
GET /fabric/secrets/{key}
Response: {"value": "the-secret-value"}
```

Backend: environment variables (Phase 1), Vault/AWS Secrets Manager (Phase 2).

### Lookup Service

```
GET /fabric/lookup/{table}/{key}
Response: {"value": "looked-up-value"}
```

Backend: in-memory map (Phase 1), database/cache (Phase 2). Useful for enrichment processors that need reference data without embedding a database client.

### Content Store

Threshold-based auto-offload with explicit ack for cleanup.

#### How It Works

```
FlowFile arrives at fabric:
  content < threshold (default 256KB) → keep inline as base64 in JSON
  content >= threshold →
    1. Store raw bytes: PUT /fabric/content/{contentId}
    2. Set FlowFile.content = null
    3. Set attributes: content.ref = contentId, content.length = N
    4. Forward lightweight FlowFile JSON (metadata only)

Processor receives FlowFile:
  content != null    → use directly (base64 decode)
  content.ref exists → GET /fabric/content/{ref} to retrieve raw bytes

Fan-out:
  IRS fans FlowFile to 3 destinations → fabric sets ackCount = 3 on contentId

Cleanup:
  Terminal sink finishes → POST /fabric/content/{contentId}/ack
  Fabric decrements ackCount
  ackCount reaches 0 → content deleted from store
```

#### API

```
PUT   /fabric/content/{id}        Body: raw bytes → stores content, returns contentId
GET   /fabric/content/{id}        → returns raw bytes
POST  /fabric/content/{id}/ack    → decrements ack count, deletes when 0
```

#### Content Mutability

When a processor modifies large content, it writes new content to the store (copy-on-write). The old contentId remains valid for other fan-out branches. The processor returns a FlowFile with `content.ref` pointing to the new contentId.

#### Storage Backend

Phase 1: local filesystem with hashed subdirectories (`store/{id[0:2]}/{id[2:4]}/{id}`) for performance at scale.
Phase 2: S3 with configurable bucket.

#### Safety Net

TTL-based expiry as a fallback GC — content older than a configurable max age (default 24h) is deleted regardless of ack count, to prevent leaks from crashed processors that never ack.

### Health / Registration

```
POST /fabric/register
Body: {"name": "my-processor", "endpoint": "http://localhost:8081", "type": "enrichment"}

GET  /fabric/health
Response: {"status": "ok", "processors": [...], "stats": {...}}
```

---

## Delivery Adapters

Same pattern as scalable-NiFi's `CommAdapter` interface. The router uses the appropriate adapter based on destination type.

```
interface DeliveryAdapter:
  fn deliver(destination: String, flowFile: FlowFile): DeliveryResult
  fn healthCheck(destination: String): bool
```

| Adapter | Transport | Use Case |
|---------|-----------|----------|
| **HttpAdapter** | HTTP POST | Primary — processor sidecars |
| **ChannelAdapter** | Go channel | Built-in fabric processors (log, metrics) |
| **NatsAdapter** | NATS JetStream | Cross-pod routing (Phase 2) |

The adapter is determined by the destination URL scheme:
- `http://...` → HttpAdapter
- `channel://...` → ChannelAdapter
- `nats://...` → NatsAdapter

---

## Deployment Modes

### Local Dev

```
zinc-flow run --config pipeline.yaml
```

The fabric starts processor binaries as child processes, manages their lifecycle, and routes between them. Everything runs on one machine. Good for development and testing.

```
pipeline.yaml:
  processors:
    - name: enrich
      binary: ./processors/enrich
      port: 8081
    - name: sink
      binary: ./processors/sink
      port: 8082

  rules:
    - ruleset: main
      rules:
        - name: all-to-enrich
          condition: true
          destination: http://localhost:8081/process
        - name: enriched-to-sink
          condition: EXISTS attributes["enriched"]
          destination: http://localhost:8082/process
```

### Production

Processors are independently deployed (containers, systemd, etc.). The fabric only knows their HTTP endpoints via registration or config.

```
                    ┌─────────────────────┐
                    │   zinc-flow fabric   │
                    │                      │
                    │  ┌──────────────┐    │
   HTTP POST        │  │  IRS Router  │    │   HTTP POST
  ──────────────────>│  │  (rules)     │────────────────────>  processor-a:8081
                    │  └──────────────┘    │
  FlowFile in       │  ┌──────────────┐    │   FlowFile out
                    │  │  Audit Log   │    │
                    │  └──────────────┘    │
                    │  ┌──────────────┐    │   HTTP POST
                    │  │  Content     │────────────────────>  processor-b:8082
                    │  │  Store       │    │
                    │  └──────────────┘    │
                    └─────────────────────┘
```

The fabric is one process. Processors are separate processes. Communication is HTTP.

---

## Audit Model

Every routing change is logged:

```json
{
  "timestamp": "2026-04-02T10:15:30Z",
  "actor": "admin@example.com",
  "action": "toggle_rule",
  "ruleset": "order-processing",
  "rule": "high-priority-orders",
  "before": {"enabled": true},
  "after": {"enabled": false}
}
```

| Field | Purpose |
|-------|---------|
| `timestamp` | When the change happened |
| `actor` | Who made the change (API caller) |
| `action` | What type of change (add_ruleset, toggle_rule, remove_ruleset, register_processor, etc.) |
| `ruleset` / `rule` | What was changed |
| `before` / `after` | State diff |

Phase 1: append-only JSON file (`audit.jsonl`)
Phase 2: embedded store (SQLite or BoltDB) with query API

---

## Failure Handling

### No Rules Matched

When the IRS evaluates a FlowFile and no rules match, the FlowFile is routed to the **failure queue** (equivalent to scalable-NiFi's `defaultRoutingFailureQueue`). This is a configurable destination — by default, logged and stored for inspection.

### Processor Error (HTTP 500)

FlowFile routed to DLQ with error metadata attached:

```json
{
  "attributes": {
    "error.processor": "enrich",
    "error.message": "connection timeout",
    "error.timestamp": "2026-04-02T10:15:30Z"
  }
}
```

### Processor Unreachable

Health check fails → mark processor as unhealthy → stop routing to it → alert. FlowFiles queue in the fabric until the processor recovers or is replaced.

### Back-Pressure (HTTP 429)

Fabric backs off with exponential retry. FlowFiles buffer in the fabric's internal queue (bounded). If queue fills → back-pressure propagates upstream.

---

## Scalable-NiFi Patterns: Kept vs Simplified

| Pattern | Scalable-NiFi | zinc-flow v2 |
|---------|--------------|-------------|
| **IRS Router** | External Spring Boot service (IRS Backbone) | Embedded in fabric binary |
| **Rules Engine** | EasyRules + MVEL expressions | Pure Go, attribute-based predicates |
| **Transport** | SQS queues + REST adapters | HTTP POST (primary), NATS (Phase 2) |
| **Topology Management** | Cartographer + git + ArgoCD | Config file + management API |
| **Auditability** | Git commits + K8s informers + Elasticsearch | Append-only audit log |
| **Service Registry** | Separate microservice | Embedded endpoint registry |
| **Content Store** | Storage Gateway (S3-backed) | Embedded, filesystem (Phase 1), S3 (Phase 2) |
| **Deployment** | K8s Deployments per processor | Any process manager (K8s, docker, systemd, local) |

**What we keep**: IRS routing model, rule-based evaluation, fan-out, live toggle, adapter pattern, failure queue, content-by-reference for large payloads.

**What we simplify**: No external dependencies for core functionality. The fabric is self-contained. Processors are plain HTTP servers.

---

## What This Means for Existing Code

The existing zinc-flow code in `src/` was built for the v1 compiled-in-processor model. The v2 architecture shifts fundamentally:

| v1 Concept | v2 Equivalent |
|------------|---------------|
| `ProcessorFn` interface (in-process) | HTTP `POST /process` contract |
| `ProcessorWorker` (goroutine + channel) | HTTP client in fabric calling processor endpoints |
| `ProcessorGroup.connect()` (channel wiring) | IRS rule configuration |
| `ProcessorResult.Routed` | Processor sets attributes, IRS rules match on them |
| Direct channel back-pressure | HTTP 429 + fabric-side bounded queue |

The `core/flowfile.zn` and `core/result.zn` data models translate directly to JSON. The processing model (FlowFile in → ProcessorResult out) is unchanged — only the transport changes from channels to HTTP.

---

## Implementation Phases

### Phase 1 — Fabric MVP

The fabric with embedded IRS, HTTP processor delivery, audit logging.

- [ ] IRS Router: rule model, evaluation engine, concurrent registry
- [ ] Management API: CRUD rules, register/deregister processors
- [ ] HTTP delivery adapter: POST FlowFiles to processor endpoints
- [ ] Audit log: append-only JSONL file
- [ ] Content store: local filesystem with size-threshold reference
- [ ] Process manager: start/stop processor binaries (local dev mode)
- [ ] Health checks: periodic GET to processor health endpoints
- [ ] Failure queue: DLQ for unmatched/errored FlowFiles
- [ ] Example processors: add-attribute, log, file-sink as standalone HTTP servers

### Phase 2 — Production Hardening

- [ ] Cross-cutting services: secrets provider, lookup service
- [ ] NATS delivery adapter for cross-pod routing
- [ ] Audit query API (embedded SQLite/BoltDB)
- [ ] Back-pressure: HTTP 429 handling, bounded internal queues
- [ ] Metrics endpoint (Prometheus)
- [ ] Pipeline config file (YAML) with validation

### Phase 3 — Cloud Native

- [ ] K8s operator for processor deployments
- [ ] Auto-scaling based on queue depth / processor latency
- [ ] Web UI for rule management and graph visualization
- [ ] OpenTelemetry tracing through FlowFile attributes

---

## Open Design Questions

1. **Back-pressure model** — HTTP 429 from processors is clear, but how does the fabric itself signal back-pressure to sources (HTTP source, file watcher, etc.)? Bounded internal queue + stop consuming from source when full?

2. **Processor discovery** — self-registration on startup (`POST /fabric/register`) vs config file vs both? Config is simpler for local dev, registration is more dynamic for production.

3. **Built-in processors** — should the fabric ship with any compiled-in processors (using ChannelAdapter for performance)? Or should everything be HTTP for consistency?

4. **Ordering guarantees** — HTTP is request-response, naturally ordered per processor. But fan-out means a FlowFile can be delivered to multiple processors concurrently. Is ordering across fan-out branches needed?

5. **Ruleset scoping** — scalable-NiFi scoped rulesets by `objectMetaDatatype`. What's the equivalent scoping key for zinc-flow? Per-source? Per-pipeline? Global?

## Resolved Design Decisions

- **Content encoding**: Always base64 for inline content (handles binary data safely over JSON)
- **Content store strategy**: Threshold-based auto-offload (default 256KB). Below threshold: inline base64. Above: stored by reference.
- **Content GC**: Explicit ack from terminal processors. Fabric tracks ack count per contentId (set during fan-out). TTL fallback for safety.
- **Content mutability**: Copy-on-write. Modified content gets new contentId. Old contentId stays valid for other fan-out branches.
