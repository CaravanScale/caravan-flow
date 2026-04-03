# zinc-flow Architecture v2

## Vision

zinc-flow is a data flow engine that scales from a single binary on a laptop to a distributed multi-group deployment on Kubernetes. The core engine is always the same -- fast, in-process, zero external deps. External messaging and orchestration are additive layers, never required.

## Deployment Modes

### Mode 1: Standalone (current)
```
zinc run .
```
- Single process, single group
- Config from local YAML file
- HTTP ingress, in-process routing
- Zero dependencies beyond the binary
- Use cases: dev, testing, edge, simple pipelines

### Mode 2: Multi-group with connectors
```
# Terminal 1: Group A
zinc run . --config group-a.yaml

# Terminal 2: Group B
zinc run . --config group-b.yaml
```
- Multiple zinc-flow processes, each running a group
- Connected via connector processors (PutNats -> GetNats, PutKafka -> GetKafka)
- Each group is independently configured
- No central coordinator -- groups are loosely coupled via message transport
- Use cases: multi-team pipelines, staging environments, performance isolation

### Mode 3: Kubernetes-managed
```
kubectl apply -f flow.yaml
```
- K8s operator deploys and manages groups as pods
- ProcessorGroup CRDs define the topology
- Flow CRD defines the global wiring (which groups connect to which)
- Operator handles scaling, rolling updates, health checks
- K8s etcd is the state store, no additional infrastructure
- Use cases: production, enterprise, auto-scaling

## Core Principle

The core engine is always self-contained. NATS/Kafka/SQS are connector processors (just ProcessorFn implementations), not infrastructure dependencies. Within a group: synchronous in-process. Between groups: connector processors.

## Data Flow

### Within a group (in-process, synchronous)
```
HTTP POST -> HttpSource -> Fabric.process(ff)
  -> tag-env.process(ff) -> logger.process(ff) -> sink.process(ff)
```

### Between groups (via connector processors)
```
Group A:
  tag-env -> logger -> PutNats("orders.processed")
                       ^ just a regular processor

Group B:
  GetNats("orders.processed") -> json-to-records -> sink
  ^ just another source, like HttpSource
```

### Backpressure chain
```
Group B slow (sink I/O)
  -> Group B bounded queue fills
  -> GetNats consumer stops acking
  -> NATS MaxAckPending reached
  -> Messages buffer in NATS stream
  -> PutNats in Group A gets publish error
  -> Group A returns Failure -> DLQ
  -> HTTP gets 503 or queued
```

## Kubernetes Operator (separate repo: zinc-flow-operator)

### ProcessorGroup CRD
```yaml
apiVersion: zincflow.io/v1
kind: ProcessorGroup
metadata:
  name: ingest
spec:
  image: zincscale/zinc-flow:latest
  replicas: 2

  providers:
    - name: nats-main
      type: nats
      config:
        url: nats://nats.default.svc:4222

  processors:
    - name: tag-env
      type: add-attribute
      config:
        key: env
        value: prod
    - name: nats-out
      type: put-nats
      config:
        provider: nats-main
        subject: orders.tagged

  sources:
    - name: http-in
      type: get-http
      config:
        port: "9091"

  routes:
    - name: tag-all
      condition:
        attribute: type
        operator: EXISTS
      destination: tag-env
    - name: publish
      condition:
        attribute: env
        operator: EXISTS
      destination: nats-out

  backpressure:
    queueDepth: 1000

status:
  ready: true
  replicas: 2
  processed: 15420
  failures: 3
```

### Flow CRD (global topology)
```yaml
apiVersion: zincflow.io/v1
kind: Flow
metadata:
  name: order-pipeline
spec:
  groups:
    - name: ingest
      ref: ingest
    - name: enrichment
      ref: enrichment
    - name: delivery
      ref: delivery

  connections:
    - from: ingest
      to: enrichment
      transport: nats
      subject: orders.tagged
    - from: enrichment
      to: delivery
      transport: nats
      subject: orders.enriched

  nats:
    url: nats://nats.default.svc:4222

status:
  groups:
    - name: ingest
      ready: true
      processed: 15420
    - name: enrichment
      ready: true
      processed: 15418
```

### Operator Responsibilities
- Reconcile ProcessorGroup: generate config.yaml ConfigMap, create Deployment + Service
- Reconcile Flow: validate group references, ensure connector subject matching, aggregate stats
- Scaling: adjust replicas, NATS queue groups handle load balancing
- Rolling updates: drain connectors before shutdown, no message loss

## Project Structure

```
ZincScale/
  zinc/                 # Zinc language compiler
  zinc-flow/            # Core engine + connector processors
    src/
      core/             # FlowFile, Content, Provider, ProcessorContext
      fabric/           # Runtime, Router, API, Sources
      processors/
        builtin.zn      # AddAttribute, Log, FileSink, JsonToRecords, RecordsToJson
        connectors/     # PutNats, GetNats, PutKafka, GetKafka, ...
    docs/
    test/
  zinc-flow-operator/   # K8s operator (Go, kubebuilder)
    api/v1/             # CRD types
    controllers/        # Reconcilers
```

## Implementation Phases

### Phase 2a: Provider framework (zinc-flow)
Provider interface, ProcessorContext, ProviderRegistry, config hierarchy, update all processors.

### Phase 2b: Connector processors (zinc-flow)
PutNats/GetNats, bounded ingress queue (backpressure), connector lifecycle.

### Phase 2c: K8s operator (zinc-flow-operator)
ProcessorGroup CRD, Flow CRD, config generation, deployment management.

### Phase 2d: Production hardening
NATS auth, retry policies, graceful shutdown, Prometheus metrics.
