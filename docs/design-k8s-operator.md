# Design — zinc-flow k8s operator + multi-worker deployment

> **Status (2026-04-17): PROPOSED.**
> Moves fleet management behind an operator + aggregator split with the
> Flow CRD as the source of truth, and preserves the single-binary
> standalone mode unchanged.

## Scope

zinc-flow must run in three shapes with one HTTP API surface and one
React UI bundle:

| Mode | Who runs it | Source of truth | UI served by |
|---|---|---|---|
| **Single-worker (standalone)** | `./zinc-flow` binary | `config.yaml` on disk | the worker itself |
| **Multi-worker classic** | aggregator pod + N worker pods | aggregator-held canonical `config.yaml` | aggregator |
| **K8s fleet** | operator + aggregator + N worker pods | `Flow` CRD in the cluster | aggregator |

Mode 1 is what ships today. Modes 2 and 3 are the new work. The
design goal is that **the React UI cannot tell which mode it's running
in** — the HTTP contract (`/api/flow`, `/api/processor-stats`,
`/api/cluster`, `/api/identity`, `/metrics`, `/api/provenance/*`,
`/api/vc/*`) is identical. What changes is the binary sitting behind
that contract and how it implements each endpoint.

This doc defines: the contract, the operator + aggregator + headless
worker responsibilities, the CRD schemas, RBAC, rollout semantics,
migration, testing, and a slice-sized phase plan.

---

## Goals and non-goals

### Goals

1. **Feature parity with NiFi 1 clustered mode** — start/stop, config
   validation before Save, status + bulletin on each processor card,
   primary-node-only scheduling, drain/shutdown, rolling config updates.
2. **Declarative fleet management** — a `Flow` CRD captures everything
   needed to stand up a cluster: processors, connections, scheduling,
   replicas, env-sourced secrets. `kubectl apply -f flow.yaml` stands up
   the fleet.
3. **Standalone mode stays a single binary** — no forced dependency on
   k8s or an aggregator. `./zinc-flow` still reads `config.yaml`,
   serves the UI, writes back on Save.
4. **Same UI everywhere** — zero conditional code paths in React keyed
   on deployment mode. Backend adapts; frontend doesn't branch.
5. **GitOps-native** — the `Flow` CRD is a yaml document, checked into
   git, reconciled by ArgoCD / Flux against the cluster. VC moves from
   the worker to the cluster-native gitops layer for fleet mode;
   standalone keeps its worker-side VC provider.
6. **Both worker runtimes supported** — the operator can manage C# and
   Java worker images interchangeably via a `runtime` field on the
   `FlowCluster` CRD. Processors from the registry work identically on
   both (already true at the config level).
7. **Secrets via env only** — the operator materializes secrets as
   `envFrom.secretRef` on the Deployment; never writes them to
   `config.yaml` or ConfigMap. Matches the existing memory rule.

### Non-goals

- **Reinventing NATS-between-pods for inter-instance messaging.** Phase
  3 (NATS) is orthogonal. This design assumes one Flow CRD → one
  Deployment of N identical replicas (horizontal scaling within a
  flow). Cross-flow messaging via NATS is handled by separate
  `ProcessorGroup` CRDs if/when we need it (future phase).
- **Running the operator against non-k8s targets.** The aggregator is
  mode-agnostic, but the operator is k8s-only.
- **Solving multi-tenancy inside one operator.** One operator instance
  per namespace (or cluster-scoped with namespace filtering). RBAC
  scoping is k8s-native.

### Hard constraints (pulled from memory and prior decisions)

- **Config keys are camelCase** on YAML/CRD schemas. No snake_case.
- **Layout is per-user localStorage, never in Flow spec.** The CRD
  stores logical topology only.
- **Secrets never written to disk or CRD.** Env vars only.
- **C# track is golden; Java is first-class.** Both need to work as
  headless workers under the same operator.

---

## Architecture

### Mode 1 — Single-worker (unchanged)

```
┌─────────────────────────────────────────────┐
│  zinc-flow (worker binary)               │
│  ┌─────────────┐  ┌──────────────────────┐  │
│  │  React UI   │  │  HTTP API            │  │
│  │  (wwwroot)  │  │  /api/flow           │  │
│  └─────────────┘  │  /api/processor-stats│  │
│                   │  /api/provenance/*   │  │
│                   │  /api/cluster (new)  │  │
│                   │  /api/identity (new) │  │
│                   │  /api/vc/status      │  │
│                   │  /api/flow/save      │  │
│                   └──────────────────────┘  │
│  ┌──────────────────────────────────────┐   │
│  │  Fabric + Registry + Providers       │   │
│  │  config.yaml (+ overlays)            │   │
│  │  VersionControlProvider (optional)   │   │
│  └──────────────────────────────────────┘   │
└─────────────────────────────────────────────┘
          :9091 (single origin)
```

What changes in standalone: the worker adds `/api/cluster` (returns a
one-element list pointing at itself) and `/api/identity` (returns
`{nodeId, version, configHash, role: "standalone"}`). Everything else
stays exactly as today.

### Mode 3 — K8s fleet (the big one)

```
           ┌───────────────────────────────┐
           │  Browser (React UI)           │
           └─────────────┬─────────────────┘
                         │ HTTPS (Ingress)
           ┌─────────────▼─────────────────┐
           │  zinc-flow-aggregator      │  ◄── serves UI bundle
           │  Deployment, 1-2 replicas     │      + API gateway
           │                               │
           │  GET  /api/flow    (reads CRD)│
           │  PATCH /api/flow   (writes CRD)│
           │  GET  /api/cluster (pod list) │
           │  GET  /api/processor-stats    │  ◄── aggregates from pods
           │  GET  /api/provenance/*       │  ◄── fan-out + merge
           │  GET  /api/identity           │  ◄── cluster-scoped
           │  GET  /metrics                │  ◄── proxies to a pod
           │  POST /api/workers/{pod}/...  │  ◄── drill-down by pod
           └──┬─────────────────────┬──────┘
              │                     │
         ┌────▼─────┐      ┌────────▼─────────────┐
         │ k8s API  │      │  Worker pods (N)      │
         │          │      │  ┌─────────────────┐  │
         │ Flow CRD │      │  │ zinc-flow    │  │
         │ Cluster  │      │  │  (headless mode)│  │
         │   CRD    │      │  │                 │  │
         │ Events   │      │  │  reads ConfigMap│  │
         └────┬─────┘      │  │  serves stats + │  │
              │            │  │    provenance + │  │
              │            │  │    metrics only │  │
              │            │  │                 │  │
              │            │  │  no UI, no VC   │  │
              │            │  └─────────────────┘  │
              │            └───────────────────────┘
              │                     ▲
              │                     │ mounted ConfigMap
              │                     │ + envFrom Secrets
         ┌────▼──────────────────────────────────┐
         │  zinc-flow-operator                │
         │  Deployment, 1 replica (leader-elect) │
         │                                       │
         │  Watches:                             │
         │    Flow CRD (spec + status)           │
         │    FlowCluster CRD                    │
         │  Reconciles:                          │
         │    ConfigMap (flow config)            │
         │    Deployment (worker pods)           │
         │    Service (headless, for stats fan-out)│
         │    ServiceAccount + RBAC              │
         │    PodDisruptionBudget                │
         │  Writes CRD Status:                   │
         │    replica health, config hash,       │
         │    reconcile state, last error        │
         └───────────────────────────────────────┘
```

### Mode 2 — Multi-worker classic (non-k8s)

Same aggregator + headless workers, but the aggregator is configured
with a static peer list (env var or file) instead of discovering via
CRD Status. No operator. The aggregator's `/api/flow` endpoint
fan-writes to each worker's `PUT /api/canonical-config` endpoint (new)
to keep them in sync. VC becomes an aggregator concern instead of a
worker concern.

```
┌──────────────────────┐
│  Browser             │
└──────┬───────────────┘
       │
┌──────▼─────────────────┐
│  aggregator            │
│  static peer list      │
│  writes config via     │
│  fan-out PUT          │
│  holds canonical       │
│    config.yaml + git   │
└──┬───────────┬────────┘
   │           │
┌──▼────┐  ┌───▼───┐
│worker1│  │workerN│
│headless│ │headless│
└────────┘ └────────┘
```

This is useful for docker-compose demos, VM deployments, and anyone
who wants horizontal scaling without k8s overhead. Not the main path,
but supported by the same binaries.

---

## The HTTP contract

The UI depends on this contract. Every mode implements all of it.

### Read endpoints

| Endpoint | Standalone semantics | Fleet/classic semantics |
|---|---|---|
| `GET /api/flow` | reads `config.yaml` off disk | reads `Flow` CRD spec (fleet) / canonical in-memory doc (classic) |
| `GET /api/processor-stats` | own stats | sum of `/api/processor-stats` across live pods |
| `GET /api/registry` | own registry | registry from any one pod (all pods are identical image) |
| `GET /api/provenance?n=N` | own recent events | merge-sorted across pods, newest N overall |
| `GET /api/provenance/{id}` | own lineage | events for that flowfile id from whichever pod has it (fan-out, first non-empty) |
| `GET /api/provenance/failures?n=N` | own failures | merge-sorted across pods |
| `GET /api/overlays` | own overlay layers | canonical overlay layers (from CRD / aggregator state) |
| `GET /api/vc/status` | own git state | cluster-level git state (gitops reconciler health) |
| `GET /metrics` | own Prometheus | aggregator's own metrics + proxy to one representative pod |
| **`GET /api/cluster`** *(new)* | `{nodes: [{id, url, role: "standalone", healthy: true}]}` | `{nodes: [{id, pod, url, role: "worker"|"primary", healthy, configHash, version, uptime}, ...]}` |
| **`GET /api/identity`** *(new)* | self-identity | cluster-level identity (nodeId = flow name, configHash from CRD) |
| **`GET /api/cluster/events`** *(new)* | empty list | recent k8s events on the Flow resource |

### Write endpoints

| Endpoint | Standalone | Fleet | Classic |
|---|---|---|---|
| `POST /api/flow/save` | write `config.yaml` + optional git commit | PATCH Flow CRD spec | update canonical + fan-out to pods |
| `POST /api/processors/add` | mutate `config.yaml` in-memory + atomic swap | stage edit, commit via `/api/flow/save` | same as standalone at the aggregator; pods get synced on save |
| `PUT /api/processors/{name}/config` | same | same | same |
| `DELETE /api/processors/remove` | same | same | same |
| `POST /api/connections` | same | same | same |
| `DELETE /api/connections` | same | same | same |
| `PUT /api/connections/{from}` | same | same | same |
| `PUT /api/entrypoints` | same | same | same |
| `POST /api/processors/enable` | toggle runtime state | sent to all pods (idempotent) | sent to all pods |
| `POST /api/processors/disable` | toggle runtime state | sent to all pods | sent to all pods |
| **`POST /api/cluster/drain`** *(new)* | no-op (returns 200) | asks operator to stop all sources and let the work-queue drain before shutdown | fan-out stop-sources |
| **`POST /api/cluster/start-sources`** *(new)* | no-op | start all sources across all pods | fan-out start-sources |
| **`POST /api/workers/{pod}/stats`** *(new)* | error 404 | proxy to a specific pod (drill-down) | proxy to a specific worker |

The write endpoints fall into two classes:

- **Stageable edits** (mutations to the flow graph shape): in fleet
  mode these stage into a transient "working copy" in the aggregator
  and are only committed to the `Flow` CRD on `/api/flow/save`. This
  lets operators build a multi-step edit and review before apply.
- **Live runtime controls** (enable/disable, drain, start-sources):
  applied immediately across all pods. Not persisted. If the operator
  reconciles, these live toggles get reset — acceptable because
  they're diagnostic/emergency knobs, not persistent config.

---

## CRD schemas

### `Flow` (namespaced)

Represents one flow graph. Has a 1:1 mapping to today's `config.yaml`
under the `flow:` key, with operator-specific extensions grafted on.

```yaml
apiVersion: zincflow.io/v1
kind: Flow
metadata:
  name: my-pipeline
  namespace: data-flows
spec:
  # Reference to the FlowCluster that provides worker settings
  # (image, replicas, scheduling). Optional — if omitted, a default
  # FlowCluster in the same namespace is used.
  clusterRef:
    name: default-cluster

  # ---- Mirrors today's config.yaml `flow:` block ----
  entryPoints:
    - tag-env

  processors:
    tag-env:
      type: UpdateAttribute
      # Pin to a specific processor version if the registry exposes
      # multiple. Omit for "latest of type".
      version: "1.0"
      # Schedule on primary node only (for sources that read a shared
      # PVC, etc). Default false.
      primaryOnly: false
      config:
        key: env
        value: dev
      connections:
        success: [tag-source]

    tag-source:
      type: UpdateAttribute
      config:
        key: source
        value: api
      connections:
        success: [logger]
        failure: [error-sink]

  # ---- Operator-specific extensions ----

  # Values to inject into processor config at reconcile time, sourced
  # from k8s Secrets. Never persisted in the CRD. Workers pick these
  # up via envFrom.
  secretRefs:
    - name: db-credentials   # k8s Secret name
      prefix: DB_             # env var prefix on the Deployment
      # Processors reference env vars as ${env:DB_PASSWORD}, resolved
      # at worker startup by ConfigLoader.

  # Optional pre-apply validation hooks. If any fail, the CRD stays in
  # `PendingValidation` state and pods aren't rolled.
  validation:
    strict: true   # reject unknown processor types, missing config keys
    customRules: []  # reserved for future webhook validators

status:
  phase: Ready | Reconciling | Degraded | PendingValidation | Error
  configHash: "sha256:abc123..."   # hash of the materialized ConfigMap
  replicas:
    desired: 3
    ready: 3
    updated: 3
  pods:
    - name: my-pipeline-worker-0
      phase: Running
      configHash: "sha256:abc123..."
      primaryNode: true
      startedAt: "2026-04-17T21:10:00Z"
    - name: my-pipeline-worker-1
      phase: Running
      configHash: "sha256:abc123..."
      primaryNode: false
      startedAt: "2026-04-17T21:10:02Z"
    - name: my-pipeline-worker-2
      phase: Running
      configHash: "sha256:abc123..."
      primaryNode: false
      startedAt: "2026-04-17T21:10:03Z"
  lastReconciledAt: "2026-04-17T21:10:05Z"
  conditions:
    - type: Ready
      status: "True"
      reason: AllPodsHealthy
      lastTransitionTime: "2026-04-17T21:10:05Z"
    - type: ConfigApplied
      status: "True"
      reason: ConfigMapGeneration=47
```

### `FlowCluster` (namespaced)

Worker runtime settings. Usually one per namespace, referenced by
one or more `Flow` resources.

```yaml
apiVersion: zincflow.io/v1
kind: FlowCluster
metadata:
  name: default-cluster
  namespace: data-flows
spec:
  # Which worker runtime to use. Both are first-class.
  runtime: csharp | java

  # Worker image. Operator provides sensible defaults per runtime.
  image:
    repository: ghcr.io/zincscale/zinc-flow-csharp
    tag: "1.0.0"
    pullPolicy: IfNotPresent

  replicas: 3

  # Primary-node scheduling. When set, the operator designates one pod
  # as primary via a label and serializes primary-only processors to
  # that pod. See "Primary-node handling" section.
  primaryNode:
    enabled: true
    strategy: StableElection   # future: LeaseBased, Random

  resources:
    requests:
      cpu: 500m
      memory: 512Mi
    limits:
      cpu: 2
      memory: 2Gi

  # Env vars injected into every worker pod (non-secret).
  env:
    - name: ZINC_LOG_LEVEL
      value: INFO

  # Optional persistent volume for processors that need durable local
  # state (content store, file watch dirs). Operator creates a PVC
  # per pod.
  storage:
    contentStore:
      enabled: true
      size: 10Gi
      storageClass: fast-ssd

  # Service configuration for the aggregator to reach workers.
  service:
    port: 9091

  # Pod disruption budget settings.
  pdb:
    minAvailable: 1

  # Rolling update strategy when the Flow spec or image changes.
  rollout:
    maxSurge: 1
    maxUnavailable: 0

status:
  observedGeneration: 12
  aggregatedStats:
    processedPerSec: 1250
    errorsPerSec: 2
```

### CRD validation

- **OpenAPI schema validation** at admission time (kubebuilder
  generates from struct tags) — covers type correctness, required
  fields, enum values.
- **Custom validating webhook** for cross-field rules — e.g. every
  connection target must name an existing processor; entry points
  must exist; `primaryOnly: true` requires `FlowCluster.primaryNode.enabled: true`.
- **Reconciler-level validation** for things a webhook can't know
  (processor type exists in the registry, config keys are valid for
  the type). Failures surface in `Status.phase: PendingValidation`
  with per-processor error messages in `Status.conditions`.

---

## Components — deep dive

### `zinc-flow-operator` (new, Go)

Follow the existing TODO/Phase-3b plan: scaffolded with kubebuilder.
Go is chosen here despite the "C# is golden" memory because the k8s
operator ecosystem (controller-runtime, code-generators,
admission-webhooks, testing with envtest) is effectively Go-only. A
C# operator is feasible via Kubernetes.dll but rebuilds infrastructure
that `controller-runtime` gives us for free and conflicts with
upstream tooling conventions. **Memory-compatible interpretation**:
"C# golden" applies to the worker and aggregator (the user-facing
binaries); the operator is an infrastructure component that's better
off following ecosystem defaults.

**Responsibilities:**
- Watch `Flow` and `FlowCluster` resources.
- Reconcile → `ConfigMap` (flow config YAML) + `Deployment` (worker
  pods) + `Service` (headless, for aggregator fan-out) +
  `ServiceAccount` + `Role`/`RoleBinding` + `PodDisruptionBudget`.
- Validate the Flow against the worker's processor registry (by
  probing an existing worker pod or by running an in-operator
  validator built from the same schemas).
- Manage primary-node election (label a pod, update the label on
  pod failure via lease-based election).
- Write `Flow.status` with replica health, config hash, and reconcile
  state.
- Emit k8s Events on significant transitions (ConfigMap updated,
  rolling restart started, validation failed).

**Image:** `ghcr.io/zincscale/zinc-flow-operator:VERSION`.
Shipped via Helm chart (see Packaging).

**Leader election:** standard controller-runtime leader election so
multiple operator pods can run for HA.

### `zinc-flow-aggregator` (new, C#)

C# matches the existing worker stack, AOT posture, and "golden track"
memory. Targets .NET 10 AOT, same toolchain as the worker.

**Responsibilities:**
- Serve the React UI bundle (single origin).
- Expose the full HTTP contract (see above) backed by:
  - k8s API for `Flow` CRD read/write (fleet mode).
  - In-memory canonical doc for classic mode.
  - Fan-out over pods for stats, provenance, runtime toggles.
- Cache pod list from `Flow.status.pods` (fleet) or env/file-based
  peer list (classic). Refresh on CRD watch event or on a 5s timer.
- Circuit-break unresponsive pods: mark unhealthy after 3 failed
  probes; exclude from fan-out reads until next watch-event bump.
- Validate flow edits client-side (same validation the operator runs)
  so Save is blocked before it ever hits the CRD.
- For writes in fleet mode: translate React payload → k8s PATCH on
  the `Flow` resource. Surface admission-webhook errors back to the
  UI verbatim.
- Handle the `/api/vc/*` endpoints by reading gitops state (e.g.
  ArgoCD Application status for the Flow CRD) or returning
  `{enabled: false}` when no gitops reconciler is configured.

**Image:** `ghcr.io/zincscale/zinc-flow-aggregator:VERSION`.
Runs as a `Deployment` with 1-2 replicas behind a `Service` + Ingress.

**Scale:** the aggregator is stateless except for the pod-health
cache. Any replica can serve any request.

**RBAC needed:**
- `get, list, watch, update, patch` on `flows.zincflow.io`
- `get, list, watch` on `flowclusters.zincflow.io`
- `get, list, watch` on `pods` (in the target namespace)
- `get, list, watch` on `events` (for the events panel)
- No write on k8s primitives — the operator owns those.

### Worker (existing `zinc-flow`, needs a headless mode)

**Additions:**
- `--mode=standalone|headless` flag (default `standalone` to preserve
  current behavior). Env var equivalent: `ZINC_MODE`.
- In headless mode:
  - UI is not served. The `wwwroot` MapStaticAssets block is skipped.
  - `VersionControlProvider` is force-disabled.
  - `/api/flow/save` returns `405 Method Not Allowed` (writes go via
    the aggregator → operator → ConfigMap → pod restart).
  - Config is loaded from a mounted ConfigMap path
    (`/etc/zinc-flow/config.yaml`) instead of the working-dir
    default. Set via `--config` flag.
  - Workers watch the ConfigMap mount for changes (k8s updates the
    file on ConfigMap update) and hot-reload if the new shape is
    compatible. If not, they exit cleanly; the Deployment restarts
    them with the new config. Hot reload is best-effort.
- New endpoints **on every worker** (both modes):
  - `GET /api/identity` → `{nodeId, version, configHash, role, uptimeSec}`
  - `GET /api/cluster` → in standalone, one-element self; in headless,
    the operator-populated pod list (optional — aggregator can do this
    too).
- Primary-node awareness: a worker reads an env var
  `ZINC_PRIMARY_NODE=true|false` injected by the operator, and
  processors marked `primaryOnly: true` only register on primary pods.

**Removals in headless mode:**
- `VersionControlProvider`
- `/api/flow/save` as a write operation
- UI serving (wwwroot not mounted)

**Preserved in both modes:**
- All `/api/processors/*`, `/api/connections/*`, `/api/entrypoints`,
  `/api/providers/*` runtime-editing endpoints. In fleet mode these
  are "live" toggles that don't persist — the aggregator uses them
  for enable/disable + start-sources/drain. The operator's next
  reconcile resets them if they drift from spec.
- Provenance, metrics, stats, validation.

### UI (existing `zinc-flow-ui-web`)

**No conditional deployment-mode logic.** The UI reads:
- `GET /api/identity` on boot to learn `role` and display "standalone"
  vs. "fleet: 3 workers" in the header.
- `GET /api/cluster` for the worker drill-down selector (hidden in
  standalone since there's only one node).
- `GET /api/cluster/events` for the k9s-style event panel (new view
  or header popover, TBD).

**New components:**
- Worker selector in the app shell header (hidden if only one node).
- Reconcile banner ("Flow reconciling: 2/3 workers on rev 47") driven
  by `/api/cluster`.
- Cluster events panel (optional new page or drawer).
- Per-node stats breakdown on each processor card in cluster mode
  (mini-bar showing which pods are doing the work).

**Unchanged:**
- Everything else. Graph CRUD, drawers, provenance/errors/lineage/metrics/settings.

---

## Reconciliation flow

A typical fleet-mode edit from the UI:

```
1. Operator edits processor config in the UI.
   → UI calls PUT /api/processors/{name}/config on aggregator.
   → Aggregator stages the edit in the working copy and echoes back.

2. Operator clicks "Save".
   → UI calls POST /api/flow/save.
   → Aggregator validates the working copy.
   → On success, aggregator PATCHes the Flow CRD spec.
   → Response: 200 { configHash: "abc123", reconciling: true }.

3. K8s admission webhook runs on the PATCH.
   → OpenAPI + custom validation webhook.
   → Operator's validating webhook also runs (e.g. processor type
     exists in registry).
   → If any rejects, k8s returns 422; aggregator surfaces to UI.

4. Operator controller sees Flow spec change.
   → Renders new ConfigMap from Flow.spec.
   → Updates ConfigMap resource.
   → Computes new configHash. Patches Flow.status.phase = Reconciling.

5. K8s propagates ConfigMap update to each pod (subPath mount).
   → Worker's file watcher sees new content.
   → If hot-reload compatible: Fabric swaps pipeline atomically.
   → If not: worker exits with clean shutdown; Deployment restarts
     pod with new config.

6. Each worker reports new configHash via /api/identity.
   → Operator's reconciler observes (via watch or poll).
   → Updates Flow.status.pods[*].configHash.
   → Once all pods match: Flow.status.phase = Ready.

7. Aggregator's watch on Flow.status sees configHash transition.
   → Next UI poll of /api/cluster reports all-green.
   → Reconcile banner disappears.
```

**Atomicity:** the CRD PATCH is atomic. If the reconciler fails
mid-rollout (e.g. one pod fails to start with new config), the Flow
stays in `Reconciling` / `Degraded` with specific pod errors in
status. The operator retries per standard controller-runtime policies.
Operators can roll back by editing the CRD spec to the previous value.

**Hot reload compatibility**: adding processors, removing processors,
changing connections, changing config values on existing processors
— all hot-reloadable (the existing atomic pipeline-swap handles this).
Incompatible changes that force a pod restart: changing the worker
image, changing env vars, changing resources. The operator knows
which category a change falls into and picks restart vs. config-reload
accordingly.

---

## Primary-node handling

Some sources must only run on one pod (GetFile on a shared PVC,
CDC-from-database where only one reader can hold the replication slot).
NiFi calls these "Primary Node" processors.

### Election

- Operator maintains a k8s `Lease` resource per Flow.
- One pod holds the lease at any time; acquires on startup if vacant,
  renews every 10s, expires after 30s.
- Operator labels the lease-holding pod with
  `zincflow.io/primary-node: "true"` and sets the env var
  `ZINC_PRIMARY_NODE=true` on that pod via a mutating webhook on
  pod creation — or, simpler, the operator re-creates the pod with
  the env var when primacy changes. **Simpler is: env var is always
  `true` on pod index 0, `false` elsewhere, and StatefulSet-like
  ordinal stability gives us quasi-election.** Reviewing trade-off
  in "Open questions".
- On pod failure, lease expires → operator promotes another pod →
  that pod's `ZINC_PRIMARY_NODE` env var is updated → pod
  restarts cleanly → primary-only processors start running on the
  new primary.

### Worker behavior

- At startup, the worker reads `ZINC_PRIMARY_NODE`. For each
  processor marked `primaryOnly: true` in the flow, it only
  instantiates the processor if primary. Non-primary pods skip those
  processors entirely (including sources — non-primary pods don't
  listen on file watchers, don't poll databases, etc).
- Handoff during primary change takes O(30s) today (lease expiry)
  plus pod restart. Good enough for batch-oriented sources; tighter
  handoff needs a more sophisticated coordinator (out of scope v1).

### UI affordance

- Processor card shows a crown icon when `primaryOnly: true`.
- Cluster view shows a badge on the primary pod.
- Editing a processor to `primaryOnly: true` shows a warning if the
  `FlowCluster` doesn't have `primaryNode.enabled: true`.

---

## Rollout strategy

`FlowCluster.spec.rollout` controls how the operator rolls config or
image changes across pods. Two options:

- **Config-only change** (CRD Flow spec changes, image unchanged):
  operator updates ConfigMap once; ConfigMap propagation is
  effectively simultaneous across pods (k8s propagation is ~60s worst
  case with subPath, 0s with direct mount — operator uses direct
  mount). Each worker hot-reloads independently. No rolling
  restart needed.
- **Image change** (FlowCluster.spec.image changes): standard k8s
  rolling update of the Deployment. `maxSurge` / `maxUnavailable`
  from `FlowCluster.spec.rollout`.

### Draining

Before an image update or operator-initiated shutdown:
- Operator calls `POST /api/cluster/drain` on each pod in sequence.
- Worker stops all sources (no new ingestion), waits for in-flight
  FlowFiles to complete, sets a "drained" flag.
- Worker's `/health` endpoint starts returning `503` so k8s
  readiness probe fails → removed from Service endpoints.
- After grace period (configurable, default 60s), pod receives
  SIGTERM and shuts down.
- Operator then proceeds to next pod.

This gives us zero-data-loss rolling updates for sources that don't
require transactional checkpoint (ListenHTTP clients get 503, retry
hits the next pod). Transactional sources (future NATS, Kafka) use
their native ack/commit semantics — out of scope here.

---

## RBAC

### Operator ServiceAccount

Cluster-scoped (for webhooks + leader election):
```yaml
- apiGroups: ["zincflow.io"]
  resources: ["flows", "flowclusters", "flows/status", "flowclusters/status"]
  verbs: ["get", "list", "watch", "update", "patch"]

- apiGroups: [""]
  resources: ["configmaps", "services", "serviceaccounts", "events"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]

- apiGroups: ["apps"]
  resources: ["deployments"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]

- apiGroups: ["coordination.k8s.io"]
  resources: ["leases"]
  verbs: ["get", "list", "watch", "create", "update", "patch"]

- apiGroups: ["policy"]
  resources: ["poddisruptionbudgets"]
  verbs: ["get", "create", "update", "patch", "delete"]

- apiGroups: ["rbac.authorization.k8s.io"]
  resources: ["roles", "rolebindings"]
  verbs: ["get", "create", "update", "patch"]
```

### Aggregator ServiceAccount (read-mostly)

Namespaced:
```yaml
- apiGroups: ["zincflow.io"]
  resources: ["flows", "flowclusters"]
  verbs: ["get", "list", "watch", "patch"]   # patch for spec edits

- apiGroups: [""]
  resources: ["pods", "events"]
  verbs: ["get", "list", "watch"]
```

### Worker ServiceAccount (nothing)

Workers don't talk to k8s. Empty SA.

---

## Packaging

### Helm chart layout

```
charts/zinc-flow/
├── Chart.yaml
├── values.yaml              # default values: image tags, replicas, RBAC on/off
├── crds/
│   ├── flow.yaml
│   └── flowcluster.yaml
└── templates/
    ├── operator-deployment.yaml
    ├── operator-rbac.yaml
    ├── operator-webhook.yaml
    ├── aggregator-deployment.yaml
    ├── aggregator-service.yaml
    ├── aggregator-ingress.yaml
    ├── aggregator-rbac.yaml
    └── _helpers.tpl
```

`helm install zinc-flow zinc-flow/zinc-flow` installs the
operator + CRDs + aggregator in one shot. Users then apply their own
`Flow` and `FlowCluster` resources.

### Kustomize alternative

`deploy/kustomize/` with bases for operator, aggregator, and
per-namespace examples. Lets GitOps users consume without Helm.

### Image registry

All images published to `ghcr.io/zincscale/`:
- `zinc-flow-csharp:TAG` (existing, worker)
- `zinc-flow-java:TAG` (existing, worker)
- `zinc-flow-aggregator:TAG` (new)
- `zinc-flow-operator:TAG` (new)

Tags: `vX.Y.Z` for releases, `main` for latest commit.

---

## Migration path

### From today (single binary) to standalone with new endpoints

1. Add `/api/identity` + `/api/cluster` to the worker. One-line
   `standalone` role for both. Ships in the next worker release.
2. UI reads `/api/identity` on boot. If role is `standalone`, hide
   the worker selector and cluster events panel. No behavior change
   for users.

This is a backward-compatible worker+UI update, shippable
independently of the fleet work.

### From standalone to multi-worker classic (non-k8s)

1. Deploy aggregator in the same network as workers.
2. Configure aggregator with a peer list (`AGGREGATOR_PEERS=http://w1:9091,http://w2:9091`).
3. Switch workers to headless mode (`ZINC_MODE=headless`,
   `ZINC_CONFIG=/etc/zinc-flow/config.yaml`).
4. Point UI users at the aggregator URL instead of any worker URL.
5. Aggregator holds the canonical `config.yaml` (+ optional git integration).

### From classic to k8s fleet

1. Install operator via Helm: `helm install zinc-flow ...`.
2. Apply `FlowCluster` + `Flow` CRDs matching the existing topology.
   Operator reconciles → Deployment + ConfigMap.
3. Cutover: point UI at the aggregator Service / Ingress; tear down
   the non-k8s aggregator + workers.

### From standalone straight to k8s fleet (most common)

Skip classic. Generate a `Flow` CRD from the existing `config.yaml`
(tool: `zinc-flow migrate-to-crd config.yaml > flow.yaml`). Apply.

---

## Testing strategy

### Operator

- **Unit tests**: reconciler table tests against a fake k8s client
  (controller-runtime's `fake.NewClientBuilder`).
- **envtest**: spin up a real `kube-apiserver` + `etcd` for
  integration tests of admission webhooks + controller loops.
- **E2E**: kind-cluster test matrix covering:
  - Create Flow → Deployment + ConfigMap materialize.
  - Edit Flow → rolling update.
  - Pod failure → reconciler recreates.
  - Primary-node lease handoff on pod kill.
  - Invalid Flow spec → admission webhook rejects.
  - Flow deletion → garbage-collect ConfigMap + Deployment.

### Aggregator

- **Unit tests**: mock k8s client + mock worker responses. Cover
  fan-out merge semantics, circuit-breaker, validation.
- **Integration**: stand up 2 standalone workers behind a real
  aggregator, exercise the full HTTP contract. No k8s needed for
  classic mode.
- **Conformance tests**: a shared test suite that exercises the HTTP
  contract; pointed at (a) a standalone worker, (b) an aggregator in
  classic mode, (c) an aggregator in fleet mode. All three must pass
  the same suite — locks in the "no conditional UI code" invariant.

### Worker (headless mode delta)

- **Unit tests**: ConfigLoader from a mounted path, primary-node
  processor instantiation, /api/identity + /api/cluster shapes.
- **Integration**: docker-compose with one worker in headless mode
  fronted by an aggregator; verify end-to-end edit → reconcile loop.

### UI

- **Visual regression tests** (future): Playwright against all three
  modes to catch deployment-mode-specific UI regressions.
- **Contract tests**: the UI's typed API client runs against a
  recorded contract fixture; catches backend-frontend drift.

---

## Phase breakdown

Each phase is commit-sized, independently shippable, and backward-
compatible with the last.

### Phase A — Contract endpoints on the worker (backward-compat)

Ships immediately, no new deployment shape.

- [ ] `GET /api/identity` on worker. Returns
      `{nodeId, version, configHash, role: "standalone", uptimeSec}`.
      `configHash` = sha256 of the serialized canonical flow config.
- [ ] `GET /api/cluster` on worker. Returns `{nodes: [self]}`.
- [ ] `GET /api/cluster/events` on worker. Returns empty list
      (nothing to report in standalone).
- [ ] UI reads `/api/identity` on boot, stores role in a React
      context, hides cluster-mode UI when `role=standalone`.
- [ ] Tests: worker exposes all three endpoints, UI handles the
      standalone path without regression.

### Phase B — Headless mode on the worker

Worker gains `--mode=headless`; shippable before the aggregator.

- [ ] `--mode=standalone|headless` flag + `ZINC_MODE` env var.
- [ ] In headless: skip wwwroot, disable VersionControlProvider,
      reject `/api/flow/save` with 405.
- [ ] `--config` flag for explicit config path (defaults to
      `./config.yaml` standalone, `/etc/zinc-flow/config.yaml`
      headless).
- [ ] ConfigMap-style file watcher: reload on file change
      (k8s-compatible — ConfigMap updates replace the file atomically
      via symlink swap).
- [ ] Tests: headless worker reads a mounted config file, hot-reloads
      on change, refuses writes, serves stats + provenance.

### Phase C — Aggregator skeleton (classic mode)

First aggregator binary. Enables multi-worker classic deployments.

- [ ] New project: `zinc-flow-aggregator-csharp/`.
- [ ] Serves the React UI bundle (reuse build output from ui-web).
- [ ] Implements read endpoints: `/api/flow` (reads canonical file),
      `/api/processor-stats` (fan-out + sum), `/api/provenance/*`
      (fan-out + merge), `/api/identity` (role=aggregator,
      cluster-scoped), `/api/cluster` (from peer list).
- [ ] Implements write endpoints: `/api/flow/save` (fan-out PUT to
      peers + canonical-file update), runtime toggles fan out to all
      peers.
- [ ] Circuit-breaker for unhealthy peers.
- [ ] Peer list from env var (`AGGREGATOR_PEERS`) or YAML file.
- [ ] Conformance tests pass against aggregator + 2 headless workers.

### Phase D — CRDs + operator skeleton

No aggregator integration yet — this phase is purely getting the
CRDs reconciling against real pods.

- [ ] New repo or dir: `zinc-flow-operator/`. Scaffolded with
      kubebuilder.
- [ ] `Flow` CRD types + generated code.
- [ ] `FlowCluster` CRD types + generated code.
- [ ] Controller: Flow → ConfigMap + Deployment + Service.
- [ ] Controller: FlowCluster → (referenced by Flow; validates
      image, replicas, etc).
- [ ] Status subresource population: phase, configHash, pods.
- [ ] Admission webhook: structural validation (cross-field rules).
- [ ] E2E test on kind: apply Flow → pods come up → edit Flow → pods
      pick up new config.

### Phase E — Aggregator fleet mode

Aggregator learns to read from k8s.

- [ ] `AGGREGATOR_MODE=classic|fleet` env var (or auto-detect via
      `k8s.io/serviceaccount` presence).
- [ ] In fleet mode: peer list comes from `Flow.status.pods` via
      a watch on the target Flow resource.
- [ ] `/api/flow/save` in fleet mode does k8s PATCH instead of file
      write + fan-out.
- [ ] `/api/cluster/events` reads k8s Events on the Flow resource.
- [ ] Conformance tests pass against aggregator in fleet mode.

### Phase F — Primary-node handling

- [ ] Operator manages Lease per Flow.
- [ ] Operator injects `ZINC_PRIMARY_NODE` env var on the primary
      pod.
- [ ] Worker respects `primaryOnly: true` during processor
      instantiation.
- [ ] UI shows crown icon on primary-only processors and primary pod.
- [ ] E2E test: kill primary pod → another pod is promoted → primary-
      only sources resume on the new primary.

### Phase G — Rollout semantics

- [ ] Operator distinguishes config-only change (no restart) from
      image change (rolling restart).
- [ ] `POST /api/cluster/drain` triggers graceful source-stop +
      readiness-probe-503 → SIGTERM.
- [ ] `POST /api/cluster/start-sources` restarts sources after drain.
- [ ] Rolling-restart honors `FlowCluster.spec.rollout` settings.
- [ ] E2E test: image change rolls pods one at a time with no dropped
      in-flight flowfiles (for idempotent sources).

### Phase H — UI polish for cluster view

- [ ] Worker selector in app shell (hidden when `nodes.length === 1`).
- [ ] Reconcile banner driven by Flow status.
- [ ] Per-node stats mini-bars on each processor card.
- [ ] Cluster events panel (new page or drawer).
- [ ] Validation warnings inline on processor cards (driven by the
      same validator the operator runs).
- [ ] Save button disabled when any validator rule fails.

### Phase I — Packaging + docs

- [ ] Helm chart in `charts/zinc-flow/`.
- [ ] Kustomize bases in `deploy/kustomize/`.
- [ ] `zinc-flow migrate-to-crd` CLI subcommand.
- [ ] Getting-started guide for each of the three modes.
- [ ] Operator runbook (common failure modes + recovery).

### Phase J — Gitops integration

- [ ] `/api/vc/status` endpoint in fleet mode reads ArgoCD
      `Application` status (or Flux `Kustomization`) for the Flow
      resource.
- [ ] Example gitops manifests in docs/examples/.

---

## Open questions

1. **Operator language: Go (recommended) vs C#.** Go fits the k8s
   ecosystem natively (kubebuilder, controller-runtime, admission
   webhooks, envtest). C# would match the worker stack but reinvents
   infrastructure. **Proposed: Go**. Dissenting view welcome.

2. **Primary-node election: Lease-based vs StatefulSet ordinal.**
   Lease gives us true election with handoff on failure. Ordinal is
   simpler (pod-0 is always primary) but primary-only sources pause
   for the duration of a pod-0 restart. **Proposed: Lease**, since
   sources that can't tolerate pause are exactly the ones that need
   primary-only scheduling.

3. **Hot reload scope.** How aggressive should the worker be about
   hot-reloading ConfigMap changes vs. restarting the pod? Easier to
   just always restart, which costs 2-5s of downtime per pod during
   rollout. **Proposed: hot-reload topology/config, restart on image
   or env change.** Measure and revisit.

4. **Aggregator HA.** Single replica with k8s Deployment restart
   semantics, or 2+ replicas behind a Service? The aggregator is
   stateless except for a pod-health cache; 2 replicas is trivially
   correct. **Proposed: default 2 replicas in fleet mode, 1 in
   classic.**

5. **ConfigMap size limit (1 MiB).** Large flows with many processors
   and big config payloads could approach this. **Proposed:
   monitor and switch to a sidecar-fetched config from an HTTP
   endpoint on the aggregator if we exceed 512 KiB.** Not a v1 concern.

6. **Admin RBAC on the UI.** Today the UI has no auth. In a multi-
   tenant cluster, who can PATCH a Flow? **Proposed: aggregator
   front-end delegates to k8s RBAC via `SubjectAccessReview`.** v2
   feature.

7. **Cross-Flow references.** If two Flows communicate via NATS (Phase
   3 of the main roadmap), does the operator validate subject/topic
   agreement? **Proposed: defer to Phase 3.** Out of scope for this
   design.

8. **Schema evolution.** When we bump `zincflow.io/v1` → `v1beta2`,
   how do we migrate existing Flow resources? **Proposed: k8s-native
   conversion webhooks, stored version = latest.** Standard k8s
   practice.

---

## Summary

Three deployment shapes, one HTTP contract, one React UI, one rollout
story. Operator is Go + kubebuilder. Aggregator is C# + AOT to match
worker. Worker gains a `headless` mode and two new discovery endpoints,
losing nothing. CRDs are `Flow` (one per flow graph) and `FlowCluster`
(worker runtime settings). Primary-node via Lease. Rolling updates via
standard k8s mechanics with graceful drain hooks. Gitops via ArgoCD /
Flux reconciling the Flow CRD from git; VC provider retires from the
worker in fleet mode but stays in standalone.

The phase breakdown is commit-sized and backward-compatible at each
step. Phase A is shippable in the next worker release; Phase C alone
enables docker-compose multi-worker clusters; Phase D-E unlocks k8s.

Next step: confirm the operator-language choice (Go vs C#) and the
primary-node election strategy, then start Phase A.
