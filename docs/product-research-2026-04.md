# zinc-flow — non-gov product research (April 2026 snapshot)

## Context

zinc-flow was built with a gov / NiFi-replacement shape in mind (IRS-style routing, airgapped schema registry, V3 FlowFile interop, provenance-grade compliance story). Now looking outward: what commercial product shapes can this engine power, outside government use? This file captures the market research so we can revisit and make a direction call without re-doing the survey.

**Not an implementation plan.** The deliverable here is a ranked list of product concepts with tradeoffs + a recommended direction. Once a direction is chosen, a separate plan and commit supersede this snapshot.

> **Update (late April 2026):** the runtime mix has changed since this was drafted — Go and Python 3.14t were retired and a Crystal 1.20 track was added. The product concepts below (notably Concept 1 "zinc-edge" which assumed the Go binary, and Concept 3 "zinc-ml" which assumed the Python runtime) would need re-grounding against the current C# / Java / Crystal set before being pursued. The market-landscape analysis is otherwise unchanged.

## What zinc-flow is today (snapshot)

Three runtimes — **C# .NET 10 AOT (~27 MB, 2M+ ff/s, zero-GC steady-state)** is the gold reference, **Java (JVM, ServiceLoader + overlay plugin model)** is the enterprise-extensibility compromise, and **Crystal 1.20 static (`preview_mt`, ~7 MB)** is the compact-binary track — it recently reached full processor parity with the C# reference (32 processors) via a compile-time macro registry plus sibling `crystal-avro` and `crystal-jsonpath` shards. (Earlier Go and Python 3.14t tracks from the prior pivot have been retired.)

Built today (csharp):
- 24 processors across attribute/text/record/Avro/routing/expression
- 3 source connectors (ListenHTTP, GetFile, GenerateFlowFile)
- FlowFile V3 round-trip (NiFi interop)
- Avro OCF read/write + deflate/zstandard codecs + schema evolution
- Embedded schema registry (airgapped, Confluent-shape REST)
- Typed expression engine (15 operators + 22 functions)
- Hot reload with atomic graph swap
- Provenance tracking (ring buffer, per-FlowFile lineage)
- ScopedContext isolation (processors only see declared providers)
- Prometheus `/metrics`, structured JSON logging
- `zinc-flow validate` CLI (pre-flight config check)
- HTTP management API (15 read + 9 mutation endpoints)
- Read-only dashboard.html at `/`
- 965 tests green under both JIT and AOT, zero analyzer warnings

Not built (designed or deferred):
- NATS multi-instance, K8s operator — Phase 3
- Kafka / SQS / S3 / Postgres / ES connectors — Phase 4
- RBAC + audit + multi-tenant — Phase 5
- Snappy codec, Confluent wire format, Parquet — deferred until user need

## Commercial landscape (April 2026)

**Segments and who plays:**

| Segment | Players | Ticket size | Shape |
|---|---|---|---|
| **Enterprise iPaaS** | SnapLogic, StreamSets, Talend, Boomi, Azure Data Factory, AWS Glue, IBM DataStage | $100K-$1M+/yr | Cloud-centric, visual, managed |
| **Embedded iPaaS (OEM for SaaS)** | Nango (700+ APIs), Ampersand, Cyclr, Put It Forward | Per-customer/seat | Library + framework for SaaS products |
| **Cloud ETL/ELT** | Airbyte (600+ connectors), Fivetran, Hevo | SaaS subscription | Connector-library focused |
| **Edge / log pipelines** | Fluent Bit (450KB!), Vector, Benthos, Fluentd | OSS + commercial support | Minimal footprint, log-focused |
| **Workflow orchestrators** | Airflow, Prefect, Dagster | OSS + managed | Task DAG, not data-flow |
| **Regulated / airgapped** | Apache NiFi (still dominant), SentinelOne AI Data Pipeline (new) | Self-hosted | Compliance + provenance focus |

**Market size signals:**
- Data integration: $30.27B by 2030
- Streaming analytics: $128.4B by 2030
- Edge computing: $21.4B → $28.5B growth 2025→2026 (28% CAGR)

**Where NiFi is losing:**
- Visual designer becomes a bottleneck at millions of events/sec
- JVM tuning + cluster ops are day-2 burden
- Cloud-native / K8s-native teams avoid it
- Scaling past single-node requires ZooKeeper + primary/secondary dance
- Ecosystem skews older-enterprise, not cloud-native

**Where NiFi still wins:**
- Provenance / audit trail is industry-standard
- 200+ processors (vs zinc-flow's 24)
- Mature visual authoring (biggest moat)
- Regulated-industry trust (HIPAA, GDPR, SOC 2 out of box)

## Use case clusters outside gov

### A. Regulated-industry NiFi replacement (healthcare, finance, defense contractors)

Hospitals doing HL7/FHIR routing. Banks doing trade reconciliation + CDC. Pharma R&D pipelines. Defense primes (Lockheed, Raytheon) that need airgapped but aren't the government itself.

**Why zinc-flow fits:** airgapped schema registry built-in, provenance tracking, ScopedContext isolation, zero external deps, tiny binary, FlowFile V3 interop lets it coexist with existing NiFi during migration.

**What's missing:** fewer connectors (no HL7, FHIR, Kafka, SQL CDC yet), no visual UI.

### B. Edge / IoT data collection

Manufacturing sensors, logistics telemetry, retail store IoT, industrial control systems. Not just logs (Fluent Bit's turf) but structured data with routing + schema awareness.

**Why zinc-flow fits:** 11 MB Go binary, no JVM, runs on Raspberry Pi / ARM64 edge boxes, handles structured data (Avro/JSON/CSV) not just syslog.

**What's missing:** MQTT source, OPC-UA source, store-and-forward durability for intermittent connectivity, time-series output.

### C. Embedded data-flow engine for SaaS products (OEM)

Observability SaaS that needs an in-agent pipeline. AI product needing a retrieval pipeline. Data science platform needing feature prep. Nango/Ampersand do **external-API integration** for SaaS; zinc-flow's angle is **internal data transformation** within the SaaS product.

**Why zinc-flow fits:** single static binary, YAML config, hot reload, multi-runtime (ship whichever matches host platform), AOT for minimal attack surface.

**What's missing:** embedding SDK (right now you run a process, not a library), multi-tenancy, per-pipeline resource limits.

### D. AI/ML data prep sidecar (python runtime differentiator)

RAG ingestion pipelines, training-data preparation, feature stores, A/B event routing for ML experiments. The Python runtime's pandas/numpy/sklearn integration is unique — no other data-flow engine ships with first-class DataFrame processors.

**Why zinc-flow fits:** Python 3.14t free-threading, pandas/numpy/sklearn native, 14 MB native binary via Cython, composable processor pattern lets data scientists chain transforms declaratively.

**What's missing:** Python runtime itself is Phase 1 (queue-based, old processor names). Needs the csharp-parity catch-up work first (~1 week per `zinc-flow-python/docs/csharp-parity-plan.md`).

### E. Content / media workflows

Publishing pipelines (image processing, video transcoding triggers, CDN invalidation), e-commerce catalog ingestion, content moderation. Historically NiFi-heavy in broadcast and publishing.

**Why zinc-flow fits:** FlowFile model handles large binary payloads well (content offload to disk claims above 256 KB), routing is first-class.

**What's missing:** media-specific processors (ffmpeg wrapper, image transforms), CDN/webhook processors, workflow semantics (scheduled + event-triggered hybrid).

### F. Developer tooling / local data movement

`zinc-flow` as the `make` or `jq` of data pipelines — a local CLI that reads YAML and runs a pipeline, useful in CI/CD, scripts, local dev environments. Low-ticket / OSS / developer-love play.

**Why zinc-flow fits:** zero-dep binary, YAML, validate subcommand already exists, fast startup.

**What's missing:** package-manager distribution (homebrew, apt, winget), examples library, cookbook docs, stdin/stdout piping mode.

## Product concepts — ranked

### Concept 1: zinc-edge — edge/IoT data pipeline OSS + commercial

**Shape:** Open-source Go binary rebranded for edge. Commercial offerings: support contracts, fleet UI for managing 1000s of edge nodes, custom processor development, hardened distributions for specific ICS/OT platforms.

**Target:** Manufacturing IoT, logistics telemetry, industrial SCADA, retail sensor fleets. Companies with a "many small sites, ship data to central" shape that need more than Fluent Bit but less than NiFi.

**Competitors:** Fluent Bit (simpler, log-only), HiveMQ Edge, EdgeX Foundry, Azure IoT Edge, AWS Greengrass.

**Investment to reach MVP:** ~3 weeks. Add MQTT source + store-and-forward buffer + OTel export. Existing Go runtime handles the rest.

**Moneymaker:** fleet-management UI + commercial support. Classic Red-Hat-style OSS business.

**Risk:** Fluent Bit is deeply entrenched for logs. Differentiation is "structured data + routing + schema awareness," which requires customer education.

### Concept 2: zinc-embed — embedded data-flow library for SaaS OEMs

**Shape:** Library licensable to SaaS companies that need an internal data-processing pipeline inside their product. Shipped as static lib (Go), NuGet (C#), or wheel (Python). Per-instance license fees or revenue-share model.

**Target:** Observability SaaS (agent-side pipelines), AI product vendors (RAG ingestion, feature prep), data platform SaaS (internal ETL). Companies currently building their own pipeline layer or wrapping Apache Beam/Kafka Streams.

**Competitors:** Nango/Ampersand/Cyclr (but they do external-API integration, different problem). Apache Camel is the closest analog — but Camel is JVM-only.

**Investment to reach MVP:** ~4-6 weeks. Requires embedding SDK (C API for Go runtime, NuGet package for C# with clean types, wheel for Python), multi-tenancy (per-pipeline resource limits, isolated storage), commercial license terms.

**Moneymaker:** license fees per SaaS customer. B2B enterprise sales.

**Risk:** Slow sales cycle (6-12 months). Needs case-study customers first.

### Concept 3: zinc-ml — AI/ML data prep platform (python-runtime-led)

**Shape:** Build the Python runtime out first, position as "pandas-native data flow." Ship as OSS core + managed SaaS for teams that don't want to self-host. Integrate deeply with Weights&Biases / MLflow / Feast / LangChain.

**Target:** AI startups building RAG pipelines, ML teams at mid-market companies, feature-engineering shops. Anyone currently duct-taping Airflow + pandas + Ray.

**Competitors:** Prefect (workflow orchestrator, not pipeline), Dagster (asset-based, data-aware), Kestra, Metaflow (Netflix OSS).

**Investment to reach MVP:** ~2 weeks for python parity + 1 week for ML-specific processors (vector store writers, embedding models, dataset splitters) + 2 weeks for cloud SaaS shell. Total ~5 weeks.

**Moneymaker:** managed SaaS pricing per pipeline-hour or per-dataset. Could also be acquihire bait for an established ML platform.

**Risk:** Crowded space with deep-pocketed competitors. Needs strong opinion on what makes zinc-flow better than Dagster for ML.

### Concept 4: zinc-healthcare — FHIR/HL7 gateway for mid-market

**Shape:** Vertical-focused distribution. zinc-flow + HL7 v2 parser + FHIR REST + EHR integration connectors + HIPAA-shaped audit log + pre-packaged pipelines for common clinical workflows (lab results, ADT, orders). Self-hosted appliance model or hosted HITRUST-certified SaaS.

**Target:** Mid-market hospitals and clinics that can't afford Mirth Connect / Cloverleaf enterprise pricing. Health-tech startups building EHR integrations.

**Competitors:** Mirth Connect (NextGen), Cloverleaf (Infor), Rhapsody, Redox (SaaS).

**Investment to reach MVP:** ~6-8 weeks. HL7 parser + FHIR client are the big builds. Audit log + HITRUST cert are ongoing compliance work.

**Moneymaker:** appliance licenses + support. Healthcare vertical has sticky, high-value contracts.

**Risk:** Healthcare sales cycles are long (12-18 months). Compliance cert (HITRUST, SOC 2) costs $100-200K before first dollar.

### Concept 5: zinc-cli — developer tooling play

**Shape:** Position zinc-flow as "`make` for data pipelines." OSS-only, developer love, maybe GitHub sponsors / consulting revenue. Focus on local dev UX, stdin/stdout mode, package-manager distribution, a cookbook site.

**Target:** Data engineers building local workflows, CI/CD pipelines that need data processing, side-project use.

**Competitors:** `jq`, `miller`, `dsq`, Benthos as CLI, Dagster for local dev.

**Investment to reach MVP:** ~1-2 weeks. Mostly polish + distribution + docs.

**Moneymaker:** weak — OSS with no clear commercial path. Useful as brand-building for one of the other concepts.

**Risk:** No path to revenue on its own.

## Recommendation

Two most credible:

1. **zinc-edge (Concept 1)** — clear market, existing Go binary is already close, differentiation is defensible (structured-data edge routing, not logs), $28B market growing 28%. Edge/IoT buyers are used to paying for commercial support on OSS.

2. **zinc-embed (Concept 2)** — highest revenue-per-customer ceiling, fits the "invisible infrastructure" thesis that zinc-flow's design already supports (single binary, zero deps, AOT, multi-runtime). But slower to first revenue.

**Suggested path:**
- Start with zinc-edge as the public story (OSS + commercial support, brand-building, developer community). Lower risk, faster feedback.
- Use zinc-edge customer relationships to learn which verticals need zinc-embed-style embedding. Convert selectively.
- zinc-healthcare / zinc-ml are follow-ups once a first commercial motion exists; both have specialized sales cycles that need a base business to sustain them.

**Anti-recommendation:** don't pursue zinc-cli as primary. It's a good side-effect of any of the above, not a standalone product.

## Next steps if a direction is chosen

Regardless of concept, the immediate work is the same:

1. **Complete csharp Phase 2f Phase 0** (worker API prep) — unlocks fleet UI for all concepts
2. **Decide whether fleet UI is built before or after picking a vertical** — affects marketing story
3. **Choose 1-2 connectors that unblock the target concept** (MQTT for zinc-edge, embedding SDK for zinc-embed, HL7 for zinc-healthcare)
4. **Write a marketing page + example pipeline repo** for the chosen concept — sells the story before heavy eng investment

Decision pending: **which concept to pursue** (or pass on all and keep building features without a specific commercial story).

## What's NOT in this research

- Pricing analysis (ticket-size ranges were estimates from public info; no primary research)
- Customer interviews (zero conversations with potential buyers)
- Competitive pricing on NiFi support contracts (Cloudera's numbers weren't public)
- Fundraising angle (whether any of these shapes are VC-scale)
- Patent / IP landscape around data-flow primitives

Revisit these if a direction is chosen.

## Sources

Market research pulled from:
- [Apache NiFi Alternatives 2026 — 5X](https://www.5x.co/blogs/apache-nifi-alternatives)
- [Top 10 Apache NiFi Alternatives & Competitors in 2026 — G2](https://www.g2.com/products/apache-nifi/competitors/alternatives)
- [Fluent Bit — cloud-native log processor](https://fluentbit.io/)
- [Edge Computing 2026 — STL Partners 50 companies](https://stlpartners.com/articles/edge-computing/50-edge-computing-companies-to-watch-in-2026/)
- [Best Embedded iPaaS Solutions 2026 — Pandium](https://www.pandium.com/blogs/best-embedded-ipaas-solutions-complete-guide)
- [Best embedded iPaaS for scalability 2026 — Nango Blog](https://nango.dev/blog/best-embedded-ipaas-for-scalability-and-flexibility/)
- [Apache NiFi Data Ingestion for Regulated Industries — Datavid](https://datavid.com/blog/apache-nifi-data-ingestion-for-regulated-industries)
- [Apache NiFi Provenance, Auditing, Compliance — dfmanager](https://www.dfmanager.com/blog/apache-nifi-provenance-auditing-compliance)
- [8 AI and Data Trends in Financial Services 2026 — Databricks](https://www.databricks.com/blog/8-ai-and-data-trends-shaping-financial-services-2026)
