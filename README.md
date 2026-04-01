# Zinc Flow

Lightweight, cloud-native data flow engine inspired by Apache NiFi. Built in [Zinc](https://github.com/ZincScale/zinc) to dogfood the zinc-go transpiler.

## What is it?

NiFi's processor model — with Zinc's simplicity, cloud-native from day one.

- A **processor** is a Zinc class implementing `ProcessorFn`
- A **pipeline** connects processors with typed channels
- Processors can be started, stopped, swapped, and scaled independently
- Local dev: everything in one process. K8s: each group is a Deployment.
- Zinc transpiles to Go — goroutines, typed channels, native binaries (~2.5MB)

## Quick Start

```bash
# Install Zinc
curl -fsSL https://raw.githubusercontent.com/ZincScale/zinc/master/install.sh | bash

# Run the pipeline
zinc run .

# Build a native binary
zinc build .
```

## Project Structure

```
zinc-flow/
├── docs/
│   ├── design-flow.md          — requirements & architecture overview
│   ├── design-flow-runtime.md  — runtime design & implementation details
│   ├── testing-strategy.md     — test plan
│   └── architecture.md         — zinc pseudocode architecture
├── src/
│   └── main.zn                 — entry point
├── zinc.toml
└── TODO.md
```

## Design Docs

- [Flow Design](docs/design-flow.md) — requirements, processor model, pipeline DSL, architecture
- [Flow Runtime](docs/design-flow-runtime.md) — runtime architecture, queue abstraction, routing, observability
- [Architecture](docs/architecture.md) — full zinc pseudocode architecture

## Status

**Phase 1 — MVP (Local Dev)** — in progress

See [TODO.md](TODO.md) for the full roadmap.

## Related

- [Zinc Language](https://github.com/ZincScale/zinc) — the language Zinc Flow is written in
