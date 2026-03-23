# Zinc Flow

Lightweight, cloud-native data flow engine inspired by Apache NiFi. Built in [Zinc](https://github.com/victorybhg/zinc) to dogfood the language.

## What is it?

NiFi's processor model — with Zinc's simplicity, cloud-native from day one.

- A **processor** is a Zinc function
- A **pipeline** connects processors with queues
- Processors can be started, stopped, swapped, and scaled independently in production
- Local dev: everything in one process. K8s: each group is a Deployment.

## Quick Start

```bash
# Install Zinc first
curl -fsSL https://raw.githubusercontent.com/victorybhg/zinc/master/install.sh | bash

# Run a pipeline
zinc flow run src/main.zn
```

## Project Structure

```
zinc-flow/
├── docs/
│   ├── design-flow.md          — requirements & architecture overview
│   └── design-flow-runtime.md  — runtime design & implementation details
├── src/
│   └── main.zn                 — entry point
├── build.mill.yaml
└── TODO.md
```

## Design Docs

- [Flow Design](docs/design-flow.md) — requirements, processor model, pipeline DSL, architecture
- [Flow Runtime](docs/design-flow-runtime.md) — runtime architecture, queue abstraction, routing, observability

## Status

**Phase 1 — MVP (Local Dev)** — not started

See [TODO.md](TODO.md) for the full roadmap.

## Related

- [Zinc Language](https://github.com/victorybhg/zinc) — the language Zinc Flow is written in
