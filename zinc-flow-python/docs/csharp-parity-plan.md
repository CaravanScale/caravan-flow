# zinc-flow-python → csharp parity plan

**Status as of 2026-04-15:** deferred. zinc-flow-python is on Phase 1
shape (queue-based fabric, 5 processors, old `routes:`-style routing,
old processor names `add-attribute` / `log` / `file-sink`). csharp is
the gold reference at ~21 processors, direct-pipeline executor, NiFi-
style connections, expression engine, Avro OCF, embedded schema
registry, ConfigException aggregator, hot reload, validate CLI.

This doc captures the effort shape so we can revisit without re-doing
the analysis.

## Ground truth about zinc-python

zinc-python is **not** a separate compiler like zinc-go. It's four
syntactic transforms over Python 3.14t:

1. `{}` blocks → Python indentation
2. Method-name aliases — `init → __init__`, `toString → __repr__`,
   `equals → __eq__`, etc.
3. Auto-`self` injection on class methods
4. Auto f-strings for `"...{expr}..."` literals

Everything else is plain Python. The full Python stdlib + PyPI are
available. There is **no sealed-type / `T?` / `or {}` discipline** —
the idiomatic errors-as-values shape in Python is plain exceptions
(`raise ConfigException`, `except ConfigException`), which maps 1:1
to the pattern csharp settled on in commit `4a1c11a`.

So "port csharp to zinc-python" really means "write Python, with
braces." No compiler work blocks it.

## Effort breakdown

| Area | Focused hours | Notes |
|---|---|---|
| Core types (FlowFile, ProcessorResult variants, Content) | ~4h | `dataclasses` + `Union` / match-statement |
| Fabric → direct-pipeline executor | ~4-6h | Port csharp's Stack-based ExecuteGraph; list as stack |
| Registry + ConfigException aggregator | ~3h | Python exceptions map 1:1 to csharp's pattern |
| 21 processors | ~2 days | ~45 min each; most are <100 LOC translations |
| Expression engine | 1 day | Tokenizer + shunting-yard + stack VM, mechanical |
| Avro OCF + schema registry | 1 day | Use `fastavro` from PyPI (see accelerator below); registry is dict+lock |
| Hot reload + validate CLI + management API | 1 day | `watchdog`, `argparse`, Flask or `http.server` |
| Test port (965 assertions) | 1-2 days | Bulk-translatable from `ZincFlow.Tests/*.cs` |
| Debug + integration buffer | 1-2 days | Python duck-typing catches at runtime what csharp catches at compile |

**Functional parity: ~8-10 focused days** (not perf parity — python
will stay ~95K ff/s vs csharp 2M).

## Minimum-viable-parity (recommended first cut)

Skip:
- Dashboard UI (already deferred for Go; python can follow)
- AOT / Cython optimization pass (separate concern — see
  feedback_no_nuitka memory: use Cython, not Nuitka)
- Stretch processors beyond Phase 2 core set (defer the expression-
  engine-adjacent Avro/CSV variants)

Target: `~5 focused days` to reach "useful standalone" (Phase 2a+2b
equivalent), then expand.

## Accelerators

- **`fastavro`** (PyPI) — production-grade Avro OCF reader/writer.
  Hand-rolling was necessary for csharp's AOT/zero-dep constraint;
  python has no such constraint. Saves ~1 full day.
- **`watchdog`** — file-watching for hot reload; avoids the polling
  loop csharp had to build.
- **`cattrs` or `pydantic`** — structured config parsing with
  validation hooks that feed straight into `ConfigException`. Could
  save time on the ConfigHelpers layer.
- **Existing DataFrame processors** — zinc-flow-python's unique
  differentiator (pandas/numpy/sklearn). Do **not** rewrite; carry
  forward unchanged into the modernized runtime.

## Critical files to rewrite

Python-side (`zinc-flow-python/src/`):

- `core/types.zn` — add the ProcessorResult variant (Single,
  Multiple, Routed, Dropped, Failure) and Content sealed type (Raw,
  Records, Claim). Currently partial.
- `core/queue.zn`, `core/dlq.zn` — **delete** after fabric rewrite.
  The queue-based model is replaced by direct-pipeline execution.
- `fabric/runtime.zn` — rewrite to the Stack-based ExecuteGraph model
  from csharp's `ZincFlow/Fabric/Fabric.cs`.
- `fabric/router.zn`, `fabric/registry.zn` — update for NiFi-style
  connections. Factory signature returns a processor instance after
  `ConfigException`-aggregated validation.
- `processors/builtin.zn` — rename types to csharp shapes
  (`UpdateAttribute`, `LogAttribute`, `PutFile`, etc.) and add the
  missing ones per the csharp reference.
- `processors/dataframe.zn` — leave as-is; this is the
  differentiator.
- `api/handlers.zn` — expand to match csharp's management API surface
  (stats, flow, providers, sources, provenance, reload, validate).
- New: `core/config_helpers.zn` — python port of csharp's
  `ConfigHelpers` (ParseInt, ParseLong, ParseBool, ParseSingleChar,
  RequireString, GetString, RequireOneOf).
- New: `core/config_exception.zn` — port of csharp's
  `ConfigException`.
- `config.yaml` — modernize to NiFi `connections:` shape, matching
  the parent `config.yaml` cleanup in commit `4cd057a`.

## Parity gate

The csharp binary and the zinc-flow-python binary should load **the
same YAML config byte-for-byte** (the "Phase 2 interop gate" idea
from `examples/mvp-pipeline.yaml`). That's the acceptance criterion.
If both pass `examples/mvp-pipeline.yaml` and produce equivalent
output, parity is achieved.

## What stays unique to python

- DataFrame processors (pandas/numpy/sklearn integration) — the
  reason python exists as a runtime. csharp will never match this.
- PyPI dependency access — any Python library is usable in a
  processor via zinc-python's normal `zinc.toml` `[python].deps`.

## Ordering if we ever start

1. Core types + ConfigException + ConfigHelpers (2 hours — unblocks
   everything)
2. Fabric direct-pipeline rewrite (1 day — biggest semantic delta)
3. Processor rename + Phase 2 set (2 days — interop-gate deliverable)
4. Expression engine + record processors (1 day)
5. Avro OCF via `fastavro` + schema registry (1 day)
6. Hot reload + validate CLI + mgmt API (1 day)
7. Test port + debug (2 days)

Stop after step 3 if "useful standalone + interop-compatible YAML" is
enough for the moment. Steps 4-7 are the "full Phase 2e" extension.
