# Design — UI wizards for non-programmer configuration

> **Status (2026-04-20): PROPOSED, deferred.**
> This document captures the next major UI phase: removing the requirement
> that operators know expression languages, JSONPath, or schema-field
> syntax to configure processors. Work is planned but not scheduled.

## Context

Caravan-flow's product pitch is visual functional programming for domain
experts (see `memory/project_audience_domain_experts.md`,
`memory/project_visual_functional_programming.md`). The graph canvas +
palette already deliver that at the topology level: drag, drop, connect.

But the **processor configuration** layer still leaks the underlying
expression machinery:

- `RouteRecord.routes` wants a JEXL-style predicate string
  (`premium: tier == "gold"; minors: age < 18`).
- `UpdateRecord.updates` wants `tax = amount * 0.07; total = amount + tax`.
- `EvaluateExpression.expressions` likewise.
- `ExtractRecordField.fields` wants `amount:order.amount;region:tenant.region`
  — the operator has to know the record's field paths by heart.
- `QueryRecord.query` wants raw JSONPath (`$[?(@.amount > 100)]`).
- `RouteOnAttribute.routes` wants a mini-DSL (`tier EQ premium`) with
  operator tokens like `EQ`, `NEQ`, `CONTAINS`, `GT`, `LT`.
- `ConvertCSVToRecord.fields` / `ConvertAvroToRecord.fields` want
  `id:long,name:string,amount:double` — a schema description language.
- `TransformRecord.operations` wants
  `rename:oldName:newName; remove:badField; compute:total:amount*1.07` —
  an op-per-row mini-DSL.

Domain experts who understand dataflow perfectly well don't always speak
these dialects. Today the `ProcessorConfigForm` renders proper typed
inputs (dropdowns, number spinners, KeyValueList repeaters, textareas for
expression fields), which is a big step up from raw key/value pairs, but
the content of those fields is still freeform text the user has to
compose from scratch.

This doc lays out a three-wave plan to close that gap.

## Goals

1. A domain expert who has never written a JEXL / EL / JSONPath
   expression can build a working flow end-to-end by clicking.
2. Sample data from the running flow flows into the configuration UI so
   operators build expressions **against real records**, not abstract
   schemas they imagined.
3. Every processor in the catalog eventually has a first-class wizard
   for its mini-DSL, with the raw textarea kept as an escape hatch for
   power users.
4. Extensibility: when someone adds a new processor, they can register a
   custom wizard component for it without forking the form renderer.

## Non-goals

- Turing-complete visual programming inside a processor config. The
  expression builder is for *composing expressions in the same language
  EL accepts*, not inventing a new one. No loops, no mutable state in
  the wizard — stays aligned with the visual-FP memory rule.
- Removing the textarea escape hatch. Power users still paste raw
  expressions; the wizard is additive.
- Record sampling as a durable observability story. Wave 1 captures a
  tiny ring buffer per processor for config-time preview; full
  provenance / debug tooling is a separate plan.

---

## Wave 1 — Record sampling + field picker

### 1a. Backend: per-processor sample ring

Add a shared `Dictionary<string, RingBuffer<FlowFileSample>>` on
`Fabric`, keyed by processor name. Capacity per ring ~5 FlowFiles.
Populate from the dispatch loop in `Fabric.drain` right after a
processor returns — store `(timestamp, attributes, content-preview)`.
Content preview is the first 4 KiB of the content as base64 (enough to
show a record in the UI, small enough not to balloon memory).

New endpoint: `GET /api/processors/{name}/samples` returns the latest N
samples from that processor's output, each `{timestamp, attributes,
contentBase64, contentType}`.

The samples ring is opt-in per processor via a new `sampling.enabled`
runtime flag (default off in production-heavy flows to keep the
inspection overhead down; default on in standalone/dev mode). When off,
the endpoint returns an empty list.

### 1b. UI: Sample tab on ProcessorDrawer

Add a fourth tab next to `config`/`connections`/`stats`: `sample`. When
selected, polls `/api/processors/{name}/samples` every 2 s and renders
the most recent output as a formatted JSON/CSV/text view (based on
`contentType` or a best-effort sniff of the first bytes).

For RecordContent-producing processors, also parse the sample as
records and show a tabular view with column headers (field names) and
rows (values). This is the gateway UX for Wave 3's record viewer.

### 1c. UI: Field picker primitive

When an Expression or KeyValueList input has focus, the Sample tab
(visible side-by-side in a split drawer layout) renders each field name
as a clickable chip. Clicking a chip inserts the field reference
(`name`, or dotted path `address.city` for nested records) into the
focused input at the cursor position.

The field picker understands basic path syntax: flat fields for top-level
keys, `.` for nested records, `[0]` for arrays (future).

**Acceptance:** a user can select `RouteRecord.routes`, click the `age`
field chip in the sample panel, then click `<`, then type `18`, and the
`routes` input fills with `age < 18` — no keyboard composition of the
expression.

### Minimal Wave 1 scope

- Backend: samples ring + endpoint. ~150 LOC.
- UI: Sample tab + field picker. ~300 LOC.
- No wizards yet — just samples + click-to-insert.

---

## Wave 2 — Expression wizard modal

### 2a. Generic expression builder component

A modal that opens when the user clicks a "Build…" button next to any
`Expression` or `KeyValueList(valueKind=Expression)` input. Structure:

- **Field picker (left pane)**: scrollable list of fields from the
  sample ring, grouped by record path. Click to insert at cursor.
- **Function palette (center pane)**: buttons for EL functions —
  `upper`, `lower`, `substring`, `contains`, `coalesce`, `round`,
  `abs`, `if`, `min`, `max`, etc. Each button inserts a parameterized
  template (`upper(|)` with the cursor between the parens).
- **Operator toolbar**: `+` `-` `*` `/` `%` `==` `!=` `<` `>` `<=` `>=`
  `&&` `||` `!`.
- **Preview pane (right or bottom)**: shows the expression evaluated
  against the most recent sample, or the parser error if malformed.
  Live-updates as the user types.
- **OK/Cancel**: OK writes the composed expression back to the
  originating input field.

### 2b. Multi-expression variants

For `UpdateRecord` / `EvaluateExpression` (KeyValueList of
field=expression pairs), the modal has a row-repeater layout: each
row has a name field (left) and the expression builder (right). Adding
rows + preserving the left-to-right semantics (later expressions see
earlier writes) surfaces as a live "fields after this step" panel that
updates cumulatively.

For `RouteRecord` (KeyValueList of route: predicate), the modal
similarly does rows with a route name and an expression, plus a preview
that shows which sample records would land on which route — operators
tune their predicates against real data.

### 2c. Parser integration

The preview pane needs a lightweight parser so the UI can show errors
without round-tripping to the backend. Options:
- Bundle a JS port of the EL parser.
- Or debounced POST to a new `/api/expression/parse` endpoint that
  compiles + returns syntax errors + evaluated result against a
  provided sample.

Lean toward the latter — one source of truth for parser behavior.

---

## Wave 3 — Domain-specific wizards

### 3a. TransformRecord wizard

Replaces the Multiline textarea for `TransformRecord.operations` with a
row-per-operation editor. Each row:
- Op dropdown: `rename`, `remove`, `add`, `copy`, `toUpper`,
  `toLower`, `default`, `compute`.
- Arg1 field: picker from sample fields (for fieldName-style args).
- Arg2 field: depends on op — literal value for `add`, target name for
  `copy`/`rename`, Expression builder for `compute`.
- Live preview: how the sample record transforms through this op row
  and every row above it.

### 3b. RouteOnAttribute wizard

Replace the Multiline with row editors:
- Route name.
- Attribute picker from known FlowFile attributes (sampled).
- Operator dropdown: `EQ`, `NEQ`, `CONTAINS`, `STARTSWITH`,
  `ENDSWITH`, `EXISTS`, `GT`, `LT`.
- Value field.

### 3c. ExtractRecordField wizard

Table view: record sample on the left with a checkbox per field; each
checked field gets a row on the right (field path → attribute name).
Auto-fills attribute name from field path's last segment but lets the
user rename. Output is the `field:attr;field:attr` encoding the
processor already expects.

### 3d. QueryRecord wizard

JSONPath builder that mirrors the expression builder but scoped to
JSONPath syntax. Common filters surface as buttons (`$[?(@.x > N)]`,
`$[*].field`, etc.) with the user filling in the blanks.

### 3e. Schema designer for Convert*ToRecord processors

`ConvertJSONToRecord` / `ConvertCSVToRecord` / `ConvertAvroToRecord` all
take a schema-ish `fields` parameter with `name:type,name:type,...`
encoding. Replace the freeform input with:

- Sample-driven inference: if a sample (e.g. first CSV row, first JSON
  object) is available, infer a starting schema and let the user
  refine.
- Row editor: name + type dropdown (`string`, `int`, `long`, `float`,
  `double`, `boolean`, `bytes`). Drag to reorder.
- Preview shows the schema applied to the sample and flags fields that
  don't parse cleanly under the chosen type.

The same component powers the Avro / CSV / JSON converter designers —
one schema editor, three processor hooks.

### 3f. Record viewer page

A dedicated observability page (separate from the drawer's Sample tab):
list every processor, click to see its last N output samples in a
tabular record view with column sort, filter, download-as-CSV. Useful
for debugging flow shapes end-to-end, not just configuring one
processor.

---

## Implementation order and gating

1. **Wave 1 first** — the sample ring and field picker pay for themselves
   immediately (operators see data) and unlock every subsequent wizard.
2. **Wave 2** — the generic expression builder. Delivers 60–70% of the
   "no EL syntax" value since the builder covers any Expression input.
3. **Wave 3 incrementally** — TransformRecord and RouteOnAttribute
   wizards first (they sit on top of a custom DSL, not generic EL),
   then ExtractRecordField (simple), then schema designer (biggest
   payoff but most UI work), then QueryRecord and the dedicated Record
   viewer page.

Each wave is shippable on its own; don't block later waves on earlier
ones except for the dependencies stated.

## Extension hook

When the generic form renderer doesn't fit, a processor registration
should be able to declare a custom wizard component. Propose a
`wizardComponent` field on `ProcessorInfo` (a string id that the UI
maps to a React component). `AddProcessorDialog` / `ProcessorDrawer`
pick the registered wizard when present, fall through to the typed
form when absent.

Makes it cheap for new processors to ship a custom UI without forking
the shared form plumbing.

## Open questions

- **Sample ring retention**: does sampling survive hot reload? Probably
  yes for kept processors, discarded for removed ones — aligns with
  provenance semantics.
- **Sampling overhead budget**: at what throughput does the 4 KiB
  content-capture become a real cost? Needs a micro-benchmark before
  enabling sampling by default in production mode.
- **Localization**: function palette labels, operator tooltips, empty
  states. Not in scope for initial delivery but the component API
  should not hard-code English strings.
- **Undo/redo inside wizards**: lower priority than getting the wizards
  shipped. Ship without; revisit based on user feedback.

## Out of scope for this doc

- Any backend changes to the expression engine itself. The wizards
  compose expressions in the existing EL; they don't extend the
  language.
- The record viewer page as a standalone observability tool (3f) could
  split off into its own design doc if scope grows; captured here so
  it's not orphaned.
- k8s aggregator / operator interactions — sampling and wizards apply
  to a single worker's API; multi-worker sample aggregation follows
  `docs/design-k8s-operator.md`.
