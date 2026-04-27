# crystal-jsonpath

RFC 9535 JSONPath for Crystal. Hand-rolled parser + evaluator over
stdlib `JSON::Any`. No reflection, AOT-safe, works under `preview_mt`.
Intended as the missing "JSONPath for Crystal" shard so applications
targeting AOT'd static binaries don't have to build their own.

Current status: **0.1.0 — pragmatic subset.** See "What's next" for the
0.2 scope.

## Install

In your `shard.yml`:

```yaml
dependencies:
  jsonpath:
    github: ZincScale/crystal-jsonpath
    version: ~> 0.1
```

Then `shards install`.

## Usage

```crystal
require "json"
require "jsonpath"

doc = JSON.parse(<<-JSON)
  {
    "store": {
      "book": [
        {"category": "reference", "author": "Nigel Rees",  "price": 8.95},
        {"category": "fiction",   "author": "Evelyn Waugh","price": 12.99}
      ]
    }
  }
  JSON

# One-shot parse + select
JsonPath.select(doc, "$.store.book[*].author")
# => [JSON::Any("Nigel Rees"), JSON::Any("Evelyn Waugh")]

# Compile once, reuse
query = JsonPath.compile("$.store.book[?(@.price < 10)].author")
JsonPath.select(doc, query)       # => [JSON::Any("Nigel Rees")]
JsonPath.select_one(doc, query)   # => JSON::Any("Nigel Rees")
```

## Supported (v0.1)

| Syntax                     | What it does                                    |
|----------------------------|-------------------------------------------------|
| `$`                        | Root of the document                             |
| `.name` or `['name']`      | Child member                                     |
| `[n]`                      | Array index (negative counts from the end)       |
| `[start:end:step]`         | Slice, Python-style; each component optional     |
| `.*` or `[*]`              | Wildcard over object values or array elements    |
| `[?(expr)]`                | Filter selector                                  |

Filter expressions support:

- Literals: strings (single- or double-quoted), numbers, `true`, `false`, `null`
- References: `@` (current node), `$` (document root), with `.name` and `[n]` steps
- Comparisons: `==`, `!=`, `<`, `<=`, `>`, `>=`
- Boolean ops: `&&`, `||`, `!`, parentheses for grouping
- Existence: a bare reference like `?(@.email)` is true when the member resolves
  to a non-null value

Comparisons that mix incompatible types (e.g. string vs number) are
treated as non-matches, not errors — this mirrors RFC 9535's "type
mismatch is not an error" stance. Ordered comparisons (`<`, `<=`,
`>`, `>=`) only admit number/number and string/string.

## What's next (v0.2)

These are in the spec but intentionally out of v0.1 scope. Each is
a meaningful feature on its own and deserves its own design and
test surface rather than being crammed in.

- **Recursive descent (`..`)** — `$..author` matching every `author`
  at any depth. Requires deciding on cycle handling for graphs that
  lie about being trees.
- **Function extensions** — `length()`, `count()`, `match()`,
  `search()`, `value()`. RFC 9535 §2.4.
- **Union selectors** — `['a', 'b']`, `[0, 2, 4]`. Needs care in
  ordering: RFC says document order, which is straightforward for
  arrays but subtle for objects.
- **Normalized output paths** — returning the canonical path to
  each match, not just the value.

## Design notes

- **Single-pass recursive-descent parser.** One char of lookahead, one
  cursor. No regex, no tokenizer layer — the grammar is small enough
  that an inline dispatch on `peek` is clearer than staging tokens.
- **`JSON::Any` all the way down.** The evaluator operates directly on
  stdlib's `JSON::Any`; we never reify into our own node types. That
  keeps the surface area of the shard small and means
  `JSON.parse(http_body)` plugs in without a conversion step.
- **Static methods on modules, not classes with state.** Parsing and
  evaluation are pure functions of their inputs.
- **No deps beyond stdlib.** `json` and `spec` are all we need.

## Known Crystal-1.20 quirks worth noting

- `out` is a reserved word that shadows local variables — we use
  `out_arr` (and friends) instead in slice helpers.
- `String#each_char_with_index` is used in the parser's `starts_with?`
  helper because hand-indexing into a `String` is O(n) per access for
  multibyte strings, and `each_char_with_index` is the cheapest
  single-pass alternative.

## License

MIT. See `LICENSE`.
