# Upstream Crystal bug reports

Parser issues hit while porting caravan-flow from C# to Crystal 1.20.0
during the April 2026 evaluation. Each report is self-contained with a
minimal reproducer verified against `crystal 1.20.0 (2026-04-16)` on
`x86_64-unknown-linux-gnu` (via `brew install crystal`, LLVM 22.1.3).

Before filing upstream, search https://github.com/crystal-lang/crystal/issues
for the search terms listed in each report — some of these may already
be known.

## Reports

1. [**`out` keyword shadows local variables after `return`**](./01-out-keyword-shadows-local.md)
   — surfaces as `expecting variable or instance variable after out` or
   `can't define def inside def` cascades. Confirmed with a 4-line reproducer.

## Filing process

1. Search issues for each report's suggested terms.
2. If no existing issue: open a new one against `crystal-lang/crystal`,
   paste the minimal reproducer + version output, label as "bug" +
   "topic:compiler:parser".
3. If an existing issue covers it: add the caravan-flow reproducer as a
   comment (one more data point) and close the local report file with
   a link.
4. PR welcome for any you want to fix — the parser is in
   `src/compiler/crystal/syntax/parser.cr`.
