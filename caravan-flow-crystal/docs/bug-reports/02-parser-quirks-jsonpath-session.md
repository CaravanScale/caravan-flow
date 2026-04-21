# Three more Crystal 1.20 parser quirks hit while writing `crystal-jsonpath`

**Crystal version:** 1.20.0 (2026-04-16) / LLVM 22.1.3 / `x86_64-unknown-linux-gnu`

These were hit in the same port session as [bug 01](./01-out-keyword-shadows-local.md).
None of them are as severe — all have obvious workarounds — but each
cost measurable debugging time because the parser's error message
didn't point at the actual cause. Worth filing for better diagnostics
even if the workarounds stay.

## A. `select` as a reserved word inside call position

### Summary

Calling a method `select` with arguments in the middle of an expression
confuses the parser into starting a fiber-select block (`select/when/end`).

### Minimal reproducer

The bug is at **call** site, not def site. Bare `select(a, b)`
inside a method body is parsed as the opener of a fiber select
statement, so the parser then expects `when` / `else` / `end`.

```crystal
module Pipeline
  def self.select(a : Int32, b : Int32) : Int32
    a + b
  end

  def self.caller : Int32
    select(1, 2)          # <-- this line explodes
  end
end

puts Pipeline.caller
```

```
$ crystal build repro.cr
 13 | select(1, 2)
            ^
Error: unexpected token: "(" (expecting when, else or end)
```

Calling through the module — `Pipeline.select(1, 2)` — parses fine,
because the explicit receiver is enough to disambiguate. The trouble
is only the unqualified call form, which is the natural thing to
write inside the module that owns the method.

### Workaround

Rename the method. `evaluate`, `choose`, `pick`, `apply` — anything
non-colliding. We renamed `JsonPath::Eval.select` → `Eval.evaluate`
in the shard. Users outside the module who called `JsonPath.select(...)`
were fine the whole time.

### Hypothesis

`select` is the fiber-select statement opener, and the parser commits
to that path as soon as it sees an unqualified `select` identifier.
A simple lookahead (if the token following `select` is `(`, it's a
call, not a statement) would disambiguate.

## B. Single-line `if cond then stmt; stmt; end` fails to parse

### Summary

The one-line form of `if/then/end` accepts a single expression but
rejects multiple semicolon-separated statements — even though a
multi-line `if/then/.../end` accepts any number of statements.

### Minimal reproducer

```crystal
x = 1
if x == 1 then puts "a"; puts "b"; end
```

```
$ crystal build repro.cr
In repro.cr:2:26
Error: unexpected token: ";"
```

### Expected

In Ruby (the closest sibling language), this is idiomatic and legal:

```ruby
if x == 1 then puts "a"; puts "b"; end  # fine in Ruby
```

Crystal inherits the multi-statement-per-line convention (`a = 1; b = 2`)
but rejects it inside one-line `if/then/end`.

### Workaround

Break to multi-line:

```crystal
if x == 1
  puts "a"
  puts "b"
end
```

Hit while porting a compact dispatch table:

```crystal
# Wanted:
if op == "==" then push(EQ); advance; next end

# Actual:
if op == "=="
  push(EQ)
  advance
  next
end
```

Minor but churny — the port doubled in line count for what should be
an equivalent form.

## C. Block type signatures reject union-typed yielded args

### Summary

Declaring a block type where one of the yielded arguments is a union
fails to parse; the compiler rejects the `|` in the union as a block-
parameter delimiter.

### Minimal reproducer

```crystal
def yielder(&block : (Float64 | String), (Float64 | String) -> Bool)
  yield(1.0, 2.0)
end

yielder do |a, b|
  a == b
end
```

```
$ crystal build repro.cr
Error: expecting token ','
```

### Workaround

Define helper methods for each concrete type combination, or use
`|` at the call site and cast internally:

```crystal
def yielder(&block : Float64, Float64 -> Bool)
  yield(1.0, 2.0)
end
```

Or use an alias that resolves to the union outside the block signature:

```crystal
alias Num = Float64 | String
def yielder(&block : Num, Num -> Bool)
  yield(1.0, 2.0)
end
```

The alias form works. The inline union doesn't. This is almost
certainly a grammar issue where the block-param parser consumes `|`
greedily as the end-of-params delimiter.

## Combined filing recommendation

These three plus the [`out`-keyword bug](./01-out-keyword-shadows-local.md)
are a natural "four parser papercuts encountered in one day porting a
real project" bundle. Useful framing for the forum post: not "here's
one obscure issue" but "here's what a real-world porter hits." Makes
prioritization easier for the maintainers — they can fix the most
impactful ones first.

Filing order suggestion:

1. **`out` keyword** (01) — worst UX: cascading "can't define def inside def" errors pointing at wrong line.
2. **`select` conflict** (here, A) — second worst because the error message gives no hint it's a keyword collision.
3. **Block union signature** (here, C) — real feature gap; workaround exists but is ugly.
4. **Single-line if/then** (here, B) — cosmetic; the multi-line form is always available.

## Reproducers

Runnable files + a `verify.sh` that re-tests on the current Crystal
are in [`reproducers/`](./reproducers/). Each file reproduces its
documented bug on Crystal 1.20.0; if a fix lands upstream, the
corresponding file will start compiling.
