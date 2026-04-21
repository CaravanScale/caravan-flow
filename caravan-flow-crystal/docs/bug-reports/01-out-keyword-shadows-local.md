# `out` keyword shadows local variable of the same name after `return` / `next` / `break`

**Crystal version:** 1.20.0 (2026-04-16) / LLVM 22.1.3 / `x86_64-unknown-linux-gnu`

## Summary

A local variable named `out` parses fine in assignment and reference
contexts, but after the keywords `return`, `next`, or `break` the parser
enters a mode that expects the C-FFI `out <ident>` syntax (the
out-parameter marker). This breaks every normal use of a local named
`out` in those positions. The user sees one of two errors depending on
what follows:

- Direct form: `Error: expecting variable or instance variable after out`
- Cascading form: `Error: can't define def inside def` *at an unrelated
  method later in the file* — because once the parser fails mid-expression
  it misreads subsequent method `def`s as nested definitions.

The second form is particularly nasty to debug because it points at a
line many hundreds below the actual problem.

## Minimal reproducer — direct form

```crystal
def a
  out = 5
  return out
end
```

```
$ crystal build repro.cr
In repro.cr:3:14

 3 | return out
                  ^
Error: expecting variable or instance variable after out
```

## Minimal reproducer — cascading form

```crystal
def a
  out = 5
  return out
end

def b
  42
end
```

```
$ crystal build repro.cr
In repro.cr:6:1

 6 | def b
     ^
Error: can't define def inside def
```

The actual bug is in method `a`; `def b` is collateral.

## Variants that all trigger the same bug

All of these fail:

```crystal
return out                       # bare
return out if cond               # trailing if
return out.size                  # method call on out
return out.as(Int32?)            # cast
next out                         # inside a block
break out                        # inside a block
```

All of these work (confirming the bug is specific to the `out`
identifier, not the surrounding syntax):

```crystal
return result           # any other local name
return value
return (out)            # parenthesized — parser leaves FFI mode
return 0 if cond        # literal value, not a local
x = out                 # reference to out in non-return context
out if cond             # trailing-if conditional, out bound at call site
```

## Root cause (hypothesis)

Crystal accepts `out <ident>` as a marker for C-FFI out-parameters, e.g.:

```crystal
lib LibFoo
  fun read(fd : Int32, out bytes_read : Int32)
end

LibFoo.read(0, out n)  # `out n` marks n as an out-parameter
```

After `return` / `next` / `break`, the parser appears to enter a state
where it treats a bare `out` identifier as the start of an `out`-parameter
expression, expecting an identifier to follow. The identifier-check is
strict (only variables / instance variables), so any token other than a
plain identifier after `out` — including `if`, `.method`, end-of-statement,
etc. — yields the error above.

Wrapping with parentheses (`return (out)`) pushes the expression into
normal expression-parser mode, sidestepping the FFI-keyword check.

## Workarounds

1. Rename the local. `out` → `result`, `accum`, `output`, etc.
2. Wrap the return value in parens: `return (out)`.
3. Use explicit `if/end` instead of trailing `if`:

   ```crystal
   if cond
     return out
   end
   ```

   Note: this alone is **not** a workaround for the bare-`return out` form;
   it only avoids the trailing-if variant.

## Suggested fix direction

The `out` FFI parse mode should only activate inside a call-argument
context where the enclosing call is a `Lib` fun binding. Outside that
context (plain method calls, expression positions, statement returns),
`out` should be treated as any other identifier. The check likely lives
around `Parser#parse_out` / `Parser#parse_call_args` in
`src/compiler/crystal/syntax/parser.cr`.

## Search terms for existing-issue check

Before filing: search https://github.com/crystal-lang/crystal/issues for:

- `expecting variable or instance variable after out`
- `return out` parser
- `out keyword` shadowing local
- `can't define def inside def`

If an issue already covers this, add the minimal reproducer above as
a comment — confirmed still present in 1.20.0 is useful data.

## Why this bit us

In the caravan-flow port, the natural convention for "initialize
accumulator, populate, return" was:

```crystal
def coerce_config(raw : JSON::Any?)
  out = {} of String => String
  return out if raw.nil?      # <-- blew up here, cascading to later def
  raw.as_h.each { |k, v| out[k] = v.to_s }
  out
end
```

The trailing-`if` form matches idiomatic Ruby/Crystal perfectly — the
fact that it silently fails (with an unrelated error 15 lines later)
was an hour of debugging. A clear error message pointing at the
actual token — something like "`out` is reserved for C-FFI
out-parameters; rename the local or use parentheses" — would save
future users that time.
