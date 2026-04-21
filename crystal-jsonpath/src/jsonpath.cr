# RFC 9535 JSONPath for Crystal. Hand-rolled parser + evaluator over
# stdlib `JSON::Any`. No reflection, AOT-safe, works under preview_mt.
#
# Entry points:
#   JsonPath.compile(src)                 # -> Query
#   JsonPath.select(document_any, query)  # -> Array(JSON::Any)
#   JsonPath.select(document_any, src)    # parse + select in one call
#   JsonPath.select_one(document_any, q)  # -> JSON::Any?
#
# Recursive descent, function extensions, and union selectors are not
# in v0.1. See README.md for scope details.

require "./jsonpath/ast"
require "./jsonpath/parser"
require "./jsonpath/eval"

module JsonPath
  VERSION = "0.1.0"

  # Compile a JSONPath string into a reusable `Query`. Parsing is
  # cheap; reuse the compiled query if you'll evaluate it many times.
  def self.compile(src : String) : Query
    Parser.parse(src)
  end

  # Evaluate a compiled query (or a raw query string) against a JSON
  # document. The document is a `JSON::Any` — use `JSON.parse` to get
  # one from a `String`/`IO`.
  def self.select(root : JSON::Any, query : Query) : Array(JSON::Any)
    Eval.evaluate(root, query)
  end

  def self.select(root : JSON::Any, src : String) : Array(JSON::Any)
    Eval.evaluate(root, compile(src))
  end

  def self.select_one(root : JSON::Any, query : Query) : JSON::Any?
    Eval.evaluate_one(root, query)
  end

  def self.select_one(root : JSON::Any, src : String) : JSON::Any?
    Eval.evaluate_one(root, compile(src))
  end
end
