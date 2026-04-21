require "json"

# Minimal expression language for EvaluateExpression / UpdateRecord /
# RouteRecord. Deliberately a subset of caravan-csharp's EL — enough
# to demonstrate the port mechanics. Supported grammar:
#
#   expr    := or_expr
#   or_expr := and_expr ('||' and_expr)*
#   and_expr:= cmp_expr ('&&' cmp_expr)*
#   cmp_expr:= add_expr (('=='|'!='|'<='|'>='|'<'|'>') add_expr)?
#   add_expr:= mul_expr (('+'|'-') mul_expr)*
#   mul_expr:= unary   (('*'|'/'|'%') unary)*
#   unary   := '!'? primary
#   primary := number | string | bool | ident | fn_call | '(' expr ')'
#   fn_call := ident '(' (expr (',' expr)*)? ')'
#
# ident lookup order: context.record fields → context.attributes →
# a few built-in keywords (true/false/null). Functions: upper, lower,
# trim, length, substring, concat, int, double, isNull, isEmpty, if.
#
# This is a tree-walking interpreter — no bytecode. Hot loops would
# cache the parsed AST on the processor (see EvaluateExpression).

module Expr
  alias Value = String | Int64 | Float64 | Bool | Nil

  struct Context
    getter attributes : Hash(String, String)
    getter record : Hash(String, JSON::Any)?

    def initialize(@attributes, @record = nil)
    end

    def lookup(name : String) : Value
      if rec = @record
        v = rec[name]?
        return coerce_json(v) unless v.nil?
      end
      @attributes[name]?
    end

    private def coerce_json(v : JSON::Any?) : Value
      return nil if v.nil?
      case raw = v.raw
      when Int64    then raw
      when Int32    then raw.to_i64
      when Float64  then raw
      when Bool     then raw
      when String   then raw
      when Nil      then nil
      else               raw.to_s
      end
    end
  end

  abstract class Node
    abstract def eval(ctx : Context) : Value
  end

  class LitNode < Node
    def initialize(@v : Value); end
    def eval(ctx : Context) : Value; @v; end
  end

  class IdentNode < Node
    def initialize(@name : String); end
    def eval(ctx : Context) : Value
      case @name
      when "true"  then true
      when "false" then false
      when "null"  then nil
      else              ctx.lookup(@name)
      end
    end
  end

  class BinOp < Node
    def initialize(@op : String, @left : Node, @right : Node); end

    def eval(ctx : Context) : Value
      a = @left.eval(ctx)
      b = @right.eval(ctx)
      case @op
      when "+"
        if a.is_a?(String) || b.is_a?(String)
          "#{a}#{b}"
        else
          num(a) + num(b)
        end
      when "-" then num(a) - num(b)
      when "*" then num(a) * num(b)
      when "/" then b == 0 ? 0.0 : num(a) / num(b)
      when "%" then num(a).to_i64 % (num(b).to_i64 == 0 ? 1_i64 : num(b).to_i64)
      when "==" then eq?(a, b)
      when "!=" then !eq?(a, b)
      when "<"  then num(a) <  num(b)
      when "<=" then num(a) <= num(b)
      when ">"  then num(a) >  num(b)
      when ">=" then num(a) >= num(b)
      when "&&" then truthy?(a) && truthy?(b)
      when "||" then truthy?(a) || truthy?(b)
      else raise "Expr: unknown op '#{@op}'"
      end
    end

    private def num(v : Value) : Float64
      case v
      when Int64   then v.to_f
      when Float64 then v
      when Bool    then v ? 1.0 : 0.0
      when String  then v.to_f? || 0.0
      else              0.0
      end
    end

    private def eq?(a : Value, b : Value) : Bool
      return true  if a.nil? && b.nil?
      return false if a.nil? || b.nil?
      a.to_s == b.to_s
    end

    private def truthy?(v : Value) : Bool
      case v
      when Nil    then false
      when Bool   then v
      when Int64  then v != 0_i64
      when Float64 then v != 0.0
      when String then !v.empty? && v != "false" && v != "0"
      end.as(Bool)
    end
  end

  class NotOp < Node
    def initialize(@inner : Node); end
    def eval(ctx : Context) : Value
      v = @inner.eval(ctx)
      case v
      when Nil    then true
      when Bool   then !v
      when String then v.empty? || v == "false" || v == "0"
      when Int64  then v == 0_i64
      when Float64 then v == 0.0
      end.as(Bool)
    end
  end

  class CallNode < Node
    def initialize(@name : String, @args : Array(Node)); end

    def eval(ctx : Context) : Value
      vals = @args.map(&.eval(ctx))
      case @name
      when "upper"    then vals[0].to_s.upcase
      when "lower"    then vals[0].to_s.downcase
      when "trim"     then vals[0].to_s.strip
      when "length"   then vals[0].to_s.size.to_i64
      when "substring"
        s = vals[0].to_s
        i = (vals[1]?.try(&.to_s.to_i?) || 0)
        j = vals.size > 2 ? (vals[2]?.try(&.to_s.to_i?) || s.size) : s.size
        s[i...j]? || ""
      when "concat"   then vals.map(&.to_s).join
      when "int"      then vals[0].to_s.to_i64? || 0_i64
      when "double"   then vals[0].to_s.to_f? || 0.0
      when "isNull"   then vals[0].nil?
      when "isEmpty"  then vals[0].nil? || vals[0].to_s.empty?
      when "contains" then vals[0].to_s.includes?(vals[1].to_s)
      when "startsWith" then vals[0].to_s.starts_with?(vals[1].to_s)
      when "endsWith" then vals[0].to_s.ends_with?(vals[1].to_s)
      when "if"       then truthy?(vals[0]) ? vals[1] : vals[2]
      else raise "Expr: unknown function '#{@name}'"
      end
    end

    private def truthy?(v : Value) : Bool
      case v
      when Nil    then false
      when Bool   then v
      when Int64  then v != 0_i64
      when Float64 then v != 0.0
      when String then !v.empty? && v != "false" && v != "0"
      end.as(Bool)
    end
  end

  # --- parser ---

  class Parser
    def self.parse(src : String) : Node
      Parser.new(src).parse_or
    end

    def initialize(@src : String)
      @pos = 0
    end

    private def peek : Char?
      @src[@pos]?
    end

    private def advance : Char?
      ch = @src[@pos]?
      @pos += 1 if ch
      ch
    end

    private def skip_ws
      while ch = peek
        break unless ch.whitespace?
        advance
      end
    end

    private def match?(s : String) : Bool
      skip_ws
      return false if @pos + s.size > @src.size
      return false unless @src[@pos, s.size] == s
      @pos += s.size
      true
    end

    def parse_or : Node
      left = parse_and
      while match?("||")
        left = BinOp.new("||", left, parse_and)
      end
      left
    end

    def parse_and : Node
      left = parse_cmp
      while match?("&&")
        left = BinOp.new("&&", left, parse_cmp)
      end
      left
    end

    def parse_cmp : Node
      left = parse_add
      %w(== != <= >= < >).each do |op|
        if match?(op)
          return BinOp.new(op, left, parse_add)
        end
      end
      left
    end

    def parse_add : Node
      left = parse_mul
      loop do
        if match?("+")
          left = BinOp.new("+", left, parse_mul)
        elsif match?("-")
          left = BinOp.new("-", left, parse_mul)
        else
          break
        end
      end
      left
    end

    def parse_mul : Node
      left = parse_unary
      loop do
        if match?("*")
          left = BinOp.new("*", left, parse_unary)
        elsif match?("/")
          left = BinOp.new("/", left, parse_unary)
        elsif match?("%")
          left = BinOp.new("%", left, parse_unary)
        else
          break
        end
      end
      left
    end

    def parse_unary : Node
      if match?("!")
        return NotOp.new(parse_unary)
      end
      parse_primary
    end

    def parse_primary : Node
      skip_ws
      ch = peek
      raise "Expr: unexpected end of input" unless ch

      case ch
      when '('
        advance
        node = parse_or
        skip_ws
        raise "Expr: expected ')'" unless advance == ')'
        node
      when '"'
        advance
        start = @pos
        while (c = peek) && c != '"'
          advance
        end
        raise "Expr: unterminated string" unless peek == '"'
        s = @src[start...@pos]
        advance
        LitNode.new(s.as(Value))
      when .number?, '-'
        start = @pos
        advance if ch == '-'
        while (c = peek) && (c.ascii_number? || c == '.')
          advance
        end
        txt = @src[start...@pos]
        v = txt.includes?('.') ? txt.to_f.as(Value) : txt.to_i64.as(Value)
        LitNode.new(v)
      else
        # ident or fn_call
        start = @pos
        while (c = peek) && (c.alphanumeric? || c == '_' || c == '.')
          advance
        end
        name = @src[start...@pos]
        raise "Expr: expected identifier at #{@pos}" if name.empty?
        skip_ws
        if peek == '('
          advance
          args = [] of Node
          skip_ws
          unless peek == ')'
            args << parse_or
            while match?(",")
              args << parse_or
            end
          end
          skip_ws
          raise "Expr: expected ')'" unless advance == ')'
          CallNode.new(name, args)
        else
          IdentNode.new(name)
        end
      end
    end
  end
end
