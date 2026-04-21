require "json"
require "./ast"

module JsonPath
  # Apply a compiled `Query` to a `JSON::Any` document and return the
  # flat list of matched values. Matches are returned in document
  # order, deduplicated only implicitly (a wildcard over a
  # 5-element array yields 5 results, even if they're equal).
  #
  # `select_one` is a convenience for queries that are known to yield
  # at most one match; it returns `nil` instead of an empty array.
  module Eval
    extend self

    def evaluate(root : JSON::Any, query : Query) : Array(JSON::Any)
      current = [root]
      query.segments.each do |seg|
        current = apply_segment(seg, current, root)
      end
      current
    end

    def evaluate_one(root : JSON::Any, query : Query) : JSON::Any?
      results = evaluate(root, query)
      results.empty? ? nil : results.first
    end

    # --- per-segment dispatch ---

    private def apply_segment(seg : Segment, inputs : Array(JSON::Any), root : JSON::Any) : Array(JSON::Any)
      case seg.kind
      when SegmentKind::Name
        apply_name(seg.name.not_nil!, inputs)
      when SegmentKind::Index
        apply_index(seg.index, inputs)
      when SegmentKind::Wildcard
        apply_wildcard(inputs)
      when SegmentKind::Slice
        apply_slice(seg.slice_start, seg.slice_end, seg.slice_step, inputs)
      when SegmentKind::Filter
        apply_filter(seg.filter_expr.not_nil!, inputs, root)
      else
        inputs
      end
    end

    private def apply_name(name : String, inputs : Array(JSON::Any)) : Array(JSON::Any)
      result = [] of JSON::Any
      inputs.each do |node|
        if h = node.as_h?
          if v = h[name]?
            result << v
          end
        end
      end
      result
    end

    private def apply_index(idx : Int32, inputs : Array(JSON::Any)) : Array(JSON::Any)
      result = [] of JSON::Any
      inputs.each do |node|
        if arr = node.as_a?
          actual = idx < 0 ? arr.size + idx : idx
          if actual >= 0 && actual < arr.size
            result << arr[actual]
          end
        end
      end
      result
    end

    private def apply_wildcard(inputs : Array(JSON::Any)) : Array(JSON::Any)
      result = [] of JSON::Any
      inputs.each do |node|
        if arr = node.as_a?
          arr.each { |v| result << v }
        elsif h = node.as_h?
          h.each_value { |v| result << v }
        end
      end
      result
    end

    private def apply_slice(start : Int32?, stop : Int32?, step : Int32?, inputs : Array(JSON::Any)) : Array(JSON::Any)
      result = [] of JSON::Any
      inputs.each do |node|
        arr = node.as_a?
        next unless arr
        slice_into(arr, start, stop, step, result)
      end
      result
    end

    # Python-style slice semantics, adapted to JSONPath:
    #   - step defaults to 1, and zero is illegal
    #   - start/stop default to opposite ends depending on step sign
    #   - negative indices count from the end
    #   - out-of-range indices clamp
    private def slice_into(arr : Array(JSON::Any), start : Int32?, stop : Int32?, step : Int32?, out_arr : Array(JSON::Any)) : Nil
      n = arr.size
      st = step || 1
      raise EvalError.new("slice step cannot be zero") if st == 0

      s_default = st > 0 ? 0 : n - 1
      e_default = st > 0 ? n : -n - 1
      s = normalize_slice_bound(start, n, s_default, st > 0)
      e = normalize_slice_bound(stop,  n, e_default, st > 0)

      i = s
      if st > 0
        while i < e
          out_arr << arr[i] if i >= 0 && i < n
          i += st
        end
      else
        while i > e
          out_arr << arr[i] if i >= 0 && i < n
          i += st
        end
      end
    end

    private def normalize_slice_bound(v : Int32?, n : Int32, default : Int32, forward : Bool) : Int32
      return default if v.nil?
      x = v
      x = n + x if x < 0
      if forward
        return 0 if x < 0
        return n if x > n
      else
        return -1 if x < -1
        return n - 1 if x > n - 1
      end
      x
    end

    private def apply_filter(expr : FilterExpr, inputs : Array(JSON::Any), root : JSON::Any) : Array(JSON::Any)
      result = [] of JSON::Any
      inputs.each do |node|
        if arr = node.as_a?
          arr.each do |candidate|
            result << candidate if truthy_filter?(expr, candidate, root)
          end
        elsif h = node.as_h?
          h.each_value do |candidate|
            result << candidate if truthy_filter?(expr, candidate, root)
          end
        end
      end
      result
    end

    # --- filter evaluation ---

    # A filter evaluates against a "current" node (`@`) and the whole
    # document (`$`), returning either:
    #   - a boolean (for comparisons, And/Or/Not, Exists)
    #   - a value (for references and literals)
    #
    # We keep the value domain small: String, Float64, Bool, Nil,
    # `JSON::Any` (for objects/arrays that fell out of a reference).
    # Comparisons only operate on numbers vs numbers, strings vs
    # strings, and equality between any two scalar kinds; everything
    # else yields `false` per RFC 9535's "type mismatch is not an
    # error, just a non-match" stance.
    private alias FilterValue = String | Float64 | Bool | Nil | JSON::Any

    private def truthy_filter?(expr : FilterExpr, current : JSON::Any, root : JSON::Any) : Bool
      value = eval_filter(expr, current, root)
      truthy?(value)
    end

    private def truthy?(value : FilterValue) : Bool
      case value
      when Bool
        value
      when Nil
        false
      when Float64
        true
      when String
        true
      when JSON::Any
        # A JSON::Any that represents a JSON `null` is falsey; anything
        # else (including an empty object/array or the boolean `false`)
        # needs unwrapping so callers get what they'd expect.
        raw = value.raw
        case raw
        when Nil  then false
        when Bool then raw
        else           true
        end
      else
        false
      end
    end

    private def eval_filter(expr : FilterExpr, current : JSON::Any, root : JSON::Any) : FilterValue
      case expr.kind
      when FilterKind::LiteralString
        expr.str_value
      when FilterKind::LiteralNumber
        expr.num_value
      when FilterKind::LiteralBool
        expr.bool_value
      when FilterKind::LiteralNull
        nil
      when FilterKind::RefCurrent, FilterKind::RefRoot, FilterKind::Exists
        resolve_ref(expr.ref_path, current, root)
      when FilterKind::Cmp
        compare(expr.cmp_op, expr.left.not_nil!, expr.right.not_nil!, current, root)
      when FilterKind::And
        truthy_filter?(expr.left.not_nil!, current, root) && truthy_filter?(expr.right.not_nil!, current, root)
      when FilterKind::Or
        truthy_filter?(expr.left.not_nil!, current, root) || truthy_filter?(expr.right.not_nil!, current, root)
      when FilterKind::Not
        !truthy_filter?(expr.operand.not_nil!, current, root)
      else
        nil
      end
    end

    # Walks a reference path from either `@` or `$`, returning `nil`
    # if any step fails to resolve. Used both for Exists (where we
    # return a value that `truthy?` interprets as existence+non-nullness)
    # and for the Cmp branches where we need the underlying scalar.
    private def resolve_ref(steps : Array(PathStep), current : JSON::Any, root : JSON::Any) : FilterValue
      node : JSON::Any? = nil
      steps.each do |step|
        case step.kind
        when PathStepKind::Current
          node = current
        when PathStepKind::Root
          node = root
        when PathStepKind::Name
          return nil if node.nil?
          h = node.as_h?
          return nil if h.nil?
          got = h[step.name.not_nil!]?
          return nil if got.nil?
          node = got
        when PathStepKind::Index
          return nil if node.nil?
          a = node.as_a?
          return nil if a.nil?
          i = step.index
          actual = i < 0 ? a.size + i : i
          return nil if actual < 0 || actual >= a.size
          node = a[actual]
        end
      end
      return nil if node.nil?
      unwrap_scalar(node)
    end

    # Turn a `JSON::Any` into a native scalar when it is one; leave
    # objects and arrays wrapped so the caller can still reason about
    # existence without forcing a coercion.
    private def unwrap_scalar(node : JSON::Any) : FilterValue
      raw = node.raw
      case raw
      when Nil     then nil
      when Bool    then raw
      when Int32   then raw.to_f64
      when Int64   then raw.to_f64
      when Float32 then raw.to_f64
      when Float64 then raw
      when String  then raw
      else              node
      end
    end

    private def compare(op : CmpOp, left : FilterExpr, right : FilterExpr, current : JSON::Any, root : JSON::Any) : Bool
      lv = eval_filter(left, current, root)
      rv = eval_filter(right, current, root)

      case op
      when CmpOp::Eq  then eq?(lv, rv)
      when CmpOp::Neq then !eq?(lv, rv)
      when CmpOp::Lt  then ordered_lt?(lv, rv)
      when CmpOp::Lte then ordered_lte?(lv, rv)
      when CmpOp::Gt  then ordered_gt?(lv, rv)
      when CmpOp::Gte then ordered_gte?(lv, rv)
      else                 false
      end
    end

    private def eq?(a : FilterValue, b : FilterValue) : Bool
      return true  if a.nil? && b.nil?
      return false if a.nil? || b.nil?
      if a.is_a?(Float64) && b.is_a?(Float64)
        return a == b
      end
      if a.is_a?(String) && b.is_a?(String)
        return a == b
      end
      if a.is_a?(Bool) && b.is_a?(Bool)
        return a == b
      end
      false
    end

    # Ordered comparison — only defined for number/number and
    # string/string. Any other pairing yields false (non-match). We
    # branch on operator rather than yielding a block because Crystal
    # can't infer a union-of-types block signature cleanly here, and
    # inlining the six cases is both shorter and faster than the
    # alternatives.
    private def ordered_lt?(a : FilterValue, b : FilterValue) : Bool
      if a.is_a?(Float64) && b.is_a?(Float64)
        a < b
      elsif a.is_a?(String) && b.is_a?(String)
        a < b
      else
        false
      end
    end

    private def ordered_lte?(a : FilterValue, b : FilterValue) : Bool
      if a.is_a?(Float64) && b.is_a?(Float64)
        a <= b
      elsif a.is_a?(String) && b.is_a?(String)
        a <= b
      else
        false
      end
    end

    private def ordered_gt?(a : FilterValue, b : FilterValue) : Bool
      if a.is_a?(Float64) && b.is_a?(Float64)
        a > b
      elsif a.is_a?(String) && b.is_a?(String)
        a > b
      else
        false
      end
    end

    private def ordered_gte?(a : FilterValue, b : FilterValue) : Bool
      if a.is_a?(Float64) && b.is_a?(Float64)
        a >= b
      elsif a.is_a?(String) && b.is_a?(String)
        a >= b
      else
        false
      end
    end
  end
end
