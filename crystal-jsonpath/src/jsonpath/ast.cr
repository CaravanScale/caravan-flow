module JsonPath
  # Errors raised by the parser or evaluator. They carry a position
  # (character offset into the original query string) when available
  # so callers can highlight bad input.
  class Error < Exception
    property position : Int32

    def initialize(message : String, @position : Int32 = -1)
      super(message)
    end
  end

  class ParseError  < Error; end
  class EvalError   < Error; end

  # A compiled JSONPath query: an ordered chain of segments to apply
  # starting from the root `$`. Each segment narrows or expands the
  # set of matched values.
  class Query
    property segments : Array(Segment)

    def initialize(@segments : Array(Segment) = [] of Segment)
    end
  end

  # A segment is one bracket or dotted step in the query. The kind
  # drives what inner data is used:
  #   Name     — `name` (member access; dotted or quoted)
  #   Index    — `index` (integer array index; negative indices count from end)
  #   Wildcard — matches every member of an object or every element of an array
  #   Slice    — `slice_start`, `slice_end`, `slice_step` (Python-style, all optional)
  #   Filter   — `filter_expr` evaluated per candidate with `@` bound to the candidate
  enum SegmentKind
    Name
    Index
    Wildcard
    Slice
    Filter
  end

  class Segment
    property kind : SegmentKind
    property name : String?
    property index : Int32
    property slice_start : Int32?
    property slice_end : Int32?
    property slice_step : Int32?
    property filter_expr : FilterExpr?

    def initialize(
      @kind : SegmentKind,
      *,
      @name : String? = nil,
      @index : Int32 = 0,
      @slice_start : Int32? = nil,
      @slice_end : Int32? = nil,
      @slice_step : Int32? = nil,
      @filter_expr : FilterExpr? = nil,
    )
    end

    def self.name(n : String) : Segment
      new(SegmentKind::Name, name: n)
    end

    def self.index(i : Int32) : Segment
      new(SegmentKind::Index, index: i)
    end

    def self.wildcard : Segment
      new(SegmentKind::Wildcard)
    end

    def self.slice(s : Int32?, e : Int32?, step : Int32?) : Segment
      new(SegmentKind::Slice, slice_start: s, slice_end: e, slice_step: step)
    end

    def self.filter(expr : FilterExpr) : Segment
      new(SegmentKind::Filter, filter_expr: expr)
    end
  end

  # Filter expressions (the body of `?( ... )`). A tiny algebra:
  # literals, references into the current node `@` or root `$`,
  # comparisons, boolean ops, and an "existence" test for bare
  # references like `?(@.email)`.
  enum FilterKind
    LiteralString
    LiteralNumber
    LiteralBool
    LiteralNull
    RefCurrent
    RefRoot
    Exists
    Cmp
    And
    Or
    Not
  end

  enum CmpOp
    Eq
    Neq
    Lt
    Lte
    Gt
    Gte
  end

  class FilterExpr
    property kind : FilterKind
    property str_value : String?
    property num_value : Float64
    property bool_value : Bool
    property ref_path : Array(PathStep)
    property cmp_op : CmpOp
    property left : FilterExpr?
    property right : FilterExpr?
    property operand : FilterExpr?

    def initialize(
      @kind : FilterKind,
      *,
      @str_value : String? = nil,
      @num_value : Float64 = 0.0,
      @bool_value : Bool = false,
      @ref_path : Array(PathStep) = [] of PathStep,
      @cmp_op : CmpOp = CmpOp::Eq,
      @left : FilterExpr? = nil,
      @right : FilterExpr? = nil,
      @operand : FilterExpr? = nil,
    )
    end
  end

  # A single step inside a filter-expression reference. Filters can
  # reach into nested members and indices (`@.address.city`,
  # `@.tags[0]`) so we need a mini path too — but we deliberately
  # keep it simpler than the top-level Query (no slices, no nested
  # filters) because those are rarely meaningful inside predicates
  # and the extra syntax would just invite bugs.
  enum PathStepKind
    Name
    Index
    Current
    Root
  end

  class PathStep
    property kind : PathStepKind
    property name : String?
    property index : Int32

    def initialize(@kind : PathStepKind, *, @name : String? = nil, @index : Int32 = 0)
    end

    def self.name(n : String) : PathStep
      new(PathStepKind::Name, name: n)
    end

    def self.index(i : Int32) : PathStep
      new(PathStepKind::Index, index: i)
    end

    def self.current : PathStep
      new(PathStepKind::Current)
    end

    def self.root : PathStep
      new(PathStepKind::Root)
    end
  end
end
