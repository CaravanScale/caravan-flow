require "./spec_helper"

describe JsonPath::Parser do
  it "parses the bare root query" do
    q = JsonPath::Parser.parse("$")
    q.segments.size.should eq 0
  end

  it "parses dotted member access" do
    q = JsonPath::Parser.parse("$.store.book")
    q.segments.size.should eq 2
    q.segments[0].kind.should eq JsonPath::SegmentKind::Name
    q.segments[0].name.should eq "store"
    q.segments[1].name.should eq "book"
  end

  it "parses bracketed member access with quoting" do
    q = JsonPath::Parser.parse(%($['store']["book"]))
    q.segments.size.should eq 2
    q.segments[0].kind.should eq JsonPath::SegmentKind::Name
    q.segments[0].name.should eq "store"
    q.segments[1].name.should eq "book"
  end

  it "parses an integer index and a negative index" do
    q = JsonPath::Parser.parse("$.books[0]")
    q.segments[-1].kind.should eq JsonPath::SegmentKind::Index
    q.segments[-1].index.should eq 0

    q2 = JsonPath::Parser.parse("$.books[-1]")
    q2.segments[-1].index.should eq -1
  end

  it "parses the wildcard in both dotted and bracketed form" do
    q = JsonPath::Parser.parse("$.*")
    q.segments[0].kind.should eq JsonPath::SegmentKind::Wildcard
    q2 = JsonPath::Parser.parse("$[*]")
    q2.segments[0].kind.should eq JsonPath::SegmentKind::Wildcard
  end

  it "parses slices with each component optional" do
    q = JsonPath::Parser.parse("$.a[1:4]")
    s = q.segments[-1]
    s.kind.should eq JsonPath::SegmentKind::Slice
    s.slice_start.should eq 1
    s.slice_end.should eq 4
    s.slice_step.should be_nil

    q2 = JsonPath::Parser.parse("$.a[::2]")
    q2.segments[-1].slice_step.should eq 2
    q2.segments[-1].slice_start.should be_nil

    q3 = JsonPath::Parser.parse("$.a[:3]")
    q3.segments[-1].slice_start.should be_nil
    q3.segments[-1].slice_end.should eq 3
  end

  it "parses filter expressions with comparisons and booleans" do
    q = JsonPath::Parser.parse("$.books[?(@.price < 10 && @.stock > 0)]")
    seg = q.segments[-1]
    seg.kind.should eq JsonPath::SegmentKind::Filter
    expr = seg.filter_expr.not_nil!
    expr.kind.should eq JsonPath::FilterKind::And
  end

  it "parses a filter with string literal equality" do
    q = JsonPath::Parser.parse(%($.books[?(@.author == 'Twain')]))
    expr = q.segments[-1].filter_expr.not_nil!
    expr.kind.should eq JsonPath::FilterKind::Cmp
    expr.cmp_op.should eq JsonPath::CmpOp::Eq
    expr.right.not_nil!.str_value.should eq "Twain"
  end

  it "parses an existence filter referring to @" do
    q = JsonPath::Parser.parse("$.users[?(@.email)]")
    expr = q.segments[-1].filter_expr.not_nil!
    expr.kind.should eq JsonPath::FilterKind::Exists
    expr.ref_path.size.should eq 2
    expr.ref_path[0].kind.should eq JsonPath::PathStepKind::Current
    expr.ref_path[1].name.should eq "email"
  end

  it "parses a filter with negation and parentheses" do
    q = JsonPath::Parser.parse("$.xs[?(!(@.x == 1))]")
    expr = q.segments[-1].filter_expr.not_nil!
    expr.kind.should eq JsonPath::FilterKind::Not
    expr.operand.not_nil!.kind.should eq JsonPath::FilterKind::Cmp
  end

  it "parses a filter reference with bracketed step" do
    q = JsonPath::Parser.parse("$.items[?(@['name'] == 'a')]")
    expr = q.segments[-1].filter_expr.not_nil!
    expr.left.not_nil!.ref_path[1].name.should eq "name"
  end

  it "parses root references inside filters" do
    q = JsonPath::Parser.parse("$.items[?(@.price == $.threshold)]")
    right = q.segments[-1].filter_expr.not_nil!.right.not_nil!
    right.ref_path[0].kind.should eq JsonPath::PathStepKind::Root
    right.ref_path[1].name.should eq "threshold"
  end

  it "rejects queries that don't start with $" do
    expect_raises(JsonPath::ParseError, /must start with/) do
      JsonPath::Parser.parse("foo")
    end
  end

  it "rejects unterminated strings in filters" do
    expect_raises(JsonPath::ParseError, /unterminated/) do
      JsonPath::Parser.parse("$.books[?(@.author == 'Twain)]")
    end
  end

  it "rejects unclosed brackets" do
    expect_raises(JsonPath::ParseError, /expected ']'/) do
      JsonPath::Parser.parse("$.a[1")
    end
  end

  it "rejects bare names without a leading segment marker" do
    expect_raises(JsonPath::ParseError) do
      JsonPath::Parser.parse("$ store")
    end
  end
end
