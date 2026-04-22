require "spec"
require "json"
require "../src/expression"

describe Expr do
  it "evaluates arithmetic + ident lookup from attributes" do
    ctx = Expr::Context.new({"amount" => "100"})
    node = Expr::Parser.parse("amount * 0.07")
    node.eval(ctx).as(Float64).should be_close(7.0, 0.001)
  end

  it "evaluates string concat with + on mixed types" do
    ctx = Expr::Context.new({"tier" => "gold"})
    node = Expr::Parser.parse(%{"tier=" + tier})
    node.eval(ctx).should eq "tier=gold"
  end

  it "supports functions: upper, lower, length, concat" do
    ctx = Expr::Context.new({"x" => "Hello"})
    Expr::Parser.parse("upper(x)").eval(ctx).should eq "HELLO"
    Expr::Parser.parse("lower(x)").eval(ctx).should eq "hello"
    Expr::Parser.parse("length(x)").eval(ctx).should eq 5_i64
    Expr::Parser.parse(%{concat("a", "-", "b")}).eval(ctx).should eq "a-b"
  end

  it "evaluates comparisons and boolean logic" do
    ctx = Expr::Context.new({"age" => "21", "tier" => "gold"})
    Expr::Parser.parse(%{age > 18 && tier == "gold"}).eval(ctx).should eq true
    Expr::Parser.parse(%{age < 18 || tier == "bulk"}).eval(ctx).should eq false
  end

  it "looks up record fields ahead of attributes" do
    rec = {"amount" => JSON::Any.new(42_i64)} of String => JSON::Any
    ctx = Expr::Context.new({"amount" => "100"}, rec)
    Expr::Parser.parse("amount").eval(ctx).should eq 42_i64
  end

  # caravan-csharp parity — these are the functions Crystal was
  # missing at the time of the April 2026 parity audit.
  it "supports math functions abs/min/max/floor/ceil/round/pow/sqrt" do
    ctx = Expr::Context.new({"n" => "9"})
    Expr::Parser.parse("abs(-7)").eval(ctx).should eq 7_i64
    Expr::Parser.parse("min(3, 1, 2)").eval(ctx).should eq 1_i64
    Expr::Parser.parse("max(3, 1, 2)").eval(ctx).should eq 3_i64
    Expr::Parser.parse("floor(1.9)").eval(ctx).should eq 1_i64
    Expr::Parser.parse("ceil(1.1)").eval(ctx).should eq 2_i64
    Expr::Parser.parse("round(1.6)").eval(ctx).should eq 2_i64
    Expr::Parser.parse("pow(2, 10)").eval(ctx).should eq 1024.0
    Expr::Parser.parse("sqrt(n)").eval(ctx).should eq 3.0
  end

  it "supports coalesce, replace, and the long/string/bool casts" do
    ctx = Expr::Context.new({"name" => "alice"})
    # coalesce returns the first non-null arg; missing attrs resolve to nil
    Expr::Parser.parse("coalesce(missing, name)").eval(ctx).should eq "alice"
    Expr::Parser.parse(%{replace("foo-bar", "-", "_")}).eval(ctx).should eq "foo_bar"
    Expr::Parser.parse(%{long("42")}).eval(ctx).should eq 42_i64
    Expr::Parser.parse("string(42)").eval(ctx).should eq "42"
    Expr::Parser.parse(%{bool("true")}).eval(ctx).should eq true
    Expr::Parser.parse(%{bool("")}).eval(ctx).should eq false
  end
end
