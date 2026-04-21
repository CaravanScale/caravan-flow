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
end
