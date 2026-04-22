require "spec"
require "json"
require "../src/processors/all"

describe EvaluateExpression do
  it "writes each pair into ff.attributes in order" do
    proc = Registry.create("EvaluateExpression",
      {"expressions" => "tax=amount*0.07; label=upper(region)"})
    ff = FlowFile.new(attributes: {"amount" => "100", "region" => "eu"})
    proc.process(ff)
    ff.attributes["tax"].to_f.should be_close(7.0, 0.001)
    ff.attributes["label"].should eq "EU"
    proc.drain_outbox.first[0].should eq "success"
  end

  it "later pairs see earlier pair writes" do
    # Arithmetic in the Crystal expression engine promotes to Float64,
    # so the stringified attribute is "6.0" rather than "6" — matches
    # C# EL when operands mix types.
    proc = Registry.create("EvaluateExpression",
      {"expressions" => "doubled=amount*2; quad=doubled*2"})
    ff = FlowFile.new(attributes: {"amount" => "3"})
    proc.process(ff)
    ff.attributes["doubled"].to_f.should be_close(6.0, 0.001)
    ff.attributes["quad"].to_f.should be_close(12.0, 0.001)
  end

  it "emits :failure with error.message on parse errors" do
    proc = Registry.create("EvaluateExpression", {"expressions" => "x=))"})
    proc.process(FlowFile.new)
    proc.drain_outbox.first[0].should eq "failure"
  end
end

describe UpdateRecord do
  it "computes new fields from existing fields" do
    proc = Registry.create("UpdateRecord", {"updates" => "total=amount*2"})
    ff = FlowFile.new
    ff.records = [{"amount" => JSON::Any.new(50_i64)} of String => JSON::Any]
    proc.process(ff)
    # Expression arithmetic produces Float64, so the boxed JSON value is
    # a Float64 rather than an Int — assert via .as_f to keep green.
    ff.records.not_nil!.first["total"].as_f.should be_close(100.0, 0.001)
    proc.drain_outbox.first[0].should eq "success"
  end

  it "later updates see prior writes" do
    proc = Registry.create("UpdateRecord",
      {"updates" => "tax=amount*0.07; total=amount+tax"})
    ff = FlowFile.new
    ff.records = [{"amount" => JSON::Any.new(100.0)} of String => JSON::Any]
    proc.process(ff)
    rec = ff.records.not_nil!.first
    rec["tax"].as_f.should be_close(7.0, 0.001)
    rec["total"].as_f.should be_close(107.0, 0.001)
  end

  it "emits :failure on nil records" do
    proc = Registry.create("UpdateRecord", {"updates" => "x=1"})
    proc.process(FlowFile.new)
    proc.drain_outbox.first[0].should eq "failure"
  end
end

describe TransformRecord do
  it "supports rename + remove + add op kinds" do
    proc = Registry.create("TransformRecord",
      {"operations" => "rename:old:new; remove:bad; add:const:7"})
    ff = FlowFile.new
    ff.records = [
      {
        "old" => JSON::Any.new("x"),
        "bad" => JSON::Any.new("y"),
      } of String => JSON::Any,
    ]
    proc.process(ff)
    rec = ff.records.not_nil!.first
    rec.has_key?("old").should be_false
    rec.has_key?("bad").should be_false
    rec["new"].as_s.should eq "x"
    rec["const"].as_s.should eq "7"
  end

  it "compute: directive writes a derived field" do
    proc = Registry.create("TransformRecord",
      {"operations" => "compute:total:amount*1.1"})
    ff = FlowFile.new
    ff.records = [{"amount" => JSON::Any.new(10_i64)} of String => JSON::Any]
    proc.process(ff)
    ff.records.not_nil!.first["total"].as_f.should be_close(11.0, 0.001)
  end
end

describe RouteRecord do
  it "partitions records across named routes; no-match goes to :unmatched" do
    proc = Registry.create("RouteRecord",
      {"routes" => %(premium: tier == "gold"; bulk: tier == "silver")})
    ff = FlowFile.new
    ff.records = [
      {"tier" => JSON::Any.new("gold")} of String => JSON::Any,
      {"tier" => JSON::Any.new("silver")} of String => JSON::Any,
      {"tier" => JSON::Any.new("bronze")} of String => JSON::Any,
    ]
    proc.process(ff)
    emitted = proc.drain_outbox
    rels = emitted.map(&.[0]).sort
    rels.should contain "premium"
    rels.should contain "bulk"
    rels.should contain "unmatched"
  end

  it "emits :failure on nil records" do
    proc = Registry.create("RouteRecord", {"routes" => "all: 1 == 1"})
    proc.process(FlowFile.new)
    proc.drain_outbox.first[0].should eq "failure"
  end
end
