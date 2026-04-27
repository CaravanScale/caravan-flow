require "spec"
require "../src/processors/all"

describe RouteOnAttribute do
  it "fans a flowfile into multiple routes when multiple rules match" do
    proc = Registry.create("RouteOnAttribute",
      {"routes" => "premium: tier EQ gold; tagged: region EXISTS"})
    ff = FlowFile.new(attributes: {"tier" => "gold", "region" => "eu"})
    proc.process(ff)
    rels = proc.drain_outbox.map(&.[0]).sort
    rels.should eq ["premium", "tagged"]
  end

  it "sends non-matching flowfiles to :unmatched" do
    proc = Registry.create("RouteOnAttribute",
      {"routes" => "premium: tier EQ gold"})
    ff = FlowFile.new(attributes: {"tier" => "silver"})
    proc.process(ff)
    proc.drain_outbox.first[0].should eq "unmatched"
  end

  it "supports CONTAINS / STARTSWITH / ENDSWITH" do
    proc = Registry.create("RouteOnAttribute",
      {"routes" => "has: url CONTAINS api; ui: url STARTSWITH http; html: url ENDSWITH html"})
    ff = FlowFile.new(attributes: {"url" => "http://site/api/index.html"})
    proc.process(ff)
    rels = proc.drain_outbox.map(&.[0]).sort
    rels.should eq ["has", "html", "ui"]
  end

  it "NEQ rule only fires when values differ" do
    proc = Registry.create("RouteOnAttribute",
      {"routes" => "diff: env NEQ prod"})
    FlowFile.new(attributes: {"env" => "stage"}).tap do |ff|
      proc.process(ff)
      proc.drain_outbox.map(&.[0]).should eq ["diff"]
    end
    FlowFile.new(attributes: {"env" => "prod"}).tap do |ff|
      proc.process(ff)
      proc.drain_outbox.map(&.[0]).should eq ["unmatched"]
    end
  end
end
