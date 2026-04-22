require "spec"
require "../src/fabric"
require "../src/processor"
require "../src/registry"
require "../src/processors/all"

describe Fabric do
  it "adds, enables, disables, and removes nodes; bumps the mutation counter" do
    fab = Fabric.new
    before = fab.mutation_counter.get
    fab.add("u", "UpdateAttribute", {"key" => "k", "value" => "v"})
    fab.mutation_counter.get.should be > before
    fab.nodes.has_key?("u").should be_true
    fab.nodes["u"].enabled?.should be_true
    fab.toggle("u", false)
    fab.nodes["u"].enabled?.should be_false
    fab.toggle("u", true)
    fab.nodes["u"].enabled?.should be_true
    fab.remove("u")
    fab.nodes.has_key?("u").should be_false
  end

  it "rejects duplicate node names" do
    fab = Fabric.new
    fab.add("x", "LogAttribute", {} of String => String)
    expect_raises(Exception, /node exists/) do
      fab.add("x", "LogAttribute", {} of String => String)
    end
  end

  it "connect/disconnect manages routes; dangling target raises" do
    fab = Fabric.new
    fab.add("a", "UpdateAttribute", {"key" => "k", "value" => "v"})
    fab.add("b", "LogAttribute", {} of String => String)
    fab.connect("a", "success", "b")
    fab.nodes["a"].routes["success"].should contain "b"

    fab.disconnect("a", "success", "b")
    fab.nodes["a"].routes["success"]?.should be_nil

    expect_raises(Exception, /no such node/) do
      fab.connect("a", "success", "missing")
    end
  end

  it "stats_map reports processed + errors + state per node" do
    fab = Fabric.new
    fab.add("n", "UpdateAttribute", {"key" => "k", "value" => "v"})
    m = fab.stats_map
    m.has_key?("n").should be_true
    m["n"][:processed].should eq 0_i64
    m["n"][:state].should eq "ENABLED"
  end

  it "find_cycle accepts self-loops" do
    fab = Fabric.new
    fab.add("a", "UpdateAttribute", {"key" => "k", "value" => "v"})
    fab.connect("a", "success", "a")
    fab.find_cycle.should_not be_nil
  end

  it "find_cycle returns nil for disjoint acyclic graphs" do
    fab = Fabric.new
    fab.add("a", "UpdateAttribute", {"key" => "k", "value" => "v"})
    fab.add("b", "LogAttribute", {} of String => String)
    fab.add("c", "UpdateAttribute", {"key" => "k2", "value" => "v"})
    fab.add("d", "LogAttribute", {} of String => String)
    fab.connect("a", "success", "b")
    fab.connect("c", "success", "d")
    fab.find_cycle.should be_nil
  end
end
