require "spec"
require "../src/fabric"
require "../src/processor"
require "../src/registry"
require "../src/processors/all"

describe Fabric do
  it "returns nil when the graph is acyclic" do
    fab = Fabric.new
    fab.add("a", "UpdateAttribute", {"key" => "k", "value" => "v"})
    fab.add("b", "LogAttribute", {} of String => String)
    fab.connect("a", "success", "b")
    fab.find_cycle.should be_nil
  end

  it "surfaces a cycle as 'a → b → a'" do
    fab = Fabric.new
    fab.add("a", "UpdateAttribute", {"key" => "k", "value" => "v"})
    fab.add("b", "UpdateAttribute", {"key" => "k2", "value" => "v2"})
    fab.connect("a", "success", "b")
    fab.connect("b", "success", "a")
    cycle = fab.find_cycle
    cycle.should_not be_nil
    cycle.not_nil!.should contain("→")
    cycle.not_nil!.should contain("a")
    cycle.not_nil!.should contain("b")
  end
end
