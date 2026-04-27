require "spec"
require "../src/edge_stats"

describe EdgeStats do
  it "bump + snapshot produce per-edge processed counters" do
    EdgeStats.clear
    5.times { EdgeStats.bump("a", "success", "b") }
    3.times { EdgeStats.bump("a", "failure", "dlq") }
    snap = EdgeStats.snapshot
    snap["a|success|b"][:processed].should eq 5_i64
    snap["a|failure|dlq"][:processed].should eq 3_i64
  end

  it "key() matches the UI's 'from|rel|to' shape" do
    EdgeStats.key("x", "y", "z").should eq "x|y|z"
  end

  it "clear wipes all counters" do
    EdgeStats.bump("a", "success", "b")
    EdgeStats.clear
    EdgeStats.snapshot.should be_empty
  end
end
