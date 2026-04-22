require "spec"
require "../src/processors/all"

describe GenerateFlowFile do
  it "emits batch_size flowfiles on source_tick, each with the configured content" do
    proc = Registry.create("GenerateFlowFile",
      {"content" => "ping", "batch_size" => "3"})
    tick = proc.source_tick
    tick.size.should eq 3
    tick.each do |ff|
      String.new(ff.content).should eq "ping"
      ff.attributes["source"].should eq "GenerateFlowFile"
    end
  end

  it "process(ff) is a no-op (sources have no inbound flowfiles)" do
    proc = Registry.create("GenerateFlowFile", {"content" => "x"})
    proc.process(FlowFile.new)
    proc.drain_outbox.should be_empty
  end

  it "registers as kind=source in the registry metadata" do
    meta = Registry.metas.find! { |m| m.name == "GenerateFlowFile" }
    meta.kind.should eq "source"
  end
end

describe "GetFile" do
  it "exposes kind=source and required output_dir param" do
    meta = Registry.metas.find! { |m| m.name == "GetFile" }
    meta.kind.should eq "source"
    meta.params.map(&.name).should contain "input_dir"
  end
end

describe "ListenHTTP" do
  it "exposes kind=source in metadata" do
    meta = Registry.metas.find! { |m| m.name == "ListenHTTP" }
    meta.kind.should eq "source"
  end
end
