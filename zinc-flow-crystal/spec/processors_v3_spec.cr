require "spec"
require "../src/processors/all"
require "../src/flowfile_v3"

describe PackageFlowFileV3 do
  it "produces bytes that start with the NiFiFF3 magic + set mime attr" do
    proc = Registry.create("PackageFlowFileV3", {} of String => String)
    ff = FlowFile.new(attributes: {"a" => "1"})
    ff.text = "payload"
    proc.process(ff)
    ff.content[0, 7].should eq FlowFileV3::MAGIC
    ff.attributes["mime.type"].should eq "application/flowfile-v3"
    proc.drain_outbox.first[0].should eq "success"
  end
end

describe UnpackageFlowFileV3 do
  it "round-trips pack → unpack with attributes + content preserved" do
    src = FlowFile.new(attributes: {"k" => "v", "n" => "7"})
    src.text = "hello"
    packed = FlowFileV3.pack(src)

    proc = Registry.create("UnpackageFlowFileV3", {} of String => String)
    wrapper = FlowFile.new(content: packed)
    proc.process(wrapper)
    emitted = proc.drain_outbox
    emitted.size.should eq 1
    rel, child = emitted.first
    rel.should eq "success"
    child.attributes["k"].should eq "v"
    child.attributes["n"].should eq "7"
    String.new(child.content).should eq "hello"
  end

  it "emits :failure with error.message on non-V3 content" do
    proc = Registry.create("UnpackageFlowFileV3", {} of String => String)
    wrapper = FlowFile.new(content: "not v3".to_slice)
    proc.process(wrapper)
    rel, emitted = proc.drain_outbox.first
    rel.should eq "failure"
    emitted.attributes.has_key?("error.message").should be_true
  end

  it "splits multi-frame packed content into N flowfiles" do
    a = FlowFile.new(attributes: {"i" => "0"})
    a.text = "aaa"
    b = FlowFile.new(attributes: {"i" => "1"})
    b.text = "bbb"
    packed = FlowFileV3.pack_multiple([a, b])

    proc = Registry.create("UnpackageFlowFileV3", {} of String => String)
    wrapper = FlowFile.new(content: packed)
    proc.process(wrapper)
    emitted = proc.drain_outbox
    emitted.size.should eq 2
    emitted.map { |(_, f)| f.attributes["i"] }.should eq ["0", "1"]
  end
end
