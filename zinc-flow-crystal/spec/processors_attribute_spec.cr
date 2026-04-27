require "spec"
require "../src/processors/all"

describe LogAttribute do
  it "passes the flowfile through on :success without mutating attrs" do
    proc = Registry.create("LogAttribute", {"prefix" => "test"})
    ff = FlowFile.new(attributes: {"k" => "v"})
    proc.process(ff)
    ff.attributes.should eq({"k" => "v"})
    emitted = proc.drain_outbox
    emitted.size.should eq 1
    emitted.first[0].should eq "success"
  end

  it "defaults prefix to 'flow' when omitted" do
    proc = Registry.create("LogAttribute", {} of String => String).as(LogAttribute)
    proc.prefix.should eq "flow"
  end
end

describe FilterAttribute do
  it "removes listed attributes in remove mode" do
    proc = Registry.create("FilterAttribute", {"mode" => "remove", "attributes" => "a;b"})
    ff = FlowFile.new(attributes: {"a" => "1", "b" => "2", "c" => "3"})
    proc.process(ff)
    ff.attributes.should eq({"c" => "3"})
    proc.drain_outbox.first[0].should eq "success"
  end

  it "keeps listed attributes in keep mode" do
    proc = Registry.create("FilterAttribute", {"mode" => "keep", "attributes" => "a;b"})
    ff = FlowFile.new(attributes: {"a" => "1", "b" => "2", "c" => "3", "d" => "4"})
    proc.process(ff)
    ff.attributes.keys.sort.should eq ["a", "b"]
  end

  it "is a no-op when the attribute list is empty-after-trim in keep mode" do
    proc = Registry.create("FilterAttribute", {"mode" => "keep", "attributes" => " ; ; "})
    ff = FlowFile.new(attributes: {"a" => "1"})
    proc.process(ff)
    ff.attributes.should eq({} of String => String)
  end

  it "rejects unknown modes with a clear error" do
    proc = FilterAttribute.new
    proc.mode = "rewrite"
    proc.attributes = "a"
    expect_raises(Exception, /mode must be/) do
      proc.process(FlowFile.new)
    end
  end
end
