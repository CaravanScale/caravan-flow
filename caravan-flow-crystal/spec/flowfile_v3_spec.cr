require "spec"
require "../src/flowfile_v3"

describe FlowFileV3 do
  it "round-trips a single flowfile with attributes + content" do
    ff = FlowFile.new(attributes: {"a" => "1", "long" => "x" * 1024})
    ff.text = "hello world"
    packed = FlowFileV3.pack(ff)
    packed[0, 7].should eq FlowFileV3::MAGIC

    unpacked = FlowFileV3.unpack_all(packed)
    unpacked.size.should eq 1
    decoded = unpacked.first
    decoded.attributes["a"].should eq "1"
    decoded.attributes["long"].should eq "x" * 1024
    String.new(decoded.content).should eq "hello world"
  end

  it "handles attribute values that exceed the 2-byte field-length threshold" do
    # Length > 0xFFFF forces the extended 4-byte form of the length prefix.
    big = "y" * 70_000
    ff = FlowFile.new(attributes: {"big" => big})
    packed = FlowFileV3.pack(ff)
    emitted = FlowFileV3.unpack_all(packed).first
    emitted.attributes["big"].should eq big
  end

  it "pack_multiple + unpack_all preserves order" do
    list = (0..4).map { |i|
      ff = FlowFile.new(attributes: {"i" => i.to_s})
      ff.text = "body-#{i}"
      ff
    }
    unpacked = FlowFileV3.unpack_all(FlowFileV3.pack_multiple(list))
    unpacked.size.should eq 5
    unpacked.each_with_index do |ff, i|
      ff.attributes["i"].should eq i.to_s
      String.new(ff.content).should eq "body-#{i}"
    end
  end

  it "returns an empty list on non-V3 input (parser is permissive)" do
    FlowFileV3.unpack_all("garbage-without-magic".to_slice).should be_empty
  end
end
