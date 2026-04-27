require "spec"
require "../src/processors/all"

describe "CompressContent + DecompressContent" do
  {"gzip", "zstd"}.each do |algo|
    it "round-trips #{algo} through the two processors" do
      payload = "the quick brown fox jumps over the lazy dog " * 64
      ff = FlowFile.new(content: payload.to_slice)

      compressor = Registry.create("CompressContent", {"algorithm" => algo, "level" => "balanced"})
      compressor.process(ff)
      emitted = compressor.drain_outbox
      emitted.size.should eq 1
      rel, compressed_ff = emitted.first
      rel.should eq "success"
      compressed_ff.content.size.should be < payload.bytesize
      compressed_ff.attributes["compression.algorithm"].should eq algo

      decompressor = Registry.create("DecompressContent", {"algorithm" => "auto"})
      decompressor.process(compressed_ff)
      dec_emitted = decompressor.drain_outbox
      dec_emitted.size.should eq 1
      rel2, restored_ff = dec_emitted.first
      rel2.should eq "success"
      String.new(restored_ff.content).should eq payload
      restored_ff.attributes.has_key?("compression.algorithm").should be_false
    end
  end

  it "emits :failure when asked to decompress a non-compressed payload" do
    ff = FlowFile.new(content: "not actually compressed".to_slice)
    ff.attributes["compression.algorithm"] = "zstd"
    decompressor = Registry.create("DecompressContent", {"algorithm" => "auto"})
    decompressor.process(ff)
    emitted = decompressor.drain_outbox
    emitted.size.should eq 1
    emitted.first[0].should eq "failure"
  end
end
