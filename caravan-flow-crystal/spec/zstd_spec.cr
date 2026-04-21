require "spec"
require "../src/zstd"

describe Zstd do
  it "reports a libzstd version string" do
    Zstd.version.should match(/^\d+\.\d+\.\d+$/)
  end

  it "round-trips small content" do
    src = "hello, zstd!".to_slice
    c = Zstd.compress(src)
    c.size.should be > 0
    Zstd.decompress(c).should eq src
  end

  it "round-trips bigger content at every level preset" do
    # 256 KiB of deterministic data — ensures we exercise the multi-
    # chunk path of the encoder, not just the single-frame fast path.
    src = Bytes.new(256 * 1024) { |i| (i % 256).to_u8 }
    [Zstd::LEVEL_FASTEST, Zstd::LEVEL_BALANCED, Zstd::LEVEL_SMALLEST].each do |lvl|
      c = Zstd.compress(src, lvl)
      c.size.should be < src.size
      Zstd.decompress(c).should eq src
    end
  end

  it "raises on corrupt input" do
    expect_raises(Zstd::Error) do
      Zstd.decompress(Bytes[0, 1, 2, 3])
    end
  end

  it "raises on empty input" do
    expect_raises(Zstd::Error, /empty/) do
      Zstd.decompress(Bytes.empty)
    end
  end
end
