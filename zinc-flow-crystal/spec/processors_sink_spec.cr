require "spec"
require "file_utils"
require "../src/processors/all"

describe PutStdout do
  # PutStdout writes straight to STDOUT; we just verify the relationship
  # emitted is :success for each supported format. Output shows up in the
  # spec runner's log but that's acceptable for a one-line preview.
  it "emits :success for each supported format" do
    ["attrs", "raw", "text", "hex"].each do |fmt|
      proc = Registry.create("PutStdout", {"format" => fmt})
      ff = FlowFile.new(attributes: {"k" => "v"})
      ff.text = "hello"
      proc.process(ff)
      proc.drain_outbox.first[0].should eq "success"
    end
  end

  it "rejects unknown formats" do
    proc = PutStdout.new
    proc.format = "binary"
    expect_raises(Exception, /unknown format/) do
      proc.process(FlowFile.new)
    end
  end
end

describe PutFile do
  it "writes content to <output_dir>/<prefix><filename><suffix>" do
    dir = File.tempname("zinc-putfile", "-spec")
    begin
      proc = Registry.create("PutFile", {
        "output_dir"       => dir,
        "naming_attribute" => "name",
        "prefix"           => "pre-",
        "suffix"           => ".out",
      })
      ff = FlowFile.new(attributes: {"name" => "greeting"})
      ff.text = "hello world"
      proc.process(ff)
      expected = File.join(dir, "pre-greeting.out")
      File.exists?(expected).should be_true
      File.read(expected).should eq "hello world"
      ff.attributes["put.file.path"].should eq expected
    ensure
      FileUtils.rm_rf(dir) if Dir.exists?(dir)
    end
  end

  it "falls back to the flowfile uuid when the naming attribute is missing" do
    dir = File.tempname("zinc-putfile-uuid", "-spec")
    begin
      proc = Registry.create("PutFile", {
        "output_dir"       => dir,
        "naming_attribute" => "name",
        "suffix"           => ".bin",
      })
      ff = FlowFile.new
      ff.text = "x"
      proc.process(ff)
      written = Dir.children(dir)
      written.size.should eq 1
      written.first.should end_with ".bin"
      written.first.should contain ff.uuid
    ensure
      FileUtils.rm_rf(dir) if Dir.exists?(dir)
    end
  end
end
