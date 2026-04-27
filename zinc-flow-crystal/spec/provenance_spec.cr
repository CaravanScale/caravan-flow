require "spec"
require "../src/provenance"

describe Provenance do
  it "record + recent returns events in most-recent-first order" do
    # Use unique flowfile ids so we can filter out events produced by
    # sibling specs running in the same process.
    ids = 3.times.map { |i| "prov-spec-#{i}-#{Time.utc.to_unix_ns}" }.to_a
    ids.each_with_index do |id, i|
      Provenance.record(id, "CREATE", "source", "note-#{i}")
    end
    mine = Provenance.recent(1024).select { |e| ids.includes?(e.flowfile) }
    mine.size.should eq 3
    # most-recent first means ids[2] comes before ids[0]
    mine.first.flowfile.should eq ids[2]
  end

  it "failures() filters down to type=FAILURE" do
    id = "prov-fail-#{Time.utc.to_unix_ns}"
    Provenance.record(id, "ROUTE", "x", "routed")
    Provenance.record(id, "FAILURE", "x", "bad")
    Provenance.failures(1024).any? { |e| e.flowfile == id }.should be_true
  end

  it "by_id returns every event for that flowfile" do
    id = "prov-id-#{Time.utc.to_unix_ns}"
    Provenance.record(id, "CREATE", "s")
    Provenance.record(id, "ROUTE", "s")
    Provenance.by_id(id).size.should eq 2
  end
end
