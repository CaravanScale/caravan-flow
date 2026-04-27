require "json"
require "./flowfile"

# Per-node sample ring: the last N (default 5) flowfiles a processor
# *emitted*, captured for the Peek tab in the UI. Keeping it bounded
# keeps memory O(nodes * 5) and avoids holding FlowFile content
# indefinitely. Content is clipped to a preview to keep the ring small.
class SampleRing
  RING_SIZE    = 5
  PREVIEW_SIZE = 512

  struct Sample
    include JSON::Serializable
    property timestamp : Int64
    property flowfile : String
    property content_type : String
    property preview : String?
    property preview_base64 : String?
    property attributes : Hash(String, String)

    def initialize(@timestamp, @flowfile, @content_type, @preview, @preview_base64, @attributes)
    end
  end

  @ring : Deque(Sample)
  @mutex : Mutex

  def initialize
    @ring = Deque(Sample).new
    @mutex = Mutex.new
  end

  def capture(ff : FlowFile) : Nil
    preview, preview_b64 = make_preview(ff)
    s = Sample.new(
      timestamp: Time.utc.to_unix_ms,
      flowfile: ff.uuid,
      content_type: ff.content_type,
      preview: preview,
      preview_base64: preview_b64,
      attributes: ff.attributes.dup,
    )
    @mutex.synchronize do
      @ring.push(s)
      while @ring.size > RING_SIZE
        @ring.shift
      end
    end
  end

  def snapshot : Array(Sample)
    @mutex.synchronize { @ring.to_a.reverse }
  end

  private def make_preview(ff : FlowFile) : {String?, String?}
    case ff.content_type
    when "records"
      if rs = ff.records
        # Preview the first record only; full batch would blow the ring.
        slice = rs.first?.try(&.to_json) || "[]"
        {slice[0...PREVIEW_SIZE], nil}
      else
        {"", nil}
      end
    when "text"
      txt = ff.text[0...PREVIEW_SIZE]
      {txt, nil}
    else
      # Raw bytes — show base64 so the UI can decide how to render.
      bytes = ff.content.size <= PREVIEW_SIZE ? ff.content : ff.content[0...PREVIEW_SIZE]
      {nil, Base64.strict_encode(bytes)}
    end
  end
end

require "base64"
