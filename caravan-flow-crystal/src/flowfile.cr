require "uuid"
require "json"

# Record = a typed key-value map. Used by record-oriented processors
# (ConvertJSONToRecord, ExtractRecordField, TransformRecord, ...). A
# FlowFile's content_type flips to "records" when it carries them.
alias Record = Hash(String, JSON::Any)

# A FlowFile is the unit of work. Content may be raw bytes, UTF-8 text,
# or — once a Convert*ToRecord processor has parsed it — a typed record
# batch. Downstream record processors look at @content_type and @records.
class FlowFile
  getter uuid : String
  property attributes : Hash(String, String)
  property content : Bytes
  property content_type : String
  property records : Array(Record)?

  def initialize(
    @content : Bytes = Bytes.empty,
    @attributes = {} of String => String,
    @content_type = "bytes",
    @records = nil
  )
    @uuid = UUID.random.to_s
  end

  # Shallow clone used by processors that fan out (SplitText,
  # SplitRecord) — attributes map is copied so siblings don't alias.
  # The new ff gets a fresh uuid via the constructor.
  def clone : FlowFile
    FlowFile.new(
      content: @content,
      attributes: @attributes.dup,
      content_type: @content_type,
      records: @records,
    )
  end

  def text : String
    String.new(@content)
  end

  def text=(s : String) : Nil
    @content = s.to_slice
    @content_type = "text"
  end

  def to_json(io : IO)
    {
      uuid:         @uuid,
      attributes:   @attributes,
      content_size: @content.size,
      content_type: @content_type,
      record_count: @records.try(&.size) || 0,
    }.to_json(io)
  end
end
