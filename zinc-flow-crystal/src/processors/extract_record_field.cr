require "../processor"
require "json"

# ExtractRecordField — copy fields from ff.records[record_index] into
# flowfile attributes. DSL: `attrName:field.path;attrName2:other.path`.
# Nested paths split on `.`.
class ExtractRecordField < Processor
  property fields : String = ""
  property record_index : Int32 = 0

  register "ExtractRecordField",
    description: "Extract record fields into FlowFile attributes",
    category: "Record",
    wizard_component: "ExtractRecordField",
    params: {
      fields: {
        type: "KeyValueList", required: true,
        entry_delim: ";", pair_delim: ":",
        placeholder: "amount:order.amount;region:tenant.region",
        description: "attrName:fieldPath pairs",
      },
      record_index: {type: "Int32", default: "0"},
    }

  private def walk(rec : Record, path : String) : String?
    parts = path.split('.')
    cur : JSON::Any = JSON::Any.new(rec)
    parts.each do |p|
      h = cur.as_h?
      return nil if h.nil?
      nxt = h[p]?
      return nil if nxt.nil?
      cur = nxt
    end
    cur.as_s? || cur.raw.to_s
  end

  def process(ff : FlowFile) : Nil
    rs = ff.records
    if rs.nil? || rs.empty? || @record_index >= rs.size
      emit "failure", ff
      return
    end
    rec = rs[@record_index]
    @fields.split(';').each do |entry|
      s = entry.strip
      next if s.empty?
      colon = s.index(':')
      next unless colon
      attr = s[0...colon].strip
      path = s[(colon + 1)..].strip
      v = walk(rec, path)
      ff.attributes[attr] = v if v
    end
    emit "success", ff
  end
end
