require "../processor"
require "csv"
require "json"

# ConvertCSVToRecord — parse FlowFile content as CSV, emit records. If
# hasHeader is true the first row provides field names; otherwise they
# come from the `fields` param ("id:long,name:string") or default to
# col0/col1/... A field's declared type drives value coercion.
class ConvertCSVToRecord < Processor
  property delimiter : String = ","
  property has_header : Bool = true
  property fields : String = ""
  property schema_name : String = "default"

  register "ConvertCSVToRecord",
    description: "Parse CSV content into records",
    category: "Conversion",
    wizard_component: "RecordFields",
    params: {
      delimiter: {type: "String", default: ",", description: "single-character column delimiter"},
      has_header: {type: "Bool", default: "true"},
      fields: {
        type: "String",
        placeholder: "id:long,name:string",
        description: "comma-separated name:type pairs; overrides header-inferred names",
      },
      schema_name: {type: "String", default: "default"},
    }

  private def parse_fields : Array({String, String})
    out = [] of {String, String}
    @fields.split(',').each do |raw|
      s = raw.strip
      next if s.empty?
      colon = s.index(':')
      if colon
        out << {s[0...colon].strip, s[(colon + 1)..].strip}
      else
        out << {s, "string"}
      end
    end
    out
  end

  private def coerce(raw : String, type : String) : JSON::Any
    case type
    when "int", "long"       then JSON::Any.new(raw.to_i64? || 0_i64)
    when "float", "double"   then JSON::Any.new(raw.to_f? || 0.0)
    when "boolean", "bool"   then JSON::Any.new(raw == "true")
    else                          JSON::Any.new(raw)
    end
  end

  def process(ff : FlowFile) : Nil
    declared = parse_fields
    all_rows = CSV.parse(ff.text, separator: @delimiter[0])

    header : Array(String)? = nil
    header = all_rows.shift if @has_header && !all_rows.empty?

    names =
      if declared.any?
        declared.map(&.[0])
      elsif header
        header
      else
        [] of String
      end

    types = declared.any? ? declared.map(&.[1]) : ["string"] * names.size

    records = [] of Record
    all_rows.each do |cells|
      rec = Record.new
      cells.each_with_index do |v, i|
        name = names[i]? || "col#{i}"
        type = types[i]? || "string"
        rec[name] = coerce(v, type)
      end
      records << rec
    end

    ff.records = records
    ff.content_type = "records"
    ff.attributes["schema.name"] = @schema_name
    ff.attributes["record.count"] = records.size.to_s
    emit "success", ff
  rescue e
    ff.attributes["error.message"] = e.message || "csv parse error"
    emit "failure", ff
  end
end
