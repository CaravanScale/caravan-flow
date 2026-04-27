require "../processor"
require "../expression"
require "json"

# UpdateRecord — for each record in ff.records, evaluate a set of
# field=expression pairs and write the results back. The expression
# context sees both the record's fields and the flowfile attributes.
class UpdateRecord < Processor
  property updates : String = ""

  register "UpdateRecord",
    description: "Set or derive record fields via expressions",
    category: "Transform",
    wizard_component: "UpdateRecord",
    params: {
      updates: {
        type: "KeyValueList", required: true,
        value_kind: "Expression",
        entry_delim: ";", pair_delim: "=",
        placeholder: "tax=amount*0.07; total=amount+tax",
        description: "field=expression pairs; later pairs see earlier writes",
      },
    }

  @compiled : Array({String, Expr::Node})? = nil

  private def compiled : Array({String, Expr::Node})
    @compiled ||= (begin
      out = [] of {String, Expr::Node}
      @updates.split(';').each do |chunk|
        s = chunk.strip
        next if s.empty?
        eq = s.index('=')
        next unless eq
        out << {s[0...eq].strip, Expr::Parser.parse(s[(eq + 1)..].strip)}
      end
      out
    end)
  end

  private def box(v : Expr::Value) : JSON::Any
    case v
    when Nil     then JSON::Any.new(nil)
    when String  then JSON::Any.new(v)
    when Int64   then JSON::Any.new(v)
    when Float64 then JSON::Any.new(v)
    when Bool    then JSON::Any.new(v)
    else              JSON::Any.new(v.to_s)
    end
  end

  def process(ff : FlowFile) : Nil
    rs = ff.records
    if rs.nil?
      emit "failure", ff
      return
    end
    rs.each do |rec|
      ctx = Expr::Context.new(ff.attributes, rec)
      compiled.each do |name, node|
        rec[name] = box(node.eval(ctx))
      end
    end
    emit "success", ff
  rescue e
    ff.attributes["error.message"] = e.message || "expression error"
    emit "failure", ff
  end
end
