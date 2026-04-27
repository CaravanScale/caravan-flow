require "../processor"
require "../expression"

# EvaluateExpression — attribute = expression pairs. Each pair's
# expression is parsed once on first `process`, then evaluated per
# flowfile. Later rows see values of earlier rows (the attribute
# mutation happens before we move on).
class EvaluateExpression < Processor
  property expressions : String = ""

  register "EvaluateExpression",
    description: "Compute attributes from expressions",
    category: "Transform",
    wizard_component: "EvaluateExpression",
    params: {
      expressions: {
        type: "KeyValueList", required: true,
        value_kind: "Expression",
        entry_delim: ";", pair_delim: "=",
        placeholder: "tax=amount*0.07; label=upper(region)",
        description: "attr=expression pairs; later pairs see earlier writes",
      },
    }

  @compiled : Array({String, Expr::Node})? = nil

  private def compiled : Array({String, Expr::Node})
    @compiled ||= parse_pairs
  end

  private def parse_pairs : Array({String, Expr::Node})
    out = [] of {String, Expr::Node}
    @expressions.split(';').each do |chunk|
      s = chunk.strip
      next if s.empty?
      eq = s.index('=')
      next unless eq
      name = s[0...eq].strip
      src = s[(eq + 1)..].strip
      out << {name, Expr::Parser.parse(src)}
    end
    out
  end

  def process(ff : FlowFile) : Nil
    ctx = Expr::Context.new(ff.attributes)
    compiled.each do |name, node|
      v = node.eval(ctx)
      ff.attributes[name] = v.to_s
    end
    emit "success", ff
  rescue e
    ff.attributes["error.message"] = e.message || "expression error"
    emit "failure", ff
  end
end
