require "../processor"
require "../expression"
require "json"

# TransformRecord — field-level operations on records. DSL mirrors the
# C# sibling:
#   rename:old:new  remove:field  add:field:value  copy:from:to
#   toUpper:field   toLower:field default:field:value
#   compute:target:expression
# Directives separated by ';'. Compile once on first use.
class TransformRecord < Processor
  property operations : String = ""

  register "TransformRecord",
    description: "Field-level operations on records",
    category: "Transform",
    wizard_component: "TransformRecord",
    params: {
      operations: {
        type: "Multiline", required: true,
        placeholder: "rename:oldName:newName; remove:badField; compute:total:amount*1.07",
        description: "semicolon-delimited op:arg1[:arg2] directives",
      },
    }

  record Op, kind : String, args : Array(String), expr : Expr::Node?

  @compiled : Array(Op)? = nil

  private def compiled : Array(Op)
    @compiled ||= parse_ops
  end

  private def parse_ops : Array(Op)
    out = [] of Op
    @operations.split(';').each do |chunk|
      s = chunk.strip
      next if s.empty?
      parts = s.split(':').map(&.strip)
      kind = parts[0]
      args = parts[1..]? || [] of String
      expr = kind == "compute" && args.size >= 2 ? Expr::Parser.parse(args[1]) : nil
      out << Op.new(kind, args, expr)
    end
    out
  end

  private def apply(op : Op, rec : Record, ctx : Expr::Context) : Nil
    case op.kind
    when "rename"
      next_if_missing_args(op, 2)
      src, dst = op.args[0], op.args[1]
      if v = rec.delete(src)
        rec[dst] = v
      end
    when "remove"
      next_if_missing_args(op, 1)
      rec.delete(op.args[0])
    when "add"
      next_if_missing_args(op, 2)
      rec[op.args[0]] = JSON::Any.new(op.args[1])
    when "copy"
      next_if_missing_args(op, 2)
      if v = rec[op.args[0]]?
        rec[op.args[1]] = v
      end
    when "toUpper"
      next_if_missing_args(op, 1)
      if v = rec[op.args[0]]?
        rec[op.args[0]] = JSON::Any.new(v.to_s.upcase)
      end
    when "toLower"
      next_if_missing_args(op, 1)
      if v = rec[op.args[0]]?
        rec[op.args[0]] = JSON::Any.new(v.to_s.downcase)
      end
    when "default"
      next_if_missing_args(op, 2)
      field = op.args[0]
      rec[field] = JSON::Any.new(op.args[1]) if rec[field]?.nil?
    when "compute"
      next_if_missing_args(op, 2)
      target = op.args[0]
      node = op.expr || raise "TransformRecord: compute missing compiled expr"
      v = node.eval(ctx)
      rec[target] = box(v)
    else
      raise "TransformRecord: unknown op #{op.kind}"
    end
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

  private def next_if_missing_args(op : Op, n : Int32) : Nil
    raise "TransformRecord: op #{op.kind} needs #{n} args, got #{op.args.size}" if op.args.size < n
  end

  def process(ff : FlowFile) : Nil
    rs = ff.records
    if rs.nil?
      emit "failure", ff
      return
    end
    ops = compiled
    rs.each do |rec|
      ctx = Expr::Context.new(ff.attributes, rec)
      ops.each { |op| apply(op, rec, ctx) }
    end
    emit "success", ff
  rescue e
    ff.attributes["error.message"] = e.message || "transform error"
    emit "failure", ff
  end
end
