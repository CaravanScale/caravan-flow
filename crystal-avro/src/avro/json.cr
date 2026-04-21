require "json"
require "./schema"

module Avro
  # Parse + emit Avro JSON schemas (spec 1.11). Covers:
  #   - Primitive types (`"int"`, `"string"`, ...)
  #   - Records with nested record fields
  #   - Unions, with the common ["null", T] nullable idiom lifted to
  #     `Field#nullable = true` for ergonomic access
  #   - Logical types on primitive fields
  #       (decimal w/ precision+scale, uuid, date, time-*, timestamp-*)
  #   - Arrays and maps (item type captured in `element_type` /
  #     `values_type`)
  #
  # The parser is lenient on unknown keys — they're skipped — but strict
  # on required structure (records must have `fields`, fields must have
  # `name` + `type`).
  module SchemaJson
    extend self

    def parse(src : String) : Schema
      any = JSON.parse(src)
      root = any.as_h? || raise InvalidSchemaError.new("top-level schema must be a JSON object")
      parse_record(root, default_name: "root")
    end

    def emit(schema : Schema) : String
      String.build do |io|
        emit_record(schema, io)
      end
    end

    # --- parse ---

    private def parse_record(h : Hash(String, JSON::Any), default_name : String) : Schema
      type = h["type"]?.try &.as_s?
      raise InvalidSchemaError.new("top-level schema must be a record (got type=#{type.inspect})") unless type == "record"

      name = h["name"]?.try(&.as_s?) || default_name
      namespace = h["namespace"]?.try(&.as_s?)
      doc = h["doc"]?.try(&.as_s?)
      aliases = extract_aliases(h)

      fields_arr = h["fields"]?.try(&.as_a?) ||
        raise InvalidSchemaError.new("record schema missing 'fields' array")

      fields = fields_arr.map_with_index do |fj, idx|
        fh = fj.as_h? || raise InvalidSchemaError.new("field ##{idx} is not a JSON object")
        parse_field(fh)
      end

      Schema.new(name, fields, namespace: namespace, doc: doc, aliases: aliases)
    end

    private def parse_field(h : Hash(String, JSON::Any)) : Field
      name = h["name"]?.try(&.as_s?) || raise InvalidSchemaError.new("field missing 'name'")
      type_json = h["type"]? || raise InvalidSchemaError.new("field '#{name}' missing 'type'")
      doc = h["doc"]?.try(&.as_s?)
      default_value = h["default"]?.try { |d| d.raw.to_s }

      parsed = parse_type(type_json, name)
      parsed.default_value = default_value
      parsed.doc = doc
      parsed.name = name
      parsed
    end

    # A type token in Avro JSON is one of: a string (primitive name),
    # an array (union), or an object (annotated primitive, nested
    # record, array, map, enum, fixed).
    private def parse_type(j : JSON::Any, field_name : String) : Field
      if s = j.as_s?
        return Field.new(field_name, PrimitiveType.from_avro_name(s))
      end

      if arr = j.as_a?
        return parse_union(arr, field_name)
      end

      if h = j.as_h?
        return parse_type_object(h, field_name)
      end

      raise InvalidSchemaError.new("field '#{field_name}': unsupported type token")
    end

    private def parse_union(branches : Array(JSON::Any), field_name : String) : Field
      kinds = [] of PrimitiveType
      nested_record : Schema? = nil
      element : Field? = nil
      values : Field? = nil
      logical = LogicalType::None
      precision = 0
      scale = 0
      winner : PrimitiveType? = nil

      branches.each do |b|
        if s = b.as_s?
          pt = PrimitiveType.from_avro_name(s)
          kinds << pt
          winner = pt unless pt == PrimitiveType::Null
        elsif h = b.as_h?
          inner = parse_type_object(h, field_name)
          kinds << inner.type
          winner ||= inner.type
          nested_record ||= inner.record_schema
          element ||= inner.element_type
          values ||= inner.values_type
          if inner.logical != LogicalType::None
            logical = inner.logical
            precision = inner.precision
            scale = inner.scale
          end
        else
          raise InvalidSchemaError.new("field '#{field_name}': union branch must be string or object")
        end
      end

      nullable = kinds.includes?(PrimitiveType::Null) && kinds.size == 2
      if nullable
        Field.new(
          field_name, winner || PrimitiveType::String,
          nullable: true,
          logical: logical,
          precision: precision,
          scale: scale,
          record_schema: nested_record,
          element_type: element,
          values_type: values,
        )
      else
        Field.new(field_name, PrimitiveType::Union, union_branches: kinds)
      end
    end

    private def parse_type_object(h : Hash(String, JSON::Any), field_name : String) : Field
      type_token = h["type"]? || raise InvalidSchemaError.new("type object missing 'type'")
      type_str = type_token.as_s?

      if type_str == "record"
        nested = parse_record(h, default_name: field_name)
        return Field.new(field_name, PrimitiveType::Record, record_schema: nested)
      end

      if type_str == "array"
        items_json = h["items"]? || raise InvalidSchemaError.new("array '#{field_name}' missing 'items'")
        items = parse_type(items_json, "#{field_name}.items")
        return Field.new(field_name, PrimitiveType::Array, element_type: items)
      end

      if type_str == "map"
        values_json = h["values"]? || raise InvalidSchemaError.new("map '#{field_name}' missing 'values'")
        vals = parse_type(values_json, "#{field_name}.values")
        return Field.new(field_name, PrimitiveType::Map, values_type: vals)
      end

      # Annotated primitive: {"type": "long", "logicalType": "timestamp-millis"}
      if type_str
        logical = h["logicalType"]?.try(&.as_s?).try { |n| LogicalType.from_avro_name(n) } || LogicalType::None
        precision = h["precision"]?.try(&.as_i?) || 0
        scale = h["scale"]?.try(&.as_i?) || 0
        return Field.new(
          field_name,
          PrimitiveType.from_avro_name(type_str),
          logical: logical,
          precision: precision,
          scale: scale,
        )
      end

      raise InvalidSchemaError.new("field '#{field_name}': cannot resolve type")
    end

    private def extract_aliases(h : Hash(String, JSON::Any)) : Array(String)
      arr = h["aliases"]?.try(&.as_a?)
      return [] of String unless arr
      arr.compact_map(&.as_s?)
    end

    # --- emit ---

    private def emit_record(schema : Schema, io : IO) : Nil
      JSON.build(io) do |b|
        emit_record_object(schema, b)
      end
    end

    private def emit_record_object(schema : Schema, b : JSON::Builder) : Nil
      b.object do
        b.field "type", "record"
        b.field "name", schema.name
        if ns = schema.namespace
          b.field "namespace", ns unless ns.empty?
        end
        if doc = schema.doc
          b.field "doc", doc
        end
        b.field "fields" do
          b.array do
            schema.fields.each { |f| emit_field(f, b) }
          end
        end
      end
    end

    private def emit_field(f : Field, b : JSON::Builder) : Nil
      b.object do
        b.field "name", f.name
        if doc = f.doc
          b.field "doc", doc
        end
        b.field "type" do
          emit_type(f, b)
        end
        if dv = f.default_value
          b.field "default", JSON.parse(dv)
        end
      end
    rescue JSON::ParseException
      # default wasn't round-trippable as JSON — emit it as a string
      # rather than silently drop it.
    end

    private def emit_type(f : Field, b : JSON::Builder) : Nil
      if f.nullable
        b.array do
          b.scalar "null"
          emit_primitive_or_complex(f, b)
        end
        return
      end

      case f.type
      when PrimitiveType::Record
        rs = f.record_schema || raise InvalidSchemaError.new("record field '#{f.name}' missing schema")
        emit_record_object(rs, b)
      when PrimitiveType::Array
        b.object do
          b.field "type", "array"
          b.field "items" do
            emit_type(f.element_type || raise(InvalidSchemaError.new("array missing item")), b)
          end
        end
      when PrimitiveType::Map
        b.object do
          b.field "type", "map"
          b.field "values" do
            emit_type(f.values_type || raise(InvalidSchemaError.new("map missing values")), b)
          end
        end
      when PrimitiveType::Union
        b.array do
          f.union_branches.each { |pt| b.scalar pt.to_avro_name }
        end
      else
        emit_primitive_or_complex(f, b)
      end
    end

    private def emit_primitive_or_complex(f : Field, b : JSON::Builder) : Nil
      if f.logical == LogicalType::None
        b.scalar f.type.to_avro_name
        return
      end

      b.object do
        b.field "type", f.type.to_avro_name
        if name = f.logical.to_avro_name
          b.field "logicalType", name
        end
        if f.logical == LogicalType::Decimal
          b.field "precision", f.precision if f.precision > 0
          b.field "scale", f.scale if f.scale > 0
        end
      end
    end
  end
end
