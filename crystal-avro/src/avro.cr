# Apache Avro 1.11 for Crystal. Hand-rolled, no reflection,
# AOT-safe, works under preview_mt.
#
# Entry points:
#   Avro::SchemaJson.parse(json) / .emit(schema)
#   Avro::BinaryCodec.write_record(io, record, schema) / .read_record(io, schema)
#
# OCF (Object Container File) and schema-evolution resolver land in
# follow-up versions; their stubs are documented in README.md.

require "./avro/schema"
require "./avro/json"
require "./avro/binary"
require "./avro/codec"
require "./avro/ocf"

module Avro
  VERSION = "0.2.0"
end
