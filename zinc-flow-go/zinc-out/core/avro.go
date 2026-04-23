package core

import (
	"fmt"
)

type FieldType int

const (
	NULL FieldType = iota
	BOOLEAN
	INT
	LONG
	FLOAT
	DOUBLE
	STRING
	BYTES
	ARRAY
	MAP
	RECORD
	ENUM
	UNION
)

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/avro.zn:23
type Field struct {
	Name string
	FieldType FieldType
	DefaultValue interface{}
}

func NewField(name string, fieldType FieldType, defaultValue interface{}) Field {
	return Field{Name: name, FieldType: fieldType, DefaultValue: defaultValue}
}

func (s Field) String() string {
	return fmt.Sprintf("Field(name=%v, fieldType=%v, defaultValue=%v)", s.Name, s.FieldType, s.DefaultValue)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/avro.zn:30
type Schema struct {
	Name string
	Fields []Field
}

func NewSchema(name string, fields []Field) Schema {
	return Schema{Name: name, Fields: fields}
}

func (s Schema) String() string {
	return fmt.Sprintf("Schema(name=%v, fields=%v)", s.Name, s.Fields)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/avro.zn:37
type GenericRecord struct {
	recordSchema Schema
	values map[string]interface{}
}

func NewGenericRecord(recordSchema Schema) *GenericRecord {
	return &GenericRecord{recordSchema: recordSchema, values: map[string]interface{}{}}
}

func (s *GenericRecord) SetField(key string, value interface{}) {
	s.values[key] = value
}

func (s *GenericRecord) GetField(key string) interface{} {
	if func() bool { _, _ok := s.values[key]; return _ok }() {
		return s.values[key]
	}
	return nil
}

func (s *GenericRecord) GetSchema() Schema {
	return s.recordSchema
}


//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/avro.zn:62
func SchemaFromFields(name string, fields []Field) Schema {
	return NewSchema(name, fields)
}

