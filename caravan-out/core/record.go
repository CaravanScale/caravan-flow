package core

import (
	"fmt"
)

type RecordReader interface {
	Read(data []byte, schema Schema) []*GenericRecord
}

type RecordWriter interface {
	Write(records []*GenericRecord, schema Schema) []byte
}

type RecordProcessor interface {
	ProcessRecord(record *GenericRecord, schema Schema) *GenericRecord
}

//line /home/vrjoshi/proj/zinc-flow/src/core/record.zn:23
func NewRecord(schema Schema, values map[string]interface{}) *GenericRecord {
	record := NewGenericRecord(schema)
	for key, value := range values {
		record.SetField(key, value)
	}
	return record
}

//line /home/vrjoshi/proj/zinc-flow/src/core/record.zn:32
func GetField(record *GenericRecord, fieldName string) string {
	val := record.GetField(fieldName)
	if val == nil {
		return ""
	}
	return fmt.Sprint(val)
}

//line /home/vrjoshi/proj/zinc-flow/src/core/record.zn:41
func WithField(record *GenericRecord, fieldName string, value interface{}) *GenericRecord {
	schema := record.GetSchema()
	_copy := NewGenericRecord(schema)
	for _, f := range schema.Fields {
		_copy.SetField(f.Name, record.GetField(f.Name))
	}
	_copy.SetField(fieldName, value)
	return _copy
}

