package processors

import (
	"fmt"
	"zinc-flow/core"
)

//line /home/vrjoshi/proj/zinc-flow/src/processors/transform_record.zn:30
type RecordOp struct {
	Op string
	Arg1 string
	Arg2 string
}

func NewRecordOp(op string, arg1 string, arg2 string) RecordOp {
	return RecordOp{Op: op, Arg1: arg1, Arg2: arg2}
}

func (s RecordOp) String() string {
	return fmt.Sprintf("RecordOp(op=%v, arg1=%v, arg2=%v)", s.Op, s.Arg1, s.Arg2)
}

//line /home/vrjoshi/proj/zinc-flow/src/processors/transform_record.zn:32
type TransformRecord struct {
	operations []RecordOp
}

func NewTransformRecord(operationsSpec string) *TransformRecord {
	s := &TransformRecord{operations: []RecordOp{}}
	s.parseOperations(operationsSpec)
	return s
}

func (s *TransformRecord) parseOperations(spec string) {
	if spec == "" {
		return
	}
	start := 0
	i := 0
	for i < len(spec) {
		if spec[i:i + 1] == ";" {
			s.addEntry(spec[start:i])
			start = i + 1
		}
		i = i + 1
	}
	if start < len(spec) {
		s.addEntry(spec[start:len(spec)])
	}
}

func (s *TransformRecord) addEntry(entry string) {
	trimmed := trimOpSpace(entry)
	if trimmed == "" {
		return
	}
	parts := []string{}
	partStart := 0
	maxParts := 3
	j := 0
	for j < len(trimmed) && len(parts) < maxParts - 1 {
		if trimmed[j:j + 1] == ":" {
			parts = append(parts, trimOpSpace(trimmed[partStart:j]))
			partStart = j + 1
		}
		j = j + 1
	}
	parts = append(parts, trimOpSpace(trimmed[partStart:len(trimmed)]))
	if len(parts) == 0 || parts[0] == "" {
		return
	}
	arg1 := ""
	arg2 := ""
	if len(parts) >= 2 {
		arg1 = parts[1]
	}
	if len(parts) >= 3 {
		arg2 = parts[2]
	}
	s.operations = append(s.operations, NewRecordOp(parts[0], arg1, arg2))
}

func (s *TransformRecord) Process(ff core.FlowFile) core.ProcessorResult {
	switch _v := ff.Content.(type) {
	case core.Records:
		schema := _v.Schema
		records := _v.Rows
		if len(records) == 0 {
			return core.NewSingle(ff)
		}
		transformed := []*core.GenericRecord{}
		for _, record := range records {
			transformed = append(transformed, s.applyOps(record, schema))
		}
		newSchema := rebuildSchema(schema, transformed[0])
		updated := core.WithContent(ff, core.NewRecords(newSchema, transformed))
		return core.NewSingle(updated)
	default:
		_ = _v
		return core.NewSingle(ff)
	}
	return nil
}

func (s *TransformRecord) applyOps(record *core.GenericRecord, schema core.Schema) *core.GenericRecord {
	values := map[string]interface{}{}
	for _, f := range schema.Fields {
		v := record.GetField(f.Name)
		if v != nil {
			values[f.Name] = v
		}
	}
	for _, op := range s.operations {
		s.applyOne(values, op)
	}
	fields := []core.Field{}
	origByName := map[string]core.FieldType{}
	for _, f := range schema.Fields {
		origByName[f.Name] = f.FieldType
	}
	for _, k := range func() []string { _keys := make([]string, 0, len(values)); for _k := range values { _keys = append(_keys, _k) }; return _keys }() {
		ft := core.STRING
		if func() bool { _, _ok := origByName[k]; return _ok }() {
			ft = origByName[k]
		}
		fields = append(fields, core.NewField(k, ft, nil))
	}
	outSchema := core.NewSchema(schema.Name, fields)
	out := core.NewGenericRecord(outSchema)
	for _, k := range func() []string { _keys := make([]string, 0, len(values)); for _k := range values { _keys = append(_keys, _k) }; return _keys }() {
		out.SetField(k, values[k])
	}
	return out
}

func (s *TransformRecord) applyOne(values map[string]interface{}, op RecordOp) {
	if op.Op == "rename" {
		if func() bool { _, _ok := values[op.Arg1]; return _ok }() {
			values[op.Arg2] = values[op.Arg1]
			delete(values, op.Arg1)
		}
		return
	}
	if op.Op == "remove" {
		delete(values, op.Arg1)
		return
	}
	if op.Op == "add" {
		values[op.Arg1] = op.Arg2
		return
	}
	if op.Op == "copy" {
		if func() bool { _, _ok := values[op.Arg1]; return _ok }() {
			values[op.Arg2] = values[op.Arg1]
		}
		return
	}
	if op.Op == "toUpper" {
		if func() bool { _, _ok := values[op.Arg1]; return _ok }() {
			v := values[op.Arg1]
			if v != nil {
				values[op.Arg1] = asciiUpper(fmt.Sprint(v))
			}
		}
		return
	}
	if op.Op == "toLower" {
		if func() bool { _, _ok := values[op.Arg1]; return _ok }() {
			v := values[op.Arg1]
			if v != nil {
				values[op.Arg1] = asciiLower(fmt.Sprint(v))
			}
		}
		return
	}
	if op.Op == "default" {
		if !func() bool { _, _ok := values[op.Arg1]; return _ok }() || values[op.Arg1] == nil {
			values[op.Arg1] = op.Arg2
		}
		return
	}
}


//line /home/vrjoshi/proj/zinc-flow/src/processors/transform_record.zn:203
func TransformRecordFactory(ctx *core.ScopedContext, config map[string]string) core.ProcessorFn {
	spec := ""
	if func() bool { _, _ok := config["operations"]; return _ok }() {
		spec = config["operations"]
	}
	return NewTransformRecord(spec)
}

//line /home/vrjoshi/proj/zinc-flow/src/processors/transform_record.zn:215
func rebuildSchema(orig core.Schema, first *core.GenericRecord) core.Schema {
	origFields := map[string]core.FieldType{}
	for _, f := range orig.Fields {
		origFields[f.Name] = f.FieldType
	}
	firstSchema := first.GetSchema()
	newFields := []core.Field{}
	for _, f := range firstSchema.Fields {
		ft := f.FieldType
		if func() bool { _, _ok := origFields[f.Name]; return _ok }() {
			ft = origFields[f.Name]
		}
		newFields = append(newFields, core.NewField(f.Name, ft, nil))
	}
	return core.NewSchema(orig.Name, newFields)
}

//line /home/vrjoshi/proj/zinc-flow/src/processors/transform_record.zn:232
func trimOpSpace(s string) string {
	start := 0
	for start < len(s) {
		c := s[start:start + 1]
		if c != " " && c != "\t" {
			break
		}
		start = start + 1
	}
	stop := len(s)
	for stop > start {
		c := s[stop - 1:stop]
		if c != " " && c != "\t" {
			break
		}
		stop = stop - 1
	}
	return s[start:stop]
}

//line /home/vrjoshi/proj/zinc-flow/src/processors/transform_record.zn:252
func asciiUpper(s string) string {
	out := ""
	i := 0
	for i < len(s) {
		c := s[i:i + 1]
		if c >= "a" && c <= "z" {
			b := byte(s[i])
			out = out + string(byte(b - 32))
		} else {
			out = out + c
		}
		i = i + 1
	}
	return out
}

//line /home/vrjoshi/proj/zinc-flow/src/processors/transform_record.zn:268
func asciiLower(s string) string {
	out := ""
	i := 0
	for i < len(s) {
		c := s[i:i + 1]
		if c >= "A" && c <= "Z" {
			b := byte(s[i])
			out = out + string(byte(b + 32))
		} else {
			out = out + c
		}
		i = i + 1
	}
	return out
}

