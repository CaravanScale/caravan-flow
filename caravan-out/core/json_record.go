package core

import (
	"encoding/json"
)

//line /home/vrjoshi/proj/zinc-flow/src/core/json_record.zn:7
type JsonRecordReader struct {
}

func NewJsonRecordReader() *JsonRecordReader {
	return &JsonRecordReader{}
}

func (s *JsonRecordReader) Read(data []byte, schema Schema) []*GenericRecord {
	rawList := []map[string]interface{}{}
	_tryerr := func() error {
		err1 := json.Unmarshal(data, &rawList)
		if err1 != nil {
			return err1
		}
		return nil
	}()
	if _tryerr != nil {
		e := _tryerr
		_ = e
		return []*GenericRecord{}
	}
	effectiveSchema := schema
	if len(schema.Fields) == 0 && len(rawList) > 0 {
		effectiveSchema = inferSchema(schema.Name, rawList[0])
	}
	records := []*GenericRecord{}
	for _, raw := range rawList {
		record := NewGenericRecord(effectiveSchema)
		for _, f := range effectiveSchema.Fields {
			if func() bool { _, _ok := raw[f.Name]; return _ok }() {
				record.SetField(f.Name, raw[f.Name])
			} else 			if f.DefaultValue != nil {
				record.SetField(f.Name, f.DefaultValue)
			}
		}
		records = append(records, record)
	}
	return records
}


//line /home/vrjoshi/proj/zinc-flow/src/core/json_record.zn:44
type JsonRecordWriter struct {
}

func NewJsonRecordWriter() *JsonRecordWriter {
	return &JsonRecordWriter{}
}

func (s *JsonRecordWriter) Write(records []*GenericRecord, schema Schema) []byte {
	rawList := []map[string]interface{}{}
	for _, record := range records {
		raw := map[string]interface{}{}
		for _, f := range schema.Fields {
			val := record.GetField(f.Name)
			if val != nil {
				raw[f.Name] = val
			}
		}
		rawList = append(rawList, raw)
	}
	_tryerr2_val, _tryerr2_ret, _tryerr2 := func() ([]byte, bool, error) {
		result, err3 := json.Marshal(rawList)
		if err3 != nil {
			return nil, false, err3
		}
		return result, true, nil
		return nil, false, nil
	}()
	if !_tryerr2_ret && _tryerr2 != nil {
		e := _tryerr2
		_ = e
		return make([]byte, 0)
	}
	if _tryerr2_ret {
		return _tryerr2_val
	}
	return nil
}


//line /home/vrjoshi/proj/zinc-flow/src/core/json_record.zn:73
func inferSchema(name string, sample map[string]interface{}) Schema {
	fields := []Field{}
	keys := func() []string { _keys := make([]string, 0, len(sample)); for _k := range sample { _keys = append(_keys, _k) }; return _keys }()
	for _, key := range keys {
		val := sample[key]
		ft := inferFieldType(val)
		fields = append(fields, NewField(key, ft, nil))
	}
	return NewSchema(name, fields)
}

//line /home/vrjoshi/proj/zinc-flow/src/core/json_record.zn:86
func inferFieldType(val interface{}) FieldType {
	if val == nil {
		return NULL
	}
	switch _v := val.(type) {
	case string:
		_ = _v
		return STRING
	case bool:
		_ = _v
		return BOOLEAN
	case float64:
		_ = _v
		return DOUBLE
	case int:
		_ = _v
		return INT
	case int64:
		_ = _v
		return LONG
	default:
		_ = _v
		panic("unreachable")
	}
	return STRING
}

