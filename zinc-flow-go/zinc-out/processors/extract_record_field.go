package processors

import (
	"fmt"
	"zinc-flow/core"
)

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/extract_record_field.zn:27
type FieldMapping struct {
	Field string
	Attr string
}

func NewFieldMapping(field string, attr string) FieldMapping {
	return FieldMapping{Field: field, Attr: attr}
}

func (s FieldMapping) String() string {
	return fmt.Sprintf("FieldMapping(field=%v, attr=%v)", s.Field, s.Attr)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/extract_record_field.zn:29
type ExtractRecordField struct {
	mappings []FieldMapping
	recordIndex int
}

func NewExtractRecordField(fieldsSpec string, recordIndex int) *ExtractRecordField {
	s := &ExtractRecordField{recordIndex: recordIndex, mappings: []FieldMapping{}}
	s.parseFields(fieldsSpec)
	return s
}

func (s *ExtractRecordField) parseFields(spec string) {
	if spec == "" {
		return
	}
	start := 0
	i := 0
	for i < len(spec) {
		if spec[i:i + 1] == ";" {
			s.addPair(spec[start:i])
			start = i + 1
		}
		i = i + 1
	}
	if start < len(spec) {
		s.addPair(spec[start:len(spec)])
	}
}

func (s *ExtractRecordField) addPair(pair string) {
	trimmed := trimPairSpace(pair)
	if trimmed == "" {
		return
	}
	colonIdx := -1
	j := 0
	for j < len(trimmed) {
		if trimmed[j:j + 1] == ":" {
			colonIdx = j
			break
		}
		j = j + 1
	}
	if colonIdx <= 0 || colonIdx == len(trimmed) - 1 {
		return
	}
	fname := trimPairSpace(trimmed[0:colonIdx])
	aname := trimPairSpace(trimmed[colonIdx + 1:len(trimmed)])
	if fname == "" || aname == "" {
		return
	}
	s.mappings = append(s.mappings, NewFieldMapping(fname, aname))
}

func (s *ExtractRecordField) Process(ff core.FlowFile) core.ProcessorResult {
	switch _v := ff.Content.(type) {
	case core.Records:
		records := _v.Rows
		if len(records) == 0 || s.recordIndex >= len(records) {
			return core.NewSingle(ff)
		}
		record := records[s.recordIndex]
		result := ff
		for _, m := range s.mappings {
			val := record.GetField(m.Field)
			if val != nil {
				result = core.WithAttribute(result, m.Attr, fmt.Sprint(val))
			}
		}
		return core.NewSingle(result)
	default:
		_ = _v
		return core.NewSingle(ff)
	}
	return nil
}


//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/extract_record_field.zn:110
func ExtractRecordFieldFactory(ctx *core.ScopedContext, config map[string]string) core.ProcessorFn {
	fieldsSpec := ""
	if func() bool { _, _ok := config["fields"]; return _ok }() {
		fieldsSpec = config["fields"]
	}
	idx := 0
	if func() bool { _, _ok := config["record_index"]; return _ok }() {
		idx = parseIntOr(config["record_index"], 0)
	}
	return NewExtractRecordField(fieldsSpec, idx)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/extract_record_field.zn:125
func parseIntOr(s string, fallback int) int {
	if s == "" {
		return fallback
	}
	neg := false
	start := 0
	if s[0:1] == "-" {
		neg = true
		start = 1
	}
	n := 0
	i := start
	for i < len(s) {
		c := s[i:i + 1]
		if c < "0" || c > "9" {
			return fallback
		}
		n = n * 10 + int(byte(s[i])) - 48
		i = i + 1
	}
	if neg {
		return 0 - n
	}
	return n
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/extract_record_field.zn:151
func trimPairSpace(s string) string {
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

