package processors

import (
	"zinc-flow/core"
)

//line /home/vrjoshi/proj/zinc-flow/src/processors/filter_attribute.zn:16
type FilterAttribute struct {
	removeMode bool
	attrSet map[string]bool
}

func NewFilterAttribute(mode string, attributesSpec string) *FilterAttribute {
	s := &FilterAttribute{removeMode: filterLowerAscii(mode) != "keep", attrSet: map[string]bool{}}
	if attributesSpec != "" {
		start := 0
		i := 0
		for i < len(attributesSpec) {
			if attributesSpec[i:i + 1] == ";" {
				s.addName(attributesSpec[start:i])
				start = i + 1
			}
			i = i + 1
		}
		if start < len(attributesSpec) {
			s.addName(attributesSpec[start:len(attributesSpec)])
		}
	}
	return s
}

func (s *FilterAttribute) addName(name string) {
	trimmed := trimFilterSpace(name)
	if trimmed != "" {
		s.attrSet[trimmed] = true
	}
}

func (s *FilterAttribute) Process(ff core.FlowFile) core.ProcessorResult {
	filtered := map[string]string{}
	for _, k := range func() []string { _keys := make([]string, 0, len(ff.Attributes)); for _k := range ff.Attributes { _keys = append(_keys, _k) }; return _keys }() {
		inSet := func() bool { _, _ok := s.attrSet[k]; return _ok }()
		if s.removeMode && !inSet {
			filtered[k] = ff.Attributes[k]
		}
		if !s.removeMode && inSet {
			filtered[k] = ff.Attributes[k]
		}
	}
	updated := core.NewFlowFile(ff.Id, filtered, ff.Content, ff.Timestamp)
	return core.NewSingle(updated)
}


//line /home/vrjoshi/proj/zinc-flow/src/processors/filter_attribute.zn:67
func FilterAttributeFactory(ctx *core.ScopedContext, config map[string]string) core.ProcessorFn {
	mode := ""
	if func() bool { _, _ok := config["mode"]; return _ok }() {
		mode = config["mode"]
	}
	attrs := ""
	if func() bool { _, _ok := config["attributes"]; return _ok }() {
		attrs = config["attributes"]
	}
	return NewFilterAttribute(mode, attrs)
}

//line /home/vrjoshi/proj/zinc-flow/src/processors/filter_attribute.zn:78
func filterLowerAscii(s string) string {
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

//line /home/vrjoshi/proj/zinc-flow/src/processors/filter_attribute.zn:94
func trimFilterSpace(s string) string {
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

