package processors

import (
	"zinc-flow/core"
	"github.com/ZincScale/zinc-stdlib/logging"
)

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/log_attribute.zn:17
type LogAttribute struct {
	prefix string
}

func NewLogAttribute(prefix string) *LogAttribute {
	return &LogAttribute{prefix: prefix}
}

func (s *LogAttribute) Process(ff core.FlowFile) core.ProcessorResult {
	logging.Info("flowfile", "processor", s.prefix, "id", ff.Id)
	for _, k := range func() []string { _keys := make([]string, 0, len(ff.Attributes)); for _k := range ff.Attributes { _keys = append(_keys, _k) }; return _keys }() {
		logging.Info("flowfile.attr", "processor", s.prefix, "id", ff.Id, "key", k, "value", ff.Attributes[k])
	}
	return core.NewSingle(ff)
}


//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/log_attribute.zn:41
func LogAttributeFactory(ctx *core.ScopedContext, config map[string]string) core.ProcessorFn {
	prefix := "flow"
	if func() bool { _, _ok := config["prefix"]; return _ok }() && config["prefix"] != "" {
		prefix = config["prefix"]
	}
	return NewLogAttribute(prefix)
}

