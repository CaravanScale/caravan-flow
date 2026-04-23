package processors

import (
	"zinc-flow/core"
)

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/update_attribute.zn:17
type UpdateAttribute struct {
	key string
	value string
}

func NewUpdateAttribute(key string, value string) *UpdateAttribute {
	return &UpdateAttribute{key: key, value: value}
}

func (s *UpdateAttribute) Process(ff core.FlowFile) core.ProcessorResult {
	updated := core.WithAttribute(ff, s.key, s.value)
	return core.NewSingle(updated)
}


//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/update_attribute.zn:32
func UpdateAttributeFactory(ctx *core.ScopedContext, config map[string]string) core.ProcessorFn {
	return NewUpdateAttribute(config["key"], config["value"])
}

