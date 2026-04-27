package model

import (
	"fmt"
	"zinc-flow/core"
)

//line /home/vrjoshi/proj/zinc-flow/src/fabric/model/result.zn:8
func SerializeResult(result core.ProcessorResult) string {
	switch _v := result.(type) {
	case core.Single:
		ff := _v.Ff
		return "{\"type\":\"single\",\"id\":\"" + ff.Id + "\"}"
	case core.Multiple:
		ffs := _v.Ffs
		return "{\"type\":\"multiple\",\"count\":" + fmt.Sprint(len(ffs)) + "}"
	case core.Dropped:
		reason := _v.Reason
		return "{\"type\":\"dropped\",\"reason\":\"" + reason + "\"}"
	case core.Routed:
		route := _v.Route
		ff := _v.Ff
		return "{\"type\":\"routed\",\"route\":\"" + route + "\",\"id\":\"" + ff.Id + "\"}"
	case core.Failure:
		reason := _v.Reason
		ff := _v.Ff
		return "{\"type\":\"failure\",\"reason\":\"" + reason + "\",\"id\":\"" + ff.Id + "\"}"
	default:
		_ = _v
		panic("unreachable")
	}
	return ""
}

