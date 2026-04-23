package processors

import (
	"fmt"
	"zinc-flow/core"
	"strings"
)

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/route_on_attribute.zn:25
type Route struct {
	Relationship string
	Attribute string
	Op string
	Value string
}

func NewRoute(relationship string, attribute string, op string, value string) Route {
	return Route{Relationship: relationship, Attribute: attribute, Op: op, Value: value}
}

func (s Route) String() string {
	return fmt.Sprintf("Route(relationship=%v, attribute=%v, op=%v, value=%v)", s.Relationship, s.Attribute, s.Op, s.Value)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/route_on_attribute.zn:27
type RouteOnAttribute struct {
	routes []Route
}

func NewRouteOnAttribute(routesSpec string) *RouteOnAttribute {
	s := &RouteOnAttribute{routes: []Route{}}
	s.parseRoutes(routesSpec)
	return s
}

func (s *RouteOnAttribute) parseRoutes(spec string) {
	if spec == "" {
		return
	}
	entries := splitAndTrim(spec, ";")
	for _, entry := range entries {
		colonIdx := indexOfByte(entry, ":")
		if colonIdx <= 0 {
			continue
		}
		name := trimSpace(entry[0:colonIdx])
		conditionStr := trimSpace(entry[colonIdx + 1:len(entry)])
		parts := splitOnSpace(conditionStr, 3)
		if len(parts) < 2 {
			continue
		}
		attribute := parts[0]
		op := upperCase(parts[1])
		value := ""
		if len(parts) >= 3 {
			value = parts[2]
		}
		s.routes = append(s.routes, NewRoute(name, attribute, op, value))
	}
}

func (s *RouteOnAttribute) Process(ff core.FlowFile) core.ProcessorResult {
	for _, route := range s.routes {
		if evaluate(route, ff.Attributes) {
			return core.NewRouted(route.Relationship, ff)
		}
	}
	return core.NewRouted("unmatched", ff)
}


//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/route_on_attribute.zn:75
func RouteOnAttributeFactory(ctx *core.ScopedContext, config map[string]string) core.ProcessorFn {
	spec := ""
	if func() bool { _, _ok := config["routes"]; return _ok }() {
		spec = config["routes"]
	}
	return NewRouteOnAttribute(spec)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/route_on_attribute.zn:85
func evaluate(r Route, attrs map[string]string) bool {
	if r.Op == "EXISTS" {
		return func() bool { _, _ok := attrs[r.Attribute]; return _ok }()
	}
	if !func() bool { _, _ok := attrs[r.Attribute]; return _ok }() {
		return false
	}
	actual := attrs[r.Attribute]
	if r.Op == "EQ" {
		return actual == r.Value
	}
	if r.Op == "NEQ" {
		return actual != r.Value
	}
	if r.Op == "CONTAINS" {
		return strings.Contains(actual, r.Value)
	}
	if r.Op == "STARTSWITH" {
		return strings.HasPrefix(actual, r.Value)
	}
	if r.Op == "ENDSWITH" {
		return strings.HasSuffix(actual, r.Value)
	}
	if r.Op == "GT" {
		return actual > r.Value
	}
	if r.Op == "LT" {
		return actual < r.Value
	}
	return false
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/route_on_attribute.zn:119
func splitAndTrim(s string, sep string) []string {
	out := []string{}
	start := 0
	i := 0
	for i < len(s) {
		if s[i:i + 1] == sep {
			part := trimSpace(s[start:i])
			if part != "" {
				out = append(out, part)
			}
			start = i + 1
		}
		i = i + 1
	}
	tail := trimSpace(s[start:len(s)])
	if tail != "" {
		out = append(out, tail)
	}
	return out
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/route_on_attribute.zn:140
func indexOfByte(s string, b string) int {
	i := 0
	for i < len(s) {
		if s[i:i + 1] == b {
			return i
		}
		i = i + 1
	}
	return -1
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/route_on_attribute.zn:151
func trimSpace(s string) string {
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

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/route_on_attribute.zn:171
func splitOnSpace(s string, maxParts int) []string {
	out := []string{}
	start := 0
	i := 0
	for i < len(s) && len(out) < maxParts - 1 {
		c := s[i:i + 1]
		if c == " " || c == "\t" {
			if i > start {
				out = append(out, s[start:i])
			}
			for i < len(s) && (s[i:i + 1] == " " || s[i:i + 1] == "\t") {
				i = i + 1
			}
			start = i
			continue
		}
		i = i + 1
	}
	if start < len(s) {
		out = append(out, s[start:len(s)])
	}
	return out
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/route_on_attribute.zn:196
func upperCase(s string) string {
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

