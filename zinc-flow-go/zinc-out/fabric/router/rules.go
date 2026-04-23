package router

import (
	"fmt"
)

type Operator int

const (
	EQ Operator = iota
	NEQ
	GT
	LT
	CONTAINS
	STARTSWITH
	ENDSWITH
	EXISTS
)

type Joiner int

const (
	AND Joiner = iota
	OR
)

type RuleType interface {
	isRuleType()
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/fabric/router/rules.zn:25
type BaseRule struct {
	Attribute string
	Operator Operator
	Value string
}

func NewBaseRule(attribute string, operator Operator, value string) BaseRule {
	return BaseRule{Attribute: attribute, Operator: operator, Value: value}
}

func (s BaseRule) String() string {
	return fmt.Sprintf("BaseRule(attribute=%v, operator=%v, value=%v)", s.Attribute, s.Operator, s.Value)
}

func (BaseRule) isRuleType() {}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/fabric/router/rules.zn:28
type CompositeRule struct {
	Left RuleType
	Joiner Joiner
	Right RuleType
}

func NewCompositeRule(left RuleType, joiner Joiner, right RuleType) CompositeRule {
	return CompositeRule{Left: left, Joiner: joiner, Right: right}
}

func (s CompositeRule) String() string {
	return fmt.Sprintf("CompositeRule(left=%v, joiner=%v, right=%v)", s.Left, s.Joiner, s.Right)
}

func (CompositeRule) isRuleType() {}


//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/fabric/router/rules.zn:32
type Destination struct {
	Endpoint string
	AdapterType string
}

func NewDestination(endpoint string, adapterType string) Destination {
	return Destination{Endpoint: endpoint, AdapterType: adapterType}
}

func (s Destination) String() string {
	return fmt.Sprintf("Destination(endpoint=%v, adapterType=%v)", s.Endpoint, s.AdapterType)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/fabric/router/rules.zn:38
type RoutingRule struct {
	Name string
	Condition RuleType
	Destination Destination
	Enabled bool
}

func NewRoutingRule(name string, condition RuleType, destination Destination, enabled bool) RoutingRule {
	return RoutingRule{Name: name, Condition: condition, Destination: destination, Enabled: enabled}
}

func (s RoutingRule) String() string {
	return fmt.Sprintf("RoutingRule(name=%v, condition=%v, destination=%v, enabled=%v)", s.Name, s.Condition, s.Destination, s.Enabled)
}

