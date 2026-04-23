package router

import (
	"strings"
	"sync"
)

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/fabric/router/engine.zn:7
type RulesEngine struct {
	rulesets map[string][]RoutingRule
	mu sync.Mutex
}

func NewRulesEngine() *RulesEngine {
	return &RulesEngine{rulesets: map[string][]RoutingRule{}}
}

func (s *RulesEngine) AddOrReplaceRuleset(name string, rules []RoutingRule) {
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.rulesets[name] = rules
	}()
}

func (s *RulesEngine) RemoveRuleset(name string) {
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.rulesets, name)
	}()
}

func (s *RulesEngine) ToggleRule(rulesetName string, ruleName string) {
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		rules := s.rulesets[rulesetName]
		updated := []RoutingRule{}
		for _, r := range rules {
			if r.Name == ruleName {
				updated = append(updated, NewRoutingRule(r.Name, r.Condition, r.Destination, !r.Enabled))
			} else {
				updated = append(updated, r)
			}
		}
		s.rulesets[rulesetName] = updated
	}()
}

func (s *RulesEngine) GetRulesetNames() []string {
	return func() []string { _keys := make([]string, 0, len(s.rulesets)); for _k := range s.rulesets { _keys = append(_keys, _k) }; return _keys }()
}

func (s *RulesEngine) GetAllRules() []RoutingRule {
	result := []RoutingRule{}
	names := func() []string { _keys := make([]string, 0, len(s.rulesets)); for _k := range s.rulesets { _keys = append(_keys, _k) }; return _keys }()
	for _, name := range names {
		rules := s.rulesets[name]
		for _, r := range rules {
			result = append(result, r)
		}
	}
	return result
}

func (s *RulesEngine) GetDestinations(attributes map[string]string) []Destination {
	destinations := []Destination{}
	names := func() []string { _keys := make([]string, 0, len(s.rulesets)); for _k := range s.rulesets { _keys = append(_keys, _k) }; return _keys }()
	for _, name := range names {
		rules := s.rulesets[name]
		for _, rule := range rules {
			if rule.Enabled && evaluate(rule.Condition, attributes) {
				destinations = append(destinations, rule.Destination)
			}
		}
	}
	return destinations
}


//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/fabric/router/engine.zn:79
func evaluate(condition RuleType, attributes map[string]string) bool {
	switch _v := condition.(type) {
	case BaseRule:
		attribute := _v.Attribute
		operator := _v.Operator
		value := _v.Value
		return evaluateBase(attribute, operator, value, attributes)
	case CompositeRule:
		left := _v.Left
		joiner := _v.Joiner
		right := _v.Right
		leftResult := evaluate(left, attributes)
		switch joiner {
		case AND:
			return leftResult && evaluate(right, attributes)
		case OR:
			return leftResult || evaluate(right, attributes)
		}
	default:
		_ = _v
		panic("unreachable")
	}
	return false
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/fabric/router/engine.zn:100
func evaluateBase(attribute string, operator Operator, value string, attributes map[string]string) bool {
	switch operator {
	case EXISTS:
		return func() bool { _, _ok := attributes[attribute]; return _ok }()
	}
	if !func() bool { _, _ok := attributes[attribute]; return _ok }() {
		return false
	}
	actual := attributes[attribute]
	switch operator {
	case EQ:
		return actual == value
	case NEQ:
		return actual != value
	case CONTAINS:
		return strings.Contains(actual, value)
	case STARTSWITH:
		return strings.HasPrefix(actual, value)
	case ENDSWITH:
		return strings.HasSuffix(actual, value)
	case GT:
		return actual > value
	case LT:
		return actual < value
	}
	return false
}

