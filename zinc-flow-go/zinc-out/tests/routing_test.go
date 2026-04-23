package tests

import (
	"zinc-flow/fabric/router"
	"github.com/ZincScale/zinc-stdlib/asserts"
	"testing"
)

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/routing_test.zn:8
func TestRoutingEQAttributeEqualityRoutesCorrectly(t *testing.T) {
	engine := router.NewRulesEngine()
	rule := router.NewRoutingRule("match-orders", router.NewBaseRule("type", router.EQ, "order"), router.NewDestination("proc-1", "local"), true)
	engine.AddOrReplaceRuleset("test", []router.RoutingRule{rule})
	attrs := map[string]string{"type": "order"}
	dests := engine.GetDestinations(attrs)
	asserts.EqualInt(t, len(dests), 1)
	asserts.EqualString(t, dests[0].Endpoint, "proc-1")
	noMatch := map[string]string{"type": "event"}
	asserts.EqualInt(t, len(engine.GetDestinations(noMatch)), 0)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/routing_test.zn:22
func TestRoutingNEQInequalityRoutesCorrectly(t *testing.T) {
	engine := router.NewRulesEngine()
	rule := router.NewRoutingRule("not-orders", router.NewBaseRule("type", router.NEQ, "order"), router.NewDestination("proc-2", "local"), true)
	engine.AddOrReplaceRuleset("test", []router.RoutingRule{rule})
	attrs := map[string]string{"type": "event"}
	asserts.EqualInt(t, len(engine.GetDestinations(attrs)), 1)
	noMatch := map[string]string{"type": "order"}
	asserts.EqualInt(t, len(engine.GetDestinations(noMatch)), 0)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/routing_test.zn:34
func TestRoutingCONTAINSSubstringMatchRoutesCorrectly(t *testing.T) {
	engine := router.NewRulesEngine()
	rule := router.NewRoutingRule("has-error", router.NewBaseRule("message", router.CONTAINS, "error"), router.NewDestination("dlq", "local"), true)
	engine.AddOrReplaceRuleset("test", []router.RoutingRule{rule})
	attrs := map[string]string{"message": "got an error here"}
	asserts.EqualInt(t, len(engine.GetDestinations(attrs)), 1)
	noMatch := map[string]string{"message": "all good"}
	asserts.EqualInt(t, len(engine.GetDestinations(noMatch)), 0)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/routing_test.zn:46
func TestRoutingSTARTSWITHENDSWITHPrefixSuffixRulesCombine(t *testing.T) {
	engine := router.NewRulesEngine()
	r1 := router.NewRoutingRule("starts", router.NewBaseRule("path", router.STARTSWITH, "/api"), router.NewDestination("api-proc", "local"), true)
	r2 := router.NewRoutingRule("ends", router.NewBaseRule("path", router.ENDSWITH, ".json"), router.NewDestination("json-proc", "local"), true)
	engine.AddOrReplaceRuleset("test", []router.RoutingRule{r1, r2})
	both := map[string]string{"path": "/api/data.json"}
	asserts.EqualInt(t, len(engine.GetDestinations(both)), 2)
	onlyStarts := map[string]string{"path": "/api/data.xml"}
	startsOnly := engine.GetDestinations(onlyStarts)
	asserts.EqualInt(t, len(startsOnly), 1)
	asserts.EqualString(t, startsOnly[0].Endpoint, "api-proc")
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/routing_test.zn:61
func TestRoutingEXISTSAttributePresenceNotValue(t *testing.T) {
	engine := router.NewRulesEngine()
	rule := router.NewRoutingRule("has-priority", router.NewBaseRule("priority", router.EXISTS, ""), router.NewDestination("priority-proc", "local"), true)
	engine.AddOrReplaceRuleset("test", []router.RoutingRule{rule})
	attrs := map[string]string{"priority": "high"}
	asserts.EqualInt(t, len(engine.GetDestinations(attrs)), 1)
	noAttr := map[string]string{"type": "order"}
	asserts.EqualInt(t, len(engine.GetDestinations(noAttr)), 0)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/routing_test.zn:73
func TestRoutingCompositeANDORCombinedPredicates(t *testing.T) {
	andEngine := router.NewRulesEngine()
	andRule := router.NewRoutingRule("web-orders", router.NewCompositeRule(router.NewBaseRule("type", router.EQ, "order"), router.AND, router.NewBaseRule("source", router.EQ, "web")), router.NewDestination("web-order-proc", "local"), true)
	andEngine.AddOrReplaceRuleset("test", []router.RoutingRule{andRule})
	both := map[string]string{"type": "order", "source": "web"}
	asserts.EqualInt(t, len(andEngine.GetDestinations(both)), 1)
	onlyType := map[string]string{"type": "order", "source": "api"}
	asserts.EqualInt(t, len(andEngine.GetDestinations(onlyType)), 0)
	orEngine := router.NewRulesEngine()
	orRule := router.NewRoutingRule("any-source", router.NewCompositeRule(router.NewBaseRule("source", router.EQ, "web"), router.OR, router.NewBaseRule("source", router.EQ, "mobile")), router.NewDestination("any-proc", "local"), true)
	orEngine.AddOrReplaceRuleset("test", []router.RoutingRule{orRule})
	web := map[string]string{"source": "web"}
	asserts.EqualInt(t, len(orEngine.GetDestinations(web)), 1)
	mobile := map[string]string{"source": "mobile"}
	asserts.EqualInt(t, len(orEngine.GetDestinations(mobile)), 1)
	api := map[string]string{"source": "api"}
	asserts.EqualInt(t, len(orEngine.GetDestinations(api)), 0)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/routing_test.zn:110
func TestRoutingNoMatchMissingAttributeDisabledRulesAreSkipped(t *testing.T) {
	engine := router.NewRulesEngine()
	rule := router.NewRoutingRule("specific", router.NewBaseRule("type", router.EQ, "order"), router.NewDestination("proc-1", "local"), true)
	engine.AddOrReplaceRuleset("test", []router.RoutingRule{rule})
	attrs := map[string]string{"source": "test"}
	asserts.EqualInt(t, len(engine.GetDestinations(attrs)), 0)
	disabledRule := router.NewRoutingRule("disabled", router.NewBaseRule("type", router.EQ, "order"), router.NewDestination("proc-2", "local"), false)
	engine.AddOrReplaceRuleset("disabled-set", []router.RoutingRule{disabledRule})
	orderAttrs := map[string]string{"type": "order"}
	orderDests := engine.GetDestinations(orderAttrs)
	asserts.EqualInt(t, len(orderDests), 1)
	asserts.EqualString(t, orderDests[0].Endpoint, "proc-1")
}

