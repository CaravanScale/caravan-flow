package runtime

import (
	"github.com/ZincScale/zinc-stdlib/config"
	"github.com/ZincScale/zinc-stdlib/logging"
	"fmt"
	"zinc-flow/core"
	"zinc-flow/fabric/registry"
	"sync"
)

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/fabric/runtime/runtime.zn:22
type ProcessorDef struct {
	TypeName string
	Config map[string]string
	Requires []string
	Connections map[string][]string
}

func NewProcessorDef(typeName string, config map[string]string, requires []string, connections map[string][]string) ProcessorDef {
	return ProcessorDef{TypeName: typeName, Config: config, Requires: requires, Connections: connections}
}

func (s ProcessorDef) String() string {
	return fmt.Sprintf("ProcessorDef(typeName=%v, config=%v, requires=%v, connections=%v)", s.TypeName, s.Config, s.Requires, s.Connections)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/fabric/runtime/runtime.zn:30
type WorkItem struct {
	Ff core.FlowFile
	Processor string
	Hops int
}

func NewWorkItem(ff core.FlowFile, processor string, hops int) WorkItem {
	return WorkItem{Ff: ff, Processor: processor, Hops: hops}
}

func (s WorkItem) String() string {
	return fmt.Sprintf("WorkItem(ff=%v, processor=%v, hops=%v)", s.Ff, s.Processor, s.Hops)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/fabric/runtime/runtime.zn:36
type Fabric struct {
	reg *registry.Registry
	globalCtx *core.ProcessorContext
	graph *core.PipelineGraph
	mu sync.Mutex
	dlqStore *core.DLQ
	processorCounts map[string]int
	processorErrors map[string]int
	processorDefs map[string]ProcessorDef
	sources map[string]core.ConnectorSource
	maxHops int
	totalProcessed int
	activeExecutions int
	running bool
}

func NewFabric(reg *registry.Registry, globalCtx *core.ProcessorContext) *Fabric {
	return &Fabric{
		reg: reg,
		globalCtx: globalCtx,
		graph: core.NewPipelineGraph(),
		dlqStore: core.NewDLQ(),
		maxHops: 50,
		processorCounts: map[string]int{},
		processorErrors: map[string]int{},
		processorDefs: map[string]ProcessorDef{},
		sources: map[string]core.ConnectorSource{},
		running: false,
	}
}

func (s *Fabric) snapshotGraph() *core.PipelineGraph {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.graph
}

func (s *Fabric) swapGraph(newGraph *core.PipelineGraph) {
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.graph = newGraph
	}()
}

func (s *Fabric) LoadFlow(cfg *config.Config) {
	if cfg.Has("defaults.max_hops") {
		s.maxHops = cfg.GetInt("defaults.max_hops")
	}
	newGraph := core.NewPipelineGraph()
	newDefs := map[string]ProcessorDef{}
	procKeys := cfg.GetSubKeys("flow.processors")
	for _, name := range procKeys {
		prefix := fmt.Sprintf("flow.processors.%v", name)
		typeName := cfg.GetString(prefix + ".type")
		if !s.reg.Has(typeName) {
			logging.Error("unknown processor type", "type", typeName, "name", name)
			continue
		}
		procConfig := cfg.GetStringMap(prefix + ".config")
		requires := cfg.GetStringSlice(prefix + ".requires")
		for _, provName := range requires {
			s.globalCtx.RegisterDependent(provName, name)
		}
		ctx := s.buildScopedContextFor(requires)
		proc := s.reg.Create(typeName, ctx, procConfig)
		newGraph.AddProcessor(name, proc, requires)
		connKeys := cfg.GetSubKeys(prefix + ".connections")
		connMap := map[string][]string{}
		for _, rel := range connKeys {
			targets := cfg.GetStringSlice(prefix + ".connections." + rel)
			if len(targets) > 0 {
				newGraph.AddConnection(name, rel, targets)
				connMap[rel] = targets
			}
		}
		newDefs[name] = NewProcessorDef(typeName, procConfig, requires, connMap)
		logging.Info("processor created", "name", name, "type", typeName)
	}
	newGraph.ComputeEntryPoints()
	s.swapGraph(newGraph)
	s.processorDefs = newDefs
	for _, name := range newGraph.GetProcessorNames() {
		if !func() bool { _, _ok := s.processorCounts[name]; return _ok }() {
			s.processorCounts[name] = 0
		}
		if !func() bool { _, _ok := s.processorErrors[name]; return _ok }() {
			s.processorErrors[name] = 0
		}
	}
	logging.Info("flow loaded", "processors", len(newGraph.GetProcessorNames()), "entry_points", len(newGraph.GetEntryPoints()))
}

func (s *Fabric) Execute(ff core.FlowFile, entryPoint string) bool {
	g := s.snapshotGraph()
	s.activeExecutions = s.activeExecutions + 1
	s.executeGraph(g, ff, entryPoint)
	s.activeExecutions = s.activeExecutions - 1
	return true
}

func (s *Fabric) IngestAndExecute(ff core.FlowFile) bool {
	g := s.snapshotGraph()
	entries := g.GetEntryPoints()
	if len(entries) == 0 {
		logging.Warn("no entry points configured, dropping ff", "id", ff.Id)
		return false
	}
	if len(entries) == 1 {
		return s.Execute(ff, entries[0])
	}
	i := 1
	for i < len(entries) {
		clone := cloneFlowFile(ff)
		s.Execute(clone, entries[i])
		i = i + 1
	}
	return s.Execute(ff, entries[0])
}

func (s *Fabric) executeGraph(g *core.PipelineGraph, ff core.FlowFile, entryPoint string) {
	work := []WorkItem{}
	work = append(work, NewWorkItem(ff, entryPoint, 0))
	for len(work) > 0 {
		last := len(work) - 1
		item := work[last]
		work = work[0:last]
		if item.Hops >= s.maxHops {
			logging.Error("max hops exceeded", "processor", item.Processor, "max_hops", s.maxHops, "ff", item.Ff.Id)
			s.incProcError(item.Processor)
			continue
		}
		if !g.HasProcessor(item.Processor) {
			logging.Error("unknown processor in graph, dropping", "processor", item.Processor, "ff", item.Ff.Id)
			continue
		}
		state := g.GetState(item.Processor)
		runIt := false
		switch state {
		case core.ENABLED:
			runIt = true
		case core.DRAINING:
		case core.DISABLED:
		}
		if !runIt {
			continue
		}
		proc := g.GetProcessor(item.Processor)
		result := proc.Process(item.Ff)
		s.incProcCount(item.Processor)
		s.totalProcessed = s.totalProcessed + 1
		switch _v := result.(type) {
		case core.Single:
			out := _v.Ff
			work = s.pushDownstream(work, g, out, item.Processor, "success", item.Hops + 1)
		case core.Multiple:
			outs := _v.Ffs
			for _, out := range outs {
				work = s.pushDownstream(work, g, out, item.Processor, "success", item.Hops + 1)
			}
		case core.Routed:
			route := _v.Route
			out := _v.Ff
			work = s.pushDownstream(work, g, out, item.Processor, route, item.Hops + 1)
		case core.Dropped:
			_ = _v
		case core.Failure:
			reason := _v.Reason
			failFf := _v.Ff
			targets := g.GetTargets(item.Processor, "failure")
			if len(targets) > 0 {
				work = s.pushDownstream(work, g, failFf, item.Processor, "failure", item.Hops + 1)
			} else {
				logging.Error("unhandled failure, routing to DLQ", "processor", item.Processor, "reason", reason, "ff", failFf.Id)
				s.dlqStore.Add(failFf, item.Processor, "direct", item.Hops, reason)
				s.incProcError(item.Processor)
			}
		default:
			_ = _v
			panic("unreachable")
		}
	}
}

func (s *Fabric) pushDownstream(work []WorkItem, g *core.PipelineGraph, ff core.FlowFile, fromProcessor string, relationship string, hops int) []WorkItem {
	targets := g.GetTargets(fromProcessor, relationship)
	if len(targets) == 0 {
		return work
	}
	i := len(targets) - 1
	for i >= 1 {
		work = append(work, NewWorkItem(cloneFlowFile(ff), targets[i], hops))
		i = i - 1
	}
	work = append(work, NewWorkItem(ff, targets[0], hops))
	return work
}

func (s *Fabric) incProcCount(name string) {
	if func() bool { _, _ok := s.processorCounts[name]; return _ok }() {
		s.processorCounts[name] = s.processorCounts[name] + 1
	} else {
		s.processorCounts[name] = 1
	}
}

func (s *Fabric) incProcError(name string) {
	if func() bool { _, _ok := s.processorErrors[name]; return _ok }() {
		s.processorErrors[name] = s.processorErrors[name] + 1
	} else {
		s.processorErrors[name] = 1
	}
}

func (s *Fabric) AddSource(source core.ConnectorSource) {
	s.sources[source.GetName()] = source
	if s.running {
		source.Start(func(ff core.FlowFile) bool { return s.IngestAndExecute(ff) })
		logging.Info("source started", "name", source.GetName(), "type", source.GetSourceType())
	}
}

func (s *Fabric) StartSource(name string) bool {
	if !func() bool { _, _ok := s.sources[name]; return _ok }() {
		return false
	}
	src := s.sources[name]
	if !src.IsRunning() {
		src.Start(func(ff core.FlowFile) bool { return s.IngestAndExecute(ff) })
		logging.Info("source started", "name", name, "type", src.GetSourceType())
	}
	return true
}

func (s *Fabric) StopSource(name string) bool {
	if !func() bool { _, _ok := s.sources[name]; return _ok }() {
		return false
	}
	s.sources[name].Stop()
	logging.Info("source stopped", "name", name)
	return true
}

func (s *Fabric) GetSources() []map[string]string {
	result := []map[string]string{}
	for _, name := range func() []string { _keys := make([]string, 0, len(s.sources)); for _k := range s.sources { _keys = append(_keys, _k) }; return _keys }() {
		src := s.sources[name]
		entry := map[string]string{}
		entry["name"] = name
		entry["type"] = src.GetSourceType()
		if src.IsRunning() {
			entry["running"] = "true"
		} else {
			entry["running"] = "false"
		}
		result = append(result, entry)
	}
	return result
}

func (s *Fabric) StartAsync() {
	s.running = true
	for _, name := range func() []string { _keys := make([]string, 0, len(s.sources)); for _k := range s.sources { _keys = append(_keys, _k) }; return _keys }() {
		src := s.sources[name]
		if !src.IsRunning() {
			src.Start(func(ff core.FlowFile) bool { return s.IngestAndExecute(ff) })
		}
		logging.Info("source started", "name", name, "type", src.GetSourceType())
	}
	logging.Info("fabric started", "processors", len(s.graph.GetProcessorNames()), "entry_points", len(s.graph.GetEntryPoints()), "sources", len(s.sources))
}

func (s *Fabric) StopAsync() {
	s.running = false
	for _, name := range func() []string { _keys := make([]string, 0, len(s.sources)); for _k := range s.sources { _keys = append(_keys, _k) }; return _keys }() {
		s.sources[name].Stop()
	}
}

func (s *Fabric) Ingest(ff core.FlowFile) bool {
	return s.IngestAndExecute(ff)
}

func (s *Fabric) ReplayAt(processorName string, ff core.FlowFile) bool {
	g := s.snapshotGraph()
	if !g.HasProcessor(processorName) {
		return false
	}
	return s.Execute(ff, processorName)
}

func (s *Fabric) GetContext() *core.ProcessorContext {
	return s.globalCtx
}

func (s *Fabric) GetRegistry() *registry.Registry {
	return s.reg
}

func (s *Fabric) GetDLQ() *core.DLQ {
	return s.dlqStore
}

func (s *Fabric) GetDlqCount() int {
	return s.dlqStore.Count()
}

func (s *Fabric) GetProcessorNames() []string {
	g := s.snapshotGraph()
	return g.GetProcessorNames()
}

func (s *Fabric) GetEntryPoints() []string {
	g := s.snapshotGraph()
	return g.GetEntryPoints()
}

func (s *Fabric) GetConnections() map[string]map[string][]string {
	g := s.snapshotGraph()
	return g.GetConnections()
}

func (s *Fabric) GetProcessorType(name string) string {
	if func() bool { _, _ok := s.processorDefs[name]; return _ok }() {
		return s.processorDefs[name].TypeName
	}
	return "unknown"
}

func (s *Fabric) GetStats() map[string]int {
	stats := map[string]int{}
	stats["processed"] = s.totalProcessed
	stats["active"] = s.activeExecutions
	stats["processors"] = len(s.graph.GetProcessorNames())
	stats["dlq"] = s.dlqStore.Count()
	return stats
}

func (s *Fabric) GetProcessorStats() map[string]map[string]int {
	stats := map[string]map[string]int{}
	for _, name := range s.graph.GetProcessorNames() {
		ps := map[string]int{}
		if func() bool { _, _ok := s.processorCounts[name]; return _ok }() {
			ps["processed"] = s.processorCounts[name]
		} else {
			ps["processed"] = 0
		}
		if func() bool { _, _ok := s.processorErrors[name]; return _ok }() {
			ps["errors"] = s.processorErrors[name]
		} else {
			ps["errors"] = 0
		}
		stats[name] = ps
	}
	return stats
}

func (s *Fabric) GetProcessorState(name string) core.ComponentState {
	g := s.snapshotGraph()
	return g.GetState(name)
}

func (s *Fabric) GetProcessorRequires(name string) []string {
	g := s.snapshotGraph()
	return g.GetRequires(name)
}

func (s *Fabric) EnableProcessor(name string) bool {
	g := s.snapshotGraph()
	if !g.HasProcessor(name) {
		return false
	}
	requires := g.GetRequires(name)
	for _, provName := range requires {
		_tryerr_val, _tryerr_ret, _tryerr := func() (bool, bool, error) {
			prov, err1 := s.globalCtx.GetProvider(provName)
			if err1 != nil {
				return false, false, err1
			}
			if !prov.IsEnabled() {
				logging.Warn("cannot enable — provider disabled", "processor", name, "provider", provName)
				return false, true, nil
			}
			return false, false, nil
		}()
		if !_tryerr_ret && _tryerr != nil {
			e := _tryerr
			_ = e
			logging.Warn("cannot enable — provider missing", "processor", name, "provider", provName)
			return false
		}
		if _tryerr_ret {
			return _tryerr_val
		}
	}
	g.SetState(name, core.ENABLED)
	logging.Info("processor enabled", "name", name)
	return true
}

func (s *Fabric) DisableProcessor(name string, drainSecs int) bool {
	g := s.snapshotGraph()
	if !g.HasProcessor(name) {
		return false
	}
	g.SetState(name, core.DISABLED)
	logging.Info("processor disabled", "name", name, "drain_secs", drainSecs)
	return true
}

func (s *Fabric) EnableProvider(providerName string) bool {
	_tryerr2_val, _tryerr2_ret, _tryerr2 := func() (bool, bool, error) {
		prov, err3 := s.globalCtx.GetProvider(providerName)
		if err3 != nil {
			return false, false, err3
		}
		prov.Enable()
		logging.Info("provider enabled", "name", providerName)
		return true, true, nil
		return false, false, nil
	}()
	if !_tryerr2_ret && _tryerr2 != nil {
		e := _tryerr2
		_ = e
		return false
	}
	if _tryerr2_ret {
		return _tryerr2_val
	}
	return false
}

func (s *Fabric) DisableProvider(providerName string, drainSecs int) bool {
	_tryerr4_val, _tryerr4_ret, _tryerr4 := func() (bool, bool, error) {
		prov, err5 := s.globalCtx.GetProvider(providerName)
		if err5 != nil {
			return false, false, err5
		}
		dependents := s.globalCtx.GetDependents(providerName)
		for _, procName := range dependents {
			switch s.GetProcessorState(procName) {
			case core.ENABLED:
				s.DisableProcessor(procName, drainSecs)
			case core.DRAINING:
			case core.DISABLED:
			}
		}
		prov.Disable(drainSecs)
		logging.Info("provider disabled", "name", providerName)
		return true, true, nil
		return false, false, nil
	}()
	if !_tryerr4_ret && _tryerr4 != nil {
		e := _tryerr4
		_ = e
		return false
	}
	if _tryerr4_ret {
		return _tryerr4_val
	}
	return false
}

func (s *Fabric) SetMaxHops(n int) {
	s.maxHops = n
}

func (s *Fabric) Connect(src string, relationship string, targets []string) {
	s.graph.AddConnection(src, relationship, targets)
}

func (s *Fabric) ComputeEntryPoints() {
	s.graph.ComputeEntryPoints()
}

func (s *Fabric) AddProcessorInstance(name string, proc core.ProcessorFn, requires []string) bool {
	g := s.snapshotGraph()
	if g.HasProcessor(name) {
		logging.Warn("processor already exists", "name", name)
		return false
	}
	for _, provName := range requires {
		s.globalCtx.RegisterDependent(provName, name)
	}
	g.AddProcessor(name, proc, requires)
	g.ComputeEntryPoints()
	s.processorCounts[name] = 0
	s.processorErrors[name] = 0
	return true
}

func (s *Fabric) AddProcessor(name string, typeName string, pconfig map[string]string) bool {
	g := s.snapshotGraph()
	if g.HasProcessor(name) {
		logging.Warn("processor already exists", "name", name)
		return false
	}
	if !s.reg.Has(typeName) {
		logging.Warn("unknown processor type", "type", typeName)
		return false
	}
	requires := []string{}
	ctx := s.buildScopedContextFor(requires)
	proc := s.reg.Create(typeName, ctx, pconfig)
	g.AddProcessor(name, proc, requires)
	g.ComputeEntryPoints()
	s.processorDefs[name] = NewProcessorDef(typeName, pconfig, requires, map[string][]string{})
	s.processorCounts[name] = 0
	s.processorErrors[name] = 0
	logging.Info("processor added", "name", name, "type", typeName)
	return true
}

func (s *Fabric) RemoveProcessor(name string) bool {
	g := s.snapshotGraph()
	if !g.HasProcessor(name) {
		return false
	}
	newGraph := core.NewPipelineGraph()
	for _, n := range g.GetProcessorNames() {
		if n == name {
			continue
		}
		newGraph.AddProcessor(n, g.GetProcessor(n), g.GetRequires(n))
	}
	allConns := g.GetConnections()
	for _, srcName := range func() []string { _keys := make([]string, 0, len(allConns)); for _k := range allConns { _keys = append(_keys, _k) }; return _keys }() {
		if srcName == name {
			continue
		}
		rels := allConns[srcName]
		for _, rel := range func() []string { _keys := make([]string, 0, len(rels)); for _k := range rels { _keys = append(_keys, _k) }; return _keys }() {
			kept := []string{}
			for _, t := range rels[rel] {
				if t != name {
					kept = append(kept, t)
				}
			}
			if len(kept) > 0 {
				newGraph.AddConnection(srcName, rel, kept)
			}
		}
	}
	newGraph.ComputeEntryPoints()
	s.swapGraph(newGraph)
	delete(s.processorDefs, name)
	logging.Info("processor removed", "name", name)
	return true
}

func (s *Fabric) buildScopedContextFor(requires []string) *core.ScopedContext {
	scoped := map[string]core.Provider{}
	if len(requires) == 0 {
		providerNames := s.globalCtx.ListProviders()
		for _, pn := range providerNames {
			_tryerr6 := func() error {
				_tmp8, err7 := s.globalCtx.GetProvider(pn)
				if err7 != nil {
					return err7
				}
				scoped[pn] = _tmp8
				return nil
			}()
			if _tryerr6 != nil {
				e := _tryerr6
				_ = e
			}
		}
	} else {
		for _, pn := range requires {
			_tryerr9 := func() error {
				_tmp11, err10 := s.globalCtx.GetProvider(pn)
				if err10 != nil {
					return err10
				}
				scoped[pn] = _tmp11
				return nil
			}()
			if _tryerr9 != nil {
				e := _tryerr9
				_ = e
			}
		}
	}
	return core.NewScopedContext(scoped)
}

func (s *Fabric) Status() {
	logging.Info("fabric status", "processors", len(s.graph.GetProcessorNames()), "entry_points", len(s.graph.GetEntryPoints()), "processed", s.totalProcessed, "active", s.activeExecutions, "dlq", s.dlqStore.Count())
}


//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/fabric/runtime/runtime.zn:708
func cloneFlowFile(ff core.FlowFile) core.FlowFile {
	attrs := map[string]string{}
	for _, k := range func() []string { _keys := make([]string, 0, len(ff.Attributes)); for _k := range ff.Attributes { _keys = append(_keys, _k) }; return _keys }() {
		attrs[k] = ff.Attributes[k]
	}
	return core.NewFlowFile(ff.Id, attrs, ff.Content, ff.Timestamp)
}

