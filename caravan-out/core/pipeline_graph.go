package core

import (
	"fmt"
)

//line /home/vrjoshi/proj/zinc-flow/src/core/pipeline_graph.zn:25
type Connection struct {
	Src string
	Rel string
	Targets []string
}

func NewConnection(src string, rel string, targets []string) Connection {
	return Connection{Src: src, Rel: rel, Targets: targets}
}

func (s Connection) String() string {
	return fmt.Sprintf("Connection(src=%v, rel=%v, targets=%v)", s.Src, s.Rel, s.Targets)
}

//line /home/vrjoshi/proj/zinc-flow/src/core/pipeline_graph.zn:33
type PipelineGraph struct {
	processors map[string]ProcessorFn
	connections map[string]map[string][]string
	entryPoints []string
	processorNames []string
	processorStates map[string]ComponentState
	processorRequires map[string][]string
}

func NewPipelineGraph() *PipelineGraph {
	return &PipelineGraph{
		processors: map[string]ProcessorFn{},
		connections: map[string]map[string][]string{},
		entryPoints: []string{},
		processorNames: []string{},
		processorStates: map[string]ComponentState{},
		processorRequires: map[string][]string{},
	}
}

func (s *PipelineGraph) AddProcessor(name string, proc ProcessorFn, requires []string) {
	if !func() bool { _, _ok := s.processors[name]; return _ok }() {
		s.processorNames = append(s.processorNames, name)
	}
	s.processors[name] = proc
	s.processorStates[name] = ENABLED
	s.processorRequires[name] = requires
	if !func() bool { _, _ok := s.connections[name]; return _ok }() {
		s.connections[name] = map[string][]string{}
	}
}

func (s *PipelineGraph) AddConnection(src string, relationship string, targets []string) {
	if !func() bool { _, _ok := s.connections[src]; return _ok }() {
		s.connections[src] = map[string][]string{}
	}
	s.connections[src][relationship] = targets
}

func (s *PipelineGraph) GetTargets(procName string, relationship string) []string {
	if !func() bool { _, _ok := s.connections[procName]; return _ok }() {
		return []string{}
	}
	rels := s.connections[procName]
	if !func() bool { _, _ok := rels[relationship]; return _ok }() {
		return []string{}
	}
	return rels[relationship]
}

func (s *PipelineGraph) HasProcessor(name string) bool {
	return func() bool { _, _ok := s.processors[name]; return _ok }()
}

func (s *PipelineGraph) GetProcessor(name string) ProcessorFn {
	return s.processors[name]
}

func (s *PipelineGraph) GetProcessorNames() []string {
	return s.processorNames
}

func (s *PipelineGraph) GetConnections() map[string]map[string][]string {
	return s.connections
}

func (s *PipelineGraph) GetState(name string) ComponentState {
	if !func() bool { _, _ok := s.processorStates[name]; return _ok }() {
		return DISABLED
	}
	return s.processorStates[name]
}

func (s *PipelineGraph) SetState(name string, newState ComponentState) {
	if func() bool { _, _ok := s.processorStates[name]; return _ok }() {
		s.processorStates[name] = newState
	}
}

func (s *PipelineGraph) ComputeEntryPoints() {
	targetSet := map[string]bool{}
	for _, name := range s.processorNames {
		if func() bool { _, _ok := s.connections[name]; return _ok }() {
			rels := s.connections[name]
			for _, rel := range func() []string { _keys := make([]string, 0, len(rels)); for _k := range rels { _keys = append(_keys, _k) }; return _keys }() {
				for _, target := range rels[rel] {
					targetSet[target] = true
				}
			}
		}
	}
	entries := []string{}
	for _, name := range s.processorNames {
		if !func() bool { _, _ok := targetSet[name]; return _ok }() {
			entries = append(entries, name)
		}
	}
	s.entryPoints = entries
}

func (s *PipelineGraph) GetEntryPoints() []string {
	return s.entryPoints
}

func (s *PipelineGraph) GetRequires(name string) []string {
	if !func() bool { _, _ok := s.processorRequires[name]; return _ok }() {
		return []string{}
	}
	return s.processorRequires[name]
}


