package registry

import (
	"zinc-flow/core"
	"fmt"
)

type ProcessorFactory = func(*core.ScopedContext, map[string]string) core.ProcessorFn

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/fabric/registry/registry.zn:12
type ProcessorInfo struct {
	Name string
	Description string
	ConfigKeys []string
}

func NewProcessorInfo(name string, description string, configKeys []string) ProcessorInfo {
	return ProcessorInfo{Name: name, Description: description, ConfigKeys: configKeys}
}

func (s ProcessorInfo) String() string {
	return fmt.Sprintf("ProcessorInfo(name=%v, description=%v, configKeys=%v)", s.Name, s.Description, s.ConfigKeys)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/fabric/registry/registry.zn:19
type Registry struct {
	factories map[string]ProcessorFactory
	info map[string]ProcessorInfo
}

func NewRegistry() *Registry {
	return &Registry{factories: map[string]ProcessorFactory{}, info: map[string]ProcessorInfo{}}
}

func (s *Registry) Register(pinfo ProcessorInfo, factory ProcessorFactory) {
	s.factories[pinfo.Name] = factory
	s.info[pinfo.Name] = pinfo
}

func (s *Registry) Create(name string, ctx *core.ScopedContext, config map[string]string) core.ProcessorFn {
	factory := s.factories[name]
	return factory(ctx, config)
}

func (s *Registry) Has(name string) bool {
	return func() bool { _, _ok := s.factories[name]; return _ok }()
}

func (s *Registry) List() []ProcessorInfo {
	result := []ProcessorInfo{}
	names := func() []string { _keys := make([]string, 0, len(s.info)); for _k := range s.info { _keys = append(_keys, _k) }; return _keys }()
	for _, n := range names {
		result = append(result, s.info[n])
	}
	return result
}


