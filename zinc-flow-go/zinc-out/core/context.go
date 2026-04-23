package core

import (
	"github.com/ZincScale/zinc-stdlib/exceptions"
	"fmt"
)

type ComponentState int

const (
	DISABLED ComponentState = iota
	ENABLED
	DRAINING
)

type Provider interface {
	GetName() string
	GetType() string
	GetState() ComponentState
	Enable()
	Disable(drainTimeoutSeconds int)
	Shutdown()
	IsEnabled() bool
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/context.zn:26
type ProcessorContext struct {
	providers map[string]Provider
	dependents map[string][]string
}

func NewProcessorContext() *ProcessorContext {
	return &ProcessorContext{providers: map[string]Provider{}, dependents: map[string][]string{}}
}

func (s *ProcessorContext) AddProvider(provider Provider) {
	s.providers[provider.GetName()] = provider
}

func (s *ProcessorContext) RemoveProvider(name string) {
	if func() bool { _, _ok := s.providers[name]; return _ok }() {
		p := s.providers[name]
		if p.IsEnabled() {
			p.Disable(60)
		}
		p.Shutdown()
		delete(s.providers, name)
	}
}

func (s *ProcessorContext) GetProvider(name string) (Provider, error) {
	if !func() bool { _, _ok := s.providers[name]; return _ok }() {
		return nil, exceptions.NewConfigException(fmt.Sprintf("provider not found: %v", name))
	}
	return s.providers[name], nil
}

func (s *ProcessorContext) GetEnabledProvider(name string) (Provider, error) {
	if !func() bool { _, _ok := s.providers[name]; return _ok }() {
		return nil, exceptions.NewConfigException(fmt.Sprintf("provider not found: %v", name))
	}
	p := s.providers[name]
	if !p.IsEnabled() {
		return nil, exceptions.NewIllegalStateException(fmt.Sprintf("provider not enabled: %v", name))
	}
	return p, nil
}

func (s *ProcessorContext) ListProviders() []string {
	return func() []string { _keys := make([]string, 0, len(s.providers)); for _k := range s.providers { _keys = append(_keys, _k) }; return _keys }()
}

func (s *ProcessorContext) RegisterDependent(providerName string, processorName string) {
	if !func() bool { _, _ok := s.dependents[providerName]; return _ok }() {
		s.dependents[providerName] = []string{}
	}
	s.dependents[providerName] = append(s.dependents[providerName], processorName)
}

func (s *ProcessorContext) GetDependents(providerName string) []string {
	if func() bool { _, _ok := s.dependents[providerName]; return _ok }() {
		return s.dependents[providerName]
	}
	return []string{}
}

func (s *ProcessorContext) ShutdownAll() {
	names := func() []string { _keys := make([]string, 0, len(s.providers)); for _k := range s.providers { _keys = append(_keys, _k) }; return _keys }()
	for _, name := range names {
		p := s.providers[name]
		if p.IsEnabled() {
			p.Disable(60)
		}
		p.Shutdown()
	}
}


