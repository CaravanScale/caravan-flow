package core

import (
	"github.com/ZincScale/zinc-stdlib/config"
	"fmt"
	"github.com/ZincScale/zinc-stdlib/logging"
)

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/providers.zn:8
type ConfigProvider struct {
	providerName string
	state ComponentState
	cfg *config.Config
}

func NewConfigProvider(cfg *config.Config) *ConfigProvider {
	return &ConfigProvider{providerName: "config", state: DISABLED, cfg: cfg}
}

func (s *ConfigProvider) GetName() string {
	return s.providerName
}

func (s *ConfigProvider) GetType() string {
	return "config"
}

func (s *ConfigProvider) GetState() ComponentState {
	return s.state
}

func (s *ConfigProvider) Enable() {
	s.state = ENABLED
}

func (s *ConfigProvider) Disable(drainTimeoutSeconds int) {
	s.state = DISABLED
}

func (s *ConfigProvider) Shutdown() {
	s.state = DISABLED
}

func (s *ConfigProvider) IsEnabled() bool {
	return s.state == ENABLED
}

func (s *ConfigProvider) GetString(key string) string {
	return s.cfg.GetString(key)
}

func (s *ConfigProvider) GetInt(key string) int {
	return s.cfg.GetInt(key)
}

func (s *ConfigProvider) GetBool(key string) bool {
	return s.cfg.GetBool(key)
}

func (s *ConfigProvider) Has(key string) bool {
	return s.cfg.Has(key)
}

func (s *ConfigProvider) GetStringMap(key string) map[string]string {
	return s.cfg.GetStringMap(key)
}

func (s *ConfigProvider) GetStringSlice(key string) []string {
	return s.cfg.GetStringSlice(key)
}

func (s *ConfigProvider) GetSubKeys(key string) []string {
	return s.cfg.GetSubKeys(key)
}

func (s *ConfigProvider) GetProcessorConfig(processorName string) map[string]string {
	return s.cfg.GetStringMap(fmt.Sprintf("flow.processors.%v.config", processorName))
}


//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/providers.zn:43
type LoggingProvider struct {
	providerName string
	state ComponentState
	lm *logging.LogManager
}

func NewLoggingProvider(lm *logging.LogManager) *LoggingProvider {
	return &LoggingProvider{providerName: "logging", state: DISABLED, lm: lm}
}

func (s *LoggingProvider) GetName() string {
	return s.providerName
}

func (s *LoggingProvider) GetType() string {
	return "logging"
}

func (s *LoggingProvider) GetState() ComponentState {
	return s.state
}

func (s *LoggingProvider) Enable() {
	s.state = ENABLED
}

func (s *LoggingProvider) Disable(drainTimeoutSeconds int) {
	s.state = DISABLED
}

func (s *LoggingProvider) Shutdown() {
	s.state = DISABLED
}

func (s *LoggingProvider) IsEnabled() bool {
	return s.state == ENABLED
}

func (s *LoggingProvider) GetLogManager() *logging.LogManager {
	return s.lm
}


//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/providers.zn:66
type ContentProvider struct {
	providerName string
	state ComponentState
	store ContentStore
}

func NewContentProvider(providerName string, store ContentStore) *ContentProvider {
	return &ContentProvider{providerName: providerName, state: DISABLED, store: store}
}

func (s *ContentProvider) GetName() string {
	return s.providerName
}

func (s *ContentProvider) GetType() string {
	return "content"
}

func (s *ContentProvider) GetState() ComponentState {
	return s.state
}

func (s *ContentProvider) Enable() {
	s.state = ENABLED
}

func (s *ContentProvider) Disable(drainTimeoutSeconds int) {
	s.state = DISABLED
}

func (s *ContentProvider) Shutdown() {
	s.state = DISABLED
}

func (s *ContentProvider) IsEnabled() bool {
	return s.state == ENABLED
}

func (s *ContentProvider) GetStore() ContentStore {
	return s.store
}


