package source

import (
	"time"
	"github.com/ZincScale/zinc-stdlib/logging"
	"github.com/ZincScale/zinc-stdlib/config"
	"zinc-flow/fabric/runtime"
	"sync"
	"zinc-flow/core"
	"fmt"
)

//line /home/vrjoshi/proj/zinc-flow/src/fabric/source/generate.zn:27
type GenerateFlowFile struct {
	name string
	content []byte
	baseAttrs map[string]string
	batchSize int
	pollIntervalMs int
	mu sync.Mutex
	counter int64
	running bool
	ingestFn core.IngestFn
}

func NewGenerateFlowFile(name string, pollIntervalMs int, contentStr string, contentType string, attributesSpec string, batchSize int) *GenerateFlowFile {
	s := &GenerateFlowFile{name: name, pollIntervalMs: 1000, content: []byte(contentStr), batchSize: 1, counter: int64(0), baseAttrs: map[string]string{}, running: false}
	if pollIntervalMs > 0 {
		s.pollIntervalMs = pollIntervalMs
	}
	if batchSize > 0 {
		s.batchSize = batchSize
	}
	s.baseAttrs["source"] = name
	if contentType != "" {
		s.baseAttrs["http.content.type"] = contentType
	}
	if attributesSpec != "" {
		s.parseAttributes(attributesSpec)
	}
	return s
}

func (s *GenerateFlowFile) parseAttributes(spec string) {
	start := 0
	i := 0
	for i < len(spec) {
		if spec[i:i + 1] == ";" {
			s.addPair(spec[start:i])
			start = i + 1
		}
		i = i + 1
	}
	if start < len(spec) {
		s.addPair(spec[start:len(spec)])
	}
}

func (s *GenerateFlowFile) addPair(pair string) {
	colonIdx := -1
	j := 0
	for j < len(pair) {
		if pair[j:j + 1] == ":" {
			colonIdx = j
			break
		}
		j = j + 1
	}
	if colonIdx < 0 {
		return
	}
	k := pair[0:colonIdx]
	v := pair[colonIdx + 1:len(pair)]
	if k != "" {
		s.baseAttrs[k] = v
	}
}

func (s *GenerateFlowFile) GetName() string {
	return s.name
}

func (s *GenerateFlowFile) GetSourceType() string {
	return "GenerateFlowFile"
}

func (s *GenerateFlowFile) IsRunning() bool {
	return s.running
}

func (s *GenerateFlowFile) PollOnce() []core.FlowFile {
	batch := []core.FlowFile{}
	i := 0
	for i < s.batchSize {
		attrs := map[string]string{}
		for _, k := range func() []string { _keys := make([]string, 0, len(s.baseAttrs)); for _k := range s.baseAttrs { _keys = append(_keys, _k) }; return _keys }() {
			attrs[k] = s.baseAttrs[k]
		}
		func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.counter = s.counter + int64(1)
			attrs["generate.index"] = fmt.Sprint(s.counter)
		}()
		batch = append(batch, core.CreateFlowFile(s.content, attrs))
		i = i + 1
	}
	return batch
}

func (s *GenerateFlowFile) Start(ingest core.IngestFn) {
	s.ingestFn = ingest
	s.running = true
	go func() {
		for s.running {
			time.Sleep(time.Duration(s.pollIntervalMs) * time.Millisecond)
			if !s.running {
				break
			}
			batch := s.PollOnce()
			for _, ff := range batch {
				if !s.ingestFn(ff) {
					logging.Warn("ingest rejected (backpressure or no entry points)", "source", s.name, "ff", ff.Id)
				}
			}
		}
	}()
}

func (s *GenerateFlowFile) Stop() {
	s.running = false
}


//line /home/vrjoshi/proj/zinc-flow/src/fabric/source/generate.zn:153
func GenerateFlowFileFactory(name string, config map[string]string) core.ConnectorSource {
	contentStr := ""
	if func() bool { _, _ok := config["content"]; return _ok }() {
		contentStr = config["content"]
	}
	contentType := ""
	if func() bool { _, _ok := config["content_type"]; return _ok }() {
		contentType = config["content_type"]
	}
	attributesSpec := ""
	if func() bool { _, _ok := config["attributes"]; return _ok }() {
		attributesSpec = config["attributes"]
	}
	batchSize := 1
	if func() bool { _, _ok := config["batch_size"]; return _ok }() {
		batchSize = atoi(config["batch_size"], 1)
	}
	pollMs := 1000
	if func() bool { _, _ok := config["poll_interval_ms"]; return _ok }() {
		pollMs = atoi(config["poll_interval_ms"], 1000)
	}
	return NewGenerateFlowFile(name, pollMs, contentStr, contentType, attributesSpec, batchSize)
}

//line /home/vrjoshi/proj/zinc-flow/src/fabric/source/generate.zn:181
func LoadGenerateSource(cfg *config.Config, fab *runtime.Fabric) {
	if !cfg.Has("sources.generate.content") {
		return
	}
	flat := map[string]string{}
	flat["content"] = cfg.GetString("sources.generate.content")
	if cfg.Has("sources.generate.content_type") {
		flat["content_type"] = cfg.GetString("sources.generate.content_type")
	}
	if cfg.Has("sources.generate.attributes") {
		flat["attributes"] = cfg.GetString("sources.generate.attributes")
	}
	if cfg.Has("sources.generate.batch_size") {
		flat["batch_size"] = cfg.GetString("sources.generate.batch_size")
	}
	if cfg.Has("sources.generate.poll_interval_ms") {
		flat["poll_interval_ms"] = cfg.GetString("sources.generate.poll_interval_ms")
	}
	src := GenerateFlowFileFactory("generate", flat)
	fab.AddSource(src)
}

//line /home/vrjoshi/proj/zinc-flow/src/fabric/source/generate.zn:205
func atoi(s string, fallback int) int {
	if s == "" {
		return fallback
	}
	neg := false
	start := 0
	if s[0:1] == "-" {
		neg = true
		start = 1
	}
	n := 0
	i := start
	for i < len(s) {
		c := s[i:i + 1]
		if c < "0" || c > "9" {
			return fallback
		}
		n = n * 10 + int(byte(s[i])) - 48
		i = i + 1
	}
	if neg {
		return 0 - n
	}
	return n
}

