package processors

import (
	"zinc-flow/core"
	"sync"
	"os"
	"zinc-flow/fabric/model"
	"fmt"
)

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/put_file.zn:28
type PutFile struct {
	outputDir string
	namingAttr string
	prefix string
	suffix string
	format string
	store core.ContentStore
	mu sync.Mutex
	counter int64
}

func NewPutFile(outputDir string, namingAttr string, prefix string, suffix string, format string, store core.ContentStore) *PutFile {
	s := &PutFile{outputDir: outputDir, namingAttr: namingAttr, prefix: prefix, format: format, store: store, counter: int64(0)}
	if suffix == "" {
		s.suffix = ".dat"
	} else {
		s.suffix = suffix
	}
	err := os.MkdirAll(outputDir, 0o755)
	if err != nil {
		panic(err)
	}
	return s
}

func (s *PutFile) Process(ff core.FlowFile) core.ProcessorResult {
	resolved := core.Resolve(s.store, ff.Content)
	if resolved.ErrorMsg != "" {
		return core.NewFailure(resolved.ErrorMsg, ff)
	}
	data := resolved.Data
	if s.format == "v3" {
		data = model.PackFlowFile(ff, data)
	}
	fileName := ""
	if func() bool { _, _ok := ff.Attributes[s.namingAttr]; return _ok }() && ff.Attributes[s.namingAttr] != "" {
		fileName = s.prefix + ff.Attributes[s.namingAttr]
	} else {
		id := int64(0)
		func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.counter = s.counter + int64(1)
			id = s.counter
		}()
		fileName = s.prefix + fmt.Sprint(id) + s.suffix
	}
	fileName = basename(fileName)
	path := s.outputDir + "/" + fileName
	err1 := os.WriteFile(path, data, 0o644)
	if err1 != nil {
		panic(err1)
	}
	updated := core.WithAttribute(ff, "output.path", path)
	updated = core.WithAttribute(updated, "output.size", fmt.Sprint(len(data)))
	return core.NewSingle(updated)
}


//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/put_file.zn:90
func PutFileFactory(ctx *core.ScopedContext, config map[string]string) core.ProcessorFn {
	outputDir := ""
	if func() bool { _, _ok := config["output_dir"]; return _ok }() {
		outputDir = config["output_dir"]
	}
	namingAttr := "filename"
	if func() bool { _, _ok := config["naming_attribute"]; return _ok }() && config["naming_attribute"] != "" {
		namingAttr = config["naming_attribute"]
	}
	prefix := ""
	if func() bool { _, _ok := config["prefix"]; return _ok }() {
		prefix = config["prefix"]
	}
	suffix := ""
	if func() bool { _, _ok := config["suffix"]; return _ok }() {
		suffix = config["suffix"]
	}
	format := "raw"
	if func() bool { _, _ok := config["format"]; return _ok }() && config["format"] != "" {
		format = config["format"]
	}
	_tryerr_val, _tryerr_ret, _tryerr := func() (core.ProcessorFn, bool, error) {
		p, err1 := ctx.GetProvider("content")
		if err1 != nil {
			return nil, false, err1
		}
		cp := p.(*core.ContentProvider)
		return NewPutFile(outputDir, namingAttr, prefix, suffix, format, cp.GetStore()), true, nil
		return nil, false, nil
	}()
	if !_tryerr_ret && _tryerr != nil {
		e := _tryerr
		_ = e
		return NewPutFile(outputDir, namingAttr, prefix, suffix, format, core.NewMemoryContentStore())
	}
	if _tryerr_ret {
		return _tryerr_val
	}
	return nil
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/put_file.zn:117
func basename(s string) string {
	idx := -1
	i := 0
	for i < len(s) {
		if s[i:i + 1] == "/" {
			idx = i
		}
		i = i + 1
	}
	if idx < 0 {
		return s
	}
	return s[idx + 1:len(s)]
}

