package processors

import (
	"fmt"
	"encoding/hex"
	"zinc-flow/core"
	"zinc-flow/fabric/model"
)

//line /home/vrjoshi/proj/zinc-flow/src/processors/put_stdout.zn:22
type PutStdout struct {
	format string
	store core.ContentStore
}

func NewPutStdout(format string, store core.ContentStore) *PutStdout {
	return &PutStdout{format: format, store: store}
}

func (s *PutStdout) Process(ff core.FlowFile) core.ProcessorResult {
	if s.format == "attrs" {
		s.printAttrs(ff)
		return core.NewSingle(ff)
	}
	resolved := core.Resolve(s.store, ff.Content)
	if resolved.ErrorMsg != "" {
		return core.NewFailure(resolved.ErrorMsg, ff)
	}
	if s.format == "v3" {
		packed := model.PackFlowFile(ff, resolved.Data)
		prefix := 128
		if len(packed) < prefix {
			prefix = len(packed)
		}
		_, err := fmt.Printf(`[ff-%s] v3 (%d bytes) %s
`, ff.Id, len(packed), hex.EncodeToString(packed[0:prefix]))
		if err != nil {
			panic(err)
		}
	} else 	if s.format == "hex" {
		prefix := 128
		if len(resolved.Data) < prefix {
			prefix = len(resolved.Data)
		}
		_, err1 := fmt.Printf(`[ff-%s] (%d bytes) %s
`, ff.Id, len(resolved.Data), hex.EncodeToString(resolved.Data[0:prefix]))
		if err1 != nil {
			panic(err1)
		}
	} else {
		_, err2 := fmt.Printf(`[ff-%s] %s
`, ff.Id, string(resolved.Data))
		if err2 != nil {
			panic(err2)
		}
	}
	return core.NewSingle(ff)
}

func (s *PutStdout) printAttrs(ff core.FlowFile) {
	line := "[ff-" + ff.Id + "]"
	for _, k := range func() []string { _keys := make([]string, 0, len(ff.Attributes)); for _k := range ff.Attributes { _keys = append(_keys, _k) }; return _keys }() {
		line = line + " " + k + "=" + ff.Attributes[k]
	}
	_, err3 := fmt.Println(line)
	if err3 != nil {
		panic(err3)
	}
}


//line /home/vrjoshi/proj/zinc-flow/src/processors/put_stdout.zn:72
func PutStdoutFactory(ctx *core.ScopedContext, config map[string]string) core.ProcessorFn {
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
		return NewPutStdout(format, cp.GetStore()), true, nil
		return nil, false, nil
	}()
	if !_tryerr_ret && _tryerr != nil {
		e := _tryerr
		_ = e
		return NewPutStdout(format, core.NewMemoryContentStore())
	}
	if _tryerr_ret {
		return _tryerr_val
	}
	return nil
}

