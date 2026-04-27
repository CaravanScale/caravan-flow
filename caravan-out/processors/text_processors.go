package processors

import (
	"zinc-flow/core"
	"regexp"
	"fmt"
	"strings"
)

//line /home/vrjoshi/proj/zinc-flow/src/processors/text_processors.zn:26
type ReplaceText struct {
	pattern string
	replacement string
	replaceAll bool
	store core.ContentStore
}

func NewReplaceText(pattern string, replacement string, mode string, store core.ContentStore) *ReplaceText {
	return &ReplaceText{
		pattern: pattern,
		replacement: replacement,
		replaceAll: mode != "first",
		store: store,
	}
}

func (s *ReplaceText) Process(ff core.FlowFile) core.ProcessorResult {
	resolved := core.Resolve(s.store, ff.Content)
	if resolved.ErrorMsg != "" {
		return core.NewFailure(resolved.ErrorMsg, ff)
	}
	_tryerr_val, _tryerr_ret, _tryerr := func() (core.ProcessorResult, bool, error) {
		re, err1 := regexp.Compile(s.pattern)
		if err1 != nil {
			return nil, false, err1
		}
		text := string(resolved.Data)
		out := text
		if s.replaceAll {
			out = re.ReplaceAllString(text, s.replacement)
		} else {
			loc := re.FindStringIndex(text)
			if loc != nil {
				out = text[0:loc[0]] + re.ReplaceAllString(text[loc[0]:loc[1]], s.replacement) + text[loc[1]:len(text)]
			}
		}
		updated := core.WithContent(ff, core.NewRaw([]byte(out)))
		return core.NewSingle(updated), true, nil
		return nil, false, nil
	}()
	if !_tryerr_ret && _tryerr != nil {
		e := _tryerr
		_ = e
		return core.NewFailure(fmt.Sprintf("invalid regex: %v", s.pattern), ff)
	}
	if _tryerr_ret {
		return _tryerr_val
	}
	return nil
}


//line /home/vrjoshi/proj/zinc-flow/src/processors/text_processors.zn:67
func ReplaceTextFactory(ctx *core.ScopedContext, config map[string]string) core.ProcessorFn {
	pattern := ""
	if func() bool { _, _ok := config["pattern"]; return _ok }() {
		pattern = config["pattern"]
	}
	replacement := ""
	if func() bool { _, _ok := config["replacement"]; return _ok }() {
		replacement = config["replacement"]
	}
	mode := "all"
	if func() bool { _, _ok := config["mode"]; return _ok }() && config["mode"] != "" {
		mode = config["mode"]
	}
	store := resolveStore(ctx)
	return NewReplaceText(pattern, replacement, mode, store)
}

//line /home/vrjoshi/proj/zinc-flow/src/processors/text_processors.zn:82
type ExtractText struct {
	pattern string
	groupNames []string
	store core.ContentStore
}

func NewExtractText(pattern string, groupNamesSpec string, store core.ContentStore) *ExtractText {
	s := &ExtractText{pattern: pattern, store: store, groupNames: []string{}}
	if groupNamesSpec != "" {
		parts := strings.Split(groupNamesSpec, ",")
		for _, p := range parts {
			trimmed := strings.TrimSpace(p)
			if trimmed != "" {
				s.groupNames = append(s.groupNames, trimmed)
			}
		}
	}
	return s
}

func (s *ExtractText) Process(ff core.FlowFile) core.ProcessorResult {
	resolved := core.Resolve(s.store, ff.Content)
	if resolved.ErrorMsg != "" {
		return core.NewFailure(resolved.ErrorMsg, ff)
	}
	_tryerr2_val, _tryerr2_ret, _tryerr2 := func() (core.ProcessorResult, bool, error) {
		re, err3 := regexp.Compile(s.pattern)
		if err3 != nil {
			return nil, false, err3
		}
		text := string(resolved.Data)
		matches := re.FindStringSubmatch(text)
		if matches == nil || len(matches) == 0 {
			return core.NewSingle(ff), true, nil
		}
		result := ff
		names := re.SubexpNames()
		i := 0
		for i < len(names) && i < len(matches) {
			name := names[i]
			if name != "" {
				result = core.WithAttribute(result, name, matches[i])
			}
			i = i + 1
		}
		j := 0
		for j < len(s.groupNames) && j + 1 < len(matches) {
			if s.groupNames[j] != "" {
				result = core.WithAttribute(result, s.groupNames[j], matches[j + 1])
			}
			j = j + 1
		}
		return core.NewSingle(result), true, nil
		return nil, false, nil
	}()
	if !_tryerr2_ret && _tryerr2 != nil {
		e := _tryerr2
		_ = e
		return core.NewFailure(fmt.Sprintf("invalid regex: %v", s.pattern), ff)
	}
	if _tryerr2_ret {
		return _tryerr2_val
	}
	return nil
}


//line /home/vrjoshi/proj/zinc-flow/src/processors/text_processors.zn:146
func ExtractTextFactory(ctx *core.ScopedContext, config map[string]string) core.ProcessorFn {
	pattern := ""
	if func() bool { _, _ok := config["pattern"]; return _ok }() {
		pattern = config["pattern"]
	}
	groupNames := ""
	if func() bool { _, _ok := config["group_names"]; return _ok }() {
		groupNames = config["group_names"]
	}
	store := resolveStore(ctx)
	return NewExtractText(pattern, groupNames, store)
}

//line /home/vrjoshi/proj/zinc-flow/src/processors/text_processors.zn:157
type SplitText struct {
	delimiter string
	headerLines int
	store core.ContentStore
}

func NewSplitText(delimiter string, headerLines int, store core.ContentStore) *SplitText {
	return &SplitText{delimiter: delimiter, headerLines: headerLines, store: store}
}

func (s *SplitText) Process(ff core.FlowFile) core.ProcessorResult {
	resolved := core.Resolve(s.store, ff.Content)
	if resolved.ErrorMsg != "" {
		return core.NewFailure(resolved.ErrorMsg, ff)
	}
	_tryerr4_val, _tryerr4_ret, _tryerr4 := func() (core.ProcessorResult, bool, error) {
		re, err5 := regexp.Compile(s.delimiter)
		if err5 != nil {
			return nil, false, err5
		}
		text := string(resolved.Data)
		header := ""
		body := text
		if s.headerLines > 0 {
			lines := strings.Split(text, `
`)
			if len(lines) > s.headerLines {
				headerLinesList := lines[0:s.headerLines]
				header = strings.Join(headerLinesList, `
`) + `
`
				remaining := lines[s.headerLines:len(lines)]
				body = strings.Join(remaining, `
`)
			}
		}
		parts := re.Split(body, -1)
		if len(parts) <= 1 {
			return core.NewSingle(ff), true, nil
		}
		outFiles := []core.FlowFile{}
		i := 0
		for i < len(parts) {
			part := parts[i]
			if strings.TrimSpace(part) == "" {
				i = i + 1
				continue
			}
			body := header + part
			attrs := map[string]string{}
			attrs["split.index"] = fmt.Sprint(i)
			attrs["split.count"] = fmt.Sprint(len(parts))
			outFiles = append(outFiles, core.CreateFlowFile([]byte(body), attrs))
			i = i + 1
		}
		if len(outFiles) == 0 {
			return core.NewSingle(ff), true, nil
		}
		return core.NewMultiple(outFiles), true, nil
		return nil, false, nil
	}()
	if !_tryerr4_ret && _tryerr4 != nil {
		e := _tryerr4
		_ = e
		return core.NewFailure(fmt.Sprintf("invalid delimiter regex: %v", s.delimiter), ff)
	}
	if _tryerr4_ret {
		return _tryerr4_val
	}
	return nil
}


//line /home/vrjoshi/proj/zinc-flow/src/processors/text_processors.zn:222
func SplitTextFactory(ctx *core.ScopedContext, config map[string]string) core.ProcessorFn {
	delimiter := `
`
	if func() bool { _, _ok := config["delimiter"]; return _ok }() && config["delimiter"] != "" {
		delimiter = config["delimiter"]
	}
	headerLines := 0
	if func() bool { _, _ok := config["header_lines"]; return _ok }() {
		headerLines = textParseInt(config["header_lines"], 0)
	}
	store := resolveStore(ctx)
	return NewSplitText(delimiter, headerLines, store)
}

//line /home/vrjoshi/proj/zinc-flow/src/processors/text_processors.zn:237
func resolveStore(ctx *core.ScopedContext) core.ContentStore {
	_tryerr_val, _tryerr_ret, _tryerr := func() (core.ContentStore, bool, error) {
		p, err1 := ctx.GetProvider("content")
		if err1 != nil {
			return nil, false, err1
		}
		cp := p.(*core.ContentProvider)
		return cp.GetStore(), true, nil
		return nil, false, nil
	}()
	if !_tryerr_ret && _tryerr != nil {
		e := _tryerr
		_ = e
		return core.NewMemoryContentStore()
	}
	if _tryerr_ret {
		return _tryerr_val
	}
	return nil
}

//line /home/vrjoshi/proj/zinc-flow/src/processors/text_processors.zn:247
func textParseInt(s string, fallback int) int {
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

