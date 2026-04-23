package processors

import (
	"github.com/ZincScale/zinc-stdlib/logging"
	"zinc-flow/fabric/registry"
	"zinc-flow/core"
	"os"
)

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/builtin.zn:14
func RegisterBuiltins(reg *registry.Registry) {
	reg.Register(registry.NewProcessorInfo("UpdateAttribute", "Sets key=value attribute on FlowFiles", []string{"key", "value"}), UpdateAttributeFactory)
	reg.Register(registry.NewProcessorInfo("RouteOnAttribute", "Routes FlowFiles based on attribute predicates", []string{"routes"}), RouteOnAttributeFactory)
	reg.Register(registry.NewProcessorInfo("PutStdout", "Writes FlowFile to stdout", []string{"format"}), PutStdoutFactory)
	reg.Register(registry.NewProcessorInfo("ConvertJSONToRecord", "Parses JSON content into Avro records", []string{"schema_name"}), jsonToRecordsFactory)
	reg.Register(registry.NewProcessorInfo("ConvertRecordToJSON", "Serializes Avro records back to JSON", []string{}), recordsToJsonFactory)
	reg.Register(registry.NewProcessorInfo("ExtractRecordField", "Extracts record fields onto FlowFile attributes", []string{"fields", "record_index"}), ExtractRecordFieldFactory)
	reg.Register(registry.NewProcessorInfo("TransformRecord", "Rewrites records via rename/add/copy/remove/default/toUpper/toLower ops", []string{"operations"}), TransformRecordFactory)
	reg.Register(registry.NewProcessorInfo("ReplaceText", "Regex find/replace on content", []string{"pattern", "replacement", "mode"}), ReplaceTextFactory)
	reg.Register(registry.NewProcessorInfo("ExtractText", "Regex captures → FlowFile attributes", []string{"pattern", "group_names"}), ExtractTextFactory)
	reg.Register(registry.NewProcessorInfo("SplitText", "Regex delimiter → Multiple FlowFiles", []string{"delimiter", "header_lines"}), SplitTextFactory)
	reg.Register(registry.NewProcessorInfo("LogAttribute", "Logs FlowFile attributes and passes through", []string{"prefix"}), LogAttributeFactory)
	reg.Register(registry.NewProcessorInfo("FilterAttribute", "Keeps or removes named FlowFile attributes", []string{"mode", "attributes"}), FilterAttributeFactory)
	reg.Register(registry.NewProcessorInfo("PutFile", "Writes FlowFile content to a directory", []string{"output_dir", "naming_attribute", "prefix", "suffix", "format"}), PutFileFactory)
	reg.Register(registry.NewProcessorInfo("add-attribute", "Adds key=value attribute to FlowFiles", []string{"key", "value"}), addAttributeFactory)
	reg.Register(registry.NewProcessorInfo("file-sink", "Writes FlowFile content to disk", []string{"output_dir"}), fileSinkFactory)
	reg.Register(registry.NewProcessorInfo("log", "Logs FlowFile and passes through", []string{"prefix"}), logProcessorFactory)
	reg.Register(registry.NewProcessorInfo("json-to-records", "Parses JSON content into Avro records", []string{"schema_name"}), jsonToRecordsFactory)
	reg.Register(registry.NewProcessorInfo("records-to-json", "Serializes Avro records back to JSON", []string{}), recordsToJsonFactory)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/builtin.zn:99
func addAttributeFactory(ctx *core.ScopedContext, config map[string]string) core.ProcessorFn {
	return NewAddAttribute(config["key"], config["value"])
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/builtin.zn:103
func fileSinkFactory(ctx *core.ScopedContext, config map[string]string) core.ProcessorFn {
	_tryerr_val, _tryerr_ret, _tryerr := func() (core.ProcessorFn, bool, error) {
		p, err1 := ctx.GetProvider("content")
		if err1 != nil {
			return nil, false, err1
		}
		cp := p.(*core.ContentProvider)
		return NewFileSink(config["output_dir"], cp.GetStore()), true, nil
		return nil, false, nil
	}()
	if !_tryerr_ret && _tryerr != nil {
		e := _tryerr
		_ = e
		return NewFileSink(config["output_dir"], core.NewMemoryContentStore())
	}
	if _tryerr_ret {
		return _tryerr_val
	}
	return nil
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/builtin.zn:113
func logProcessorFactory(ctx *core.ScopedContext, config map[string]string) core.ProcessorFn {
	return NewLogProcessor(config["prefix"])
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/builtin.zn:117
func jsonToRecordsFactory(ctx *core.ScopedContext, config map[string]string) core.ProcessorFn {
	schemaName := config["schema_name"]
	if schemaName == "" {
		schemaName = "default"
	}
	_tryerr_val, _tryerr_ret, _tryerr := func() (core.ProcessorFn, bool, error) {
		p, err1 := ctx.GetProvider("content")
		if err1 != nil {
			return nil, false, err1
		}
		cp := p.(*core.ContentProvider)
		return NewJsonToRecords(schemaName, cp.GetStore()), true, nil
		return nil, false, nil
	}()
	if !_tryerr_ret && _tryerr != nil {
		e := _tryerr
		_ = e
		return NewJsonToRecords(schemaName, core.NewMemoryContentStore())
	}
	if _tryerr_ret {
		return _tryerr_val
	}
	return nil
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/builtin.zn:131
func recordsToJsonFactory(ctx *core.ScopedContext, config map[string]string) core.ProcessorFn {
	return NewRecordsToJson()
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/builtin.zn:136
type AddAttribute struct {
	key string
	value string
}

func NewAddAttribute(key string, value string) *AddAttribute {
	return &AddAttribute{key: key, value: value}
}

func (s *AddAttribute) Process(ff core.FlowFile) core.ProcessorResult {
	tagged := core.WithAttribute(ff, s.key, s.value)
	return core.NewSingle(tagged)
}


//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/builtin.zn:153
type FileSink struct {
	outputDir string
	store core.ContentStore
}

func NewFileSink(outputDir string, store core.ContentStore) *FileSink {
	return &FileSink{outputDir: outputDir, store: store}
}

func (s *FileSink) Process(ff core.FlowFile) core.ProcessorResult {
	path := s.outputDir + "/" + ff.Id + ".out"
	resolved := core.Resolve(s.store, ff.Content)
	if resolved.ErrorMsg != "" {
		return core.NewFailure(resolved.ErrorMsg, ff)
	}
	err := os.WriteFile(path, resolved.Data, 0o644)
	if err != nil {
		panic(err)
	}
	return core.NewDropped("written to " + path)
}


//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/builtin.zn:174
type LogProcessor struct {
	prefix string
}

func NewLogProcessor(prefix string) *LogProcessor {
	return &LogProcessor{prefix: prefix}
}

func (s *LogProcessor) Process(ff core.FlowFile) core.ProcessorResult {
	logging.Info("flowfile", "processor", s.prefix, "id", ff.Id, "attributes", ff.Attributes)
	return core.NewSingle(ff)
}


//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/builtin.zn:189
type JsonToRecords struct {
	schemaName string
	store core.ContentStore
}

func NewJsonToRecords(schemaName string, store core.ContentStore) *JsonToRecords {
	return &JsonToRecords{schemaName: schemaName, store: store}
}

func (s *JsonToRecords) Process(ff core.FlowFile) core.ProcessorResult {
	switch _v := ff.Content.(type) {
	case core.Raw:
		bytes := _v.Bytes
		return s.parseJson(ff, bytes)
	case core.Claim:
		_ = _v
		resolved := core.Resolve(s.store, ff.Content)
		if resolved.ErrorMsg != "" {
			return core.NewFailure(resolved.ErrorMsg, ff)
		}
		return s.parseJson(ff, resolved.Data)
	case core.Records:
		_ = _v
		return core.NewSingle(ff)
	default:
		_ = _v
		panic("unreachable")
	}
	return nil
}

func (s *JsonToRecords) parseJson(ff core.FlowFile, data []byte) core.ProcessorResult {
	schema := core.SchemaFromFields(s.schemaName, []core.Field{})
	reader := core.NewJsonRecordReader()
	records := reader.Read(data, schema)
	if len(records) == 0 {
		return core.NewFailure("no records parsed from JSON", ff)
	}
	updated := core.WithContent(ff, core.NewRecords(schema, records))
	return core.NewSingle(updated)
}


//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/processors/builtin.zn:232
type RecordsToJson struct {
}

func NewRecordsToJson() *RecordsToJson {
	return &RecordsToJson{}
}

func (s *RecordsToJson) Process(ff core.FlowFile) core.ProcessorResult {
	switch _v := ff.Content.(type) {
	case core.Records:
		schema := _v.Schema
		records := _v.Rows
		writer := core.NewJsonRecordWriter()
		bytes := writer.Write(records, schema)
		updated := core.WithContent(ff, core.NewRaw(bytes))
		return core.NewSingle(updated)
	case core.Raw:
		_ = _v
		return core.NewSingle(ff)
	case core.Claim:
		_ = _v
		return core.NewSingle(ff)
	default:
		_ = _v
		panic("unreachable")
	}
	return nil
}


