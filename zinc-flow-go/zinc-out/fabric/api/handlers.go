package api

import (
	"net/http"
	"encoding/json"
	"zinc-flow/core"
	"io"
	"fmt"
	"zinc-flow/fabric/runtime"
)

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/fabric/api/handlers.zn:12
type ApiHandler struct {
	fab *runtime.Fabric
}

func NewApiHandler(fab *runtime.Fabric) *ApiHandler {
	return &ApiHandler{fab: fab}
}

func (s *ApiHandler) StatsHandler(w http.ResponseWriter, r *http.Request) {
	stats := s.fab.GetStats()
	writeJson(w, stats)
}

func (s *ApiHandler) ProcessorsHandler(w http.ResponseWriter, r *http.Request) {
	names := s.fab.GetProcessorNames()
	writeJson(w, names)
}

func (s *ApiHandler) RegistryHandler(w http.ResponseWriter, r *http.Request) {
	reg := s.fab.GetRegistry()
	infos := reg.List()
	result := []map[string]interface{}{}
	for _, info := range infos {
		entry := map[string]interface{}{}
		entry["name"] = info.Name
		entry["description"] = info.Description
		entry["configKeys"] = info.ConfigKeys
		result = append(result, entry)
	}
	writeJson(w, result)
}

func (s *ApiHandler) FlowHandler(w http.ResponseWriter, r *http.Request) {
	flow := map[string]interface{}{}
	flow["processors"] = s.fab.GetProcessorNames()
	flow["entry_points"] = s.fab.GetEntryPoints()
	flow["connections"] = s.fab.GetConnections()
	flow["stats"] = s.fab.GetStats()
	writeJson(w, flow)
}

func (s *ApiHandler) DlqHandler(w http.ResponseWriter, r *http.Request) {
	dlq := s.fab.GetDLQ()
	entries := dlq.List()
	result := []map[string]interface{}{}
	for _, entry := range entries {
		e := map[string]interface{}{}
		e["id"] = entry.Id
		e["sourceProcessor"] = entry.SourceProcessor
		e["sourceQueue"] = entry.SourceQueue
		e["attemptCount"] = entry.AttemptCount
		e["lastError"] = entry.LastError
		e["arrivedAt"] = entry.ArrivedAt
		e["flowFileId"] = entry.FlowFile.Id
		result = append(result, e)
	}
	writeJson(w, map[string]interface{}{"count": dlq.Count(), "entries": result})
}

func (s *ApiHandler) DlqReplayHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeError(w, 405, "method not allowed")
		return
	}
	body := readBody(r)
	if body == nil {
		writeError(w, 400, "failed to read body")
		return
	}
	req := map[string]interface{}{}
	_tryerr := func() error {
		err1 := json.Unmarshal(body, &req)
		if err1 != nil {
			return err1
		}
		return nil
	}()
	if _tryerr != nil {
		e := _tryerr
		_ = e
		writeError(w, 400, "invalid json")
		return
	}
	id := strVal(req, "id")
	if id == "" {
		writeError(w, 400, "id is required")
		return
	}
	dlq := s.fab.GetDLQ()
	entry := dlq.Get(id)
	if entry.Id == "" {
		writeError(w, 404, "dlq entry not found")
		return
	}
	sourceProc := entry.SourceProcessor
	ff := dlq.Replay(id)
	s.fab.ReplayAt(sourceProc, ff)
	writeJson(w, map[string]interface{}{"status": "replayed", "id": id, "flowFileId": ff.Id, "processor": sourceProc})
}

func (s *ApiHandler) DlqReplayAllHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeError(w, 405, "method not allowed")
		return
	}
	dlq := s.fab.GetDLQ()
	entries := dlq.ReplayAll()
	count := 0
	for _, entry := range entries {
		s.fab.ReplayAt(entry.SourceProcessor, entry.FlowFile)
		count = count + 1
	}
	writeJson(w, map[string]interface{}{"status": "replayed", "count": count})
}

func (s *ApiHandler) DlqDeleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "DELETE" {
		writeError(w, 405, "method not allowed")
		return
	}
	body := readBody(r)
	if body == nil {
		writeError(w, 400, "failed to read body")
		return
	}
	req := map[string]interface{}{}
	_tryerr2 := func() error {
		err3 := json.Unmarshal(body, &req)
		if err3 != nil {
			return err3
		}
		return nil
	}()
	if _tryerr2 != nil {
		e := _tryerr2
		_ = e
		writeError(w, 400, "invalid json")
		return
	}
	id := strVal(req, "id")
	if id == "" {
		writeError(w, 400, "id is required")
		return
	}
	dlq := s.fab.GetDLQ()
	dlq.Remove(id)
	writeJson(w, map[string]interface{}{"status": "removed", "id": id})
}

func (s *ApiHandler) ProcessorStatsHandler(w http.ResponseWriter, r *http.Request) {
	writeJson(w, s.fab.GetProcessorStats())
}

func (s *ApiHandler) EnableProcessorHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeError(w, 405, "method not allowed")
		return
	}
	body := readBody(r)
	if body == nil {
		writeError(w, 400, "failed to read body")
		return
	}
	req := map[string]interface{}{}
	_tryerr4 := func() error {
		err5 := json.Unmarshal(body, &req)
		if err5 != nil {
			return err5
		}
		return nil
	}()
	if _tryerr4 != nil {
		e := _tryerr4
		_ = e
		writeError(w, 400, "invalid json")
		return
	}
	name := strVal(req, "name")
	if name == "" {
		writeError(w, 400, "name is required")
		return
	}
	ok := s.fab.EnableProcessor(name)
	if !ok {
		writeError(w, 404, "processor not found")
		return
	}
	writeJson(w, map[string]interface{}{"status": "enabled", "name": name})
}

func (s *ApiHandler) DisableProcessorHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeError(w, 405, "method not allowed")
		return
	}
	body := readBody(r)
	if body == nil {
		writeError(w, 400, "failed to read body")
		return
	}
	req := map[string]interface{}{}
	_tryerr6 := func() error {
		err7 := json.Unmarshal(body, &req)
		if err7 != nil {
			return err7
		}
		return nil
	}()
	if _tryerr6 != nil {
		e := _tryerr6
		_ = e
		writeError(w, 400, "invalid json")
		return
	}
	name := strVal(req, "name")
	if name == "" {
		writeError(w, 400, "name is required")
		return
	}
	drainSecs := 60
	ok := s.fab.DisableProcessor(name, drainSecs)
	if !ok {
		writeError(w, 404, "processor not found")
		return
	}
	writeJson(w, map[string]interface{}{"status": "draining", "name": name, "drainTimeout": drainSecs})
}

func (s *ApiHandler) ProcessorStateHandler(w http.ResponseWriter, r *http.Request) {
	body := readBody(r)
	if body == nil {
		writeError(w, 400, "failed to read body")
		return
	}
	req := map[string]interface{}{}
	_tryerr8 := func() error {
		err9 := json.Unmarshal(body, &req)
		if err9 != nil {
			return err9
		}
		return nil
	}()
	if _tryerr8 != nil {
		e := _tryerr8
		_ = e
		writeError(w, 400, "invalid json")
		return
	}
	name := strVal(req, "name")
	if name == "" {
		writeError(w, 400, "name is required")
		return
	}
	state := s.fab.GetProcessorState(name)
	stateStr := "DISABLED"
	switch state {
	case core.ENABLED:
		stateStr = "ENABLED"
	case core.DRAINING:
		stateStr = "DRAINING"
	case core.DISABLED:
		stateStr = "DISABLED"
	}
	writeJson(w, map[string]interface{}{"name": name, "state": stateStr})
}

func (s *ApiHandler) AddProcessorHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeError(w, 405, "method not allowed")
		return
	}
	body := readBody(r)
	if body == nil {
		writeError(w, 400, "failed to read body")
		return
	}
	req := map[string]interface{}{}
	_tryerr10 := func() error {
		err11 := json.Unmarshal(body, &req)
		if err11 != nil {
			return err11
		}
		return nil
	}()
	if _tryerr10 != nil {
		e := _tryerr10
		_ = e
		writeError(w, 400, "invalid json")
		return
	}
	name := strVal(req, "name")
	typeName := strVal(req, "type")
	if name == "" || typeName == "" {
		writeError(w, 400, "name and type are required")
		return
	}
	config := map[string]string{}
	if func() bool { _, _ok := req["config"]; return _ok }() {
		rawConfig := map[string]string{}
		configBytes := make([]byte, 0)
		_tryerr12 := func() error {
			_tmp14, err13 := json.Marshal(req["config"])
			if err13 != nil {
				return err13
			}
			configBytes = _tmp14
			return nil
		}()
		if _tryerr12 != nil {
			e := _tryerr12
			_ = e
		}
		_tryerr15 := func() error {
			err16 := json.Unmarshal(configBytes, &rawConfig)
			if err16 != nil {
				return err16
			}
			return nil
		}()
		if _tryerr15 != nil {
			e := _tryerr15
			_ = e
		}
		keys := func() []string { _keys := make([]string, 0, len(rawConfig)); for _k := range rawConfig { _keys = append(_keys, _k) }; return _keys }()
		for _, k := range keys {
			config[k] = rawConfig[k]
		}
	}
	ok := s.fab.AddProcessor(name, typeName, config)
	if !ok {
		writeError(w, 409, "processor already exists or unknown type")
		return
	}
	w.WriteHeader(201)
	writeJson(w, map[string]interface{}{"status": "created", "name": name})
}

func (s *ApiHandler) RemoveProcessorHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "DELETE" {
		writeError(w, 405, "method not allowed")
		return
	}
	body := readBody(r)
	if body == nil {
		writeError(w, 400, "failed to read body")
		return
	}
	req := map[string]interface{}{}
	_tryerr17 := func() error {
		err18 := json.Unmarshal(body, &req)
		if err18 != nil {
			return err18
		}
		return nil
	}()
	if _tryerr17 != nil {
		e := _tryerr17
		_ = e
		writeError(w, 400, "invalid json")
		return
	}
	name := strVal(req, "name")
	if name == "" {
		writeError(w, 400, "name is required")
		return
	}
	ok := s.fab.RemoveProcessor(name)
	if !ok {
		writeError(w, 404, "processor not found")
		return
	}
	writeJson(w, map[string]interface{}{"status": "removed", "name": name})
}

func (s *ApiHandler) ProvidersHandler(w http.ResponseWriter, r *http.Request) {
	ctx := s.fab.GetContext()
	names := ctx.ListProviders()
	result := []map[string]interface{}{}
	for _, name := range names {
		entry := map[string]interface{}{}
		entry["name"] = name
		_tryerr19 := func() error {
			prov, err20 := ctx.GetProvider(name)
			if err20 != nil {
				return err20
			}
			entry["type"] = prov.GetType()
			state := prov.GetState()
			switch state {
			case core.ENABLED:
				entry["state"] = "ENABLED"
			case core.DRAINING:
				entry["state"] = "DRAINING"
			case core.DISABLED:
				entry["state"] = "DISABLED"
			}
			return nil
		}()
		if _tryerr19 != nil {
			e := _tryerr19
			_ = e
			entry["state"] = "UNKNOWN"
		}
		result = append(result, entry)
	}
	writeJson(w, result)
}

func (s *ApiHandler) EnableProviderHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeError(w, 405, "method not allowed")
		return
	}
	body := readBody(r)
	if body == nil {
		writeError(w, 400, "failed to read body")
		return
	}
	req := map[string]interface{}{}
	_tryerr21 := func() error {
		err22 := json.Unmarshal(body, &req)
		if err22 != nil {
			return err22
		}
		return nil
	}()
	if _tryerr21 != nil {
		e := _tryerr21
		_ = e
		writeError(w, 400, "invalid json")
		return
	}
	name := strVal(req, "name")
	if name == "" {
		writeError(w, 400, "name is required")
		return
	}
	ok := s.fab.EnableProvider(name)
	if !ok {
		writeError(w, 404, "provider not found")
		return
	}
	writeJson(w, map[string]interface{}{"status": "enabled", "name": name})
}

func (s *ApiHandler) DisableProviderHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeError(w, 405, "method not allowed")
		return
	}
	body := readBody(r)
	if body == nil {
		writeError(w, 400, "failed to read body")
		return
	}
	req := map[string]interface{}{}
	_tryerr23 := func() error {
		err24 := json.Unmarshal(body, &req)
		if err24 != nil {
			return err24
		}
		return nil
	}()
	if _tryerr23 != nil {
		e := _tryerr23
		_ = e
		writeError(w, 400, "invalid json")
		return
	}
	name := strVal(req, "name")
	if name == "" {
		writeError(w, 400, "name is required")
		return
	}
	drainSecs := 60
	ok := s.fab.DisableProvider(name, drainSecs)
	if !ok {
		writeError(w, 404, "provider not found")
		return
	}
	writeJson(w, map[string]interface{}{"status": "disabled", "name": name})
}


//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/fabric/api/handlers.zn:436
func readBody(r *http.Request) []byte {
	body := make([]byte, 0)
	_tryerr := func() error {
		_tmp2, err1 := io.ReadAll(r.Body)
		if err1 != nil {
			return err1
		}
		body = _tmp2
		return nil
	}()
	if _tryerr != nil {
		e := _tryerr
		_ = e
		return nil
	}
	r.Body.Close()
	return body
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/fabric/api/handlers.zn:448
func strVal(m map[string]interface{}, key string) string {
	if func() bool { _, _ok := m[key]; return _ok }() {
		return fmt.Sprint(m[key])
	}
	return ""
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/fabric/api/handlers.zn:456
func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	result := map[string]interface{}{"error": msg}
	body := []byte("{\"error\":\"unknown\"}")
	_tryerr := func() error {
		_tmp2, err1 := json.Marshal(result)
		if err1 != nil {
			return err1
		}
		body = _tmp2
		return nil
	}()
	if _tryerr != nil {
		e := _tryerr
		_ = e
	}
	w.Write(body)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/fabric/api/handlers.zn:466
func writeJson(w http.ResponseWriter, data interface{}) {
	body := make([]byte, 0)
	_tryerr := func() error {
		_tmp2, err1 := json.Marshal(data)
		if err1 != nil {
			return err1
		}
		body = _tmp2
		return nil
	}()
	if _tryerr != nil {
		e := _tryerr
		_ = e
		w.WriteHeader(500)
		w.Write([]byte("{\"error\":\"json marshal failed\"}"))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(body)
}

