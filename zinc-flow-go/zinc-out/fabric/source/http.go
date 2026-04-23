package source

import (
	"strings"
	"zinc-flow/fabric/runtime"
	"zinc-flow/core"
	"net/http"
	"io"
	"zinc-flow/fabric/model"
	"fmt"
)

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/fabric/source/http.zn:14
type HttpSource struct {
	fab *runtime.Fabric
	store core.ContentStore
}

func NewHttpSource(fab *runtime.Fabric, store core.ContentStore) *HttpSource {
	return &HttpSource{fab: fab, store: store}
}

func (s *HttpSource) Handler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(405)
		w.Write([]byte("{\"error\":\"method not allowed\"}"))
		return
	}
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
		w.WriteHeader(400)
		w.Write([]byte("{\"error\":\"failed to read body\"}"))
		return
	}
	r.Body.Close()
	contentType := r.Header.Get("Content-Type")
	if contentType == "application/octet-stream" {
		s.handleV3(w, body)
		return
	}
	attrs := s.extractAttributes(r)
	content := core.MaybeOffload(s.store, body)
	ff := core.CreateFlowFileWithContent(content, attrs)
	accepted := s.fab.Ingest(ff)
	if !accepted {
		w.WriteHeader(503)
		w.Write([]byte("{\"error\":\"backpressure\",\"message\":\"ingest queue full\"}"))
		return
	}
	w.WriteHeader(200)
	w.Write([]byte("{\"status\":\"accepted\",\"id\":\"" + ff.Id + "\"}"))
}

func (s *HttpSource) handleV3(w http.ResponseWriter, body []byte) {
	flowfiles := model.UnpackAll(body)
	accepted := 0
	for _, ff := range flowfiles {
		if s.fab.Ingest(ff) {
			accepted = accepted + 1
		}
	}
	w.WriteHeader(200)
	w.Write([]byte("{\"status\":\"accepted\",\"count\":" + fmt.Sprint(accepted) + "}"))
}

func (s *HttpSource) extractAttributes(r *http.Request) map[string]string {
	attrs := map[string]string{}
	attrs["http.method"] = r.Method
	attrs["http.uri"] = r.URL.Path
	attrs["http.content.type"] = r.Header.Get("Content-Type")
	attrs["http.host"] = r.Host
	for key, values := range r.Header {
		if strings.HasPrefix(key, "X-Flow-") {
			attrKey := strings.ToLower(key[7:len(key)])
			attrs[attrKey] = values[0]
		}
	}
	return attrs
}

func (s *HttpSource) HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	w.Write([]byte("{\"status\":\"healthy\",\"dlq\":" + fmt.Sprint(s.fab.GetDlqCount()) + "}"))
}


