package delivery

import (
	"zinc-flow/fabric/model"
	"fmt"
	"zinc-flow/core"
	"net/http"
	"bytes"
	"io"
)

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/fabric/delivery/http.zn:11
type DeliveryResult struct {
	Success bool
	StatusCode int
	Body []byte
	ErrorMsg string
}

func NewDeliveryResult(success bool, statusCode int, body []byte, errorMsg string) DeliveryResult {
	return DeliveryResult{Success: success, StatusCode: statusCode, Body: body, ErrorMsg: errorMsg}
}

func (s DeliveryResult) String() string {
	return fmt.Sprintf("DeliveryResult(success=%v, statusCode=%v, body=%v, errorMsg=%v)", s.Success, s.StatusCode, s.Body, s.ErrorMsg)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/fabric/delivery/http.zn:19
func DeliverJson(endpoint string, ff core.FlowFile, store core.ContentStore) DeliveryResult {
	resolved := core.Resolve(store, ff.Content)
	if resolved.ErrorMsg != "" {
		return NewDeliveryResult(false, 0, make([]byte, 0), resolved.ErrorMsg)
	}
	jsonBody := "{\"id\":\"" + ff.Id + "\",\"content_size\":" + fmt.Sprint(len(resolved.Data)) + "}"
	payload := []byte(jsonBody)
	_tryerr_val, _tryerr_ret, _tryerr := func() (DeliveryResult, bool, error) {
		resp, err1 := http.Post(endpoint, "application/json", bytes.NewBuffer(payload))
		if err1 != nil {
			return DeliveryResult{}, false, err1
		}
		respBody := make([]byte, 0)
		_tryerr2 := func() error {
			_tmp4, err3 := io.ReadAll(resp.Body)
			if err3 != nil {
				return err3
			}
			respBody = _tmp4
			return nil
		}()
		if _tryerr2 != nil {
			e := _tryerr2
			_ = e
			return NewDeliveryResult(false, resp.StatusCode, make([]byte, 0), "read error"), true, nil
		}
		resp.Body.Close()
		if resp.StatusCode == 429 {
			return NewDeliveryResult(false, 429, make([]byte, 0), "backpressure"), true, nil
		}
		if resp.StatusCode >= 400 {
			return NewDeliveryResult(false, resp.StatusCode, respBody, "http " + fmt.Sprint(resp.StatusCode)), true, nil
		}
		return NewDeliveryResult(true, resp.StatusCode, respBody, ""), true, nil
		return DeliveryResult{}, false, nil
	}()
	if !_tryerr_ret && _tryerr != nil {
		e := _tryerr
		_ = e
		return NewDeliveryResult(false, 0, make([]byte, 0), "http error")
	}
	if _tryerr_ret {
		return _tryerr_val
	}
	return DeliveryResult{}
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/fabric/delivery/http.zn:52
func DeliverV3(endpoint string, ff core.FlowFile, store core.ContentStore) DeliveryResult {
	resolved := core.Resolve(store, ff.Content)
	if resolved.ErrorMsg != "" {
		return NewDeliveryResult(false, 0, make([]byte, 0), resolved.ErrorMsg)
	}
	packed := model.PackFlowFile(ff, resolved.Data)
	_tryerr_val, _tryerr_ret, _tryerr := func() (DeliveryResult, bool, error) {
		resp, err1 := http.Post(endpoint, "application/octet-stream", bytes.NewBuffer(packed))
		if err1 != nil {
			return DeliveryResult{}, false, err1
		}
		respBody := make([]byte, 0)
		_tryerr2 := func() error {
			_tmp4, err3 := io.ReadAll(resp.Body)
			if err3 != nil {
				return err3
			}
			respBody = _tmp4
			return nil
		}()
		if _tryerr2 != nil {
			e := _tryerr2
			_ = e
			return NewDeliveryResult(false, resp.StatusCode, make([]byte, 0), "read error"), true, nil
		}
		resp.Body.Close()
		if resp.StatusCode == 429 {
			return NewDeliveryResult(false, 429, make([]byte, 0), "backpressure"), true, nil
		}
		if resp.StatusCode >= 400 {
			return NewDeliveryResult(false, resp.StatusCode, respBody, "http " + fmt.Sprint(resp.StatusCode)), true, nil
		}
		return NewDeliveryResult(true, resp.StatusCode, respBody, ""), true, nil
		return DeliveryResult{}, false, nil
	}()
	if !_tryerr_ret && _tryerr != nil {
		e := _tryerr
		_ = e
		return NewDeliveryResult(false, 0, make([]byte, 0), "http error")
	}
	if _tryerr_ret {
		return _tryerr_val
	}
	return DeliveryResult{}
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/fabric/delivery/http.zn:84
func DeliverToAll(endpoints []string, ff core.FlowFile, store core.ContentStore) []DeliveryResult {
	results := []DeliveryResult{}
	for _, endpoint := range endpoints {
		result := DeliverV3(endpoint, ff, store)
		results = append(results, result)
	}
	return results
}

