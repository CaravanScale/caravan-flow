package core

import (
	"os"
	"fmt"
	"time"
)

type ContentStore interface {
	Store(data []byte) string
	Retrieve(claimId string) []byte
	Delete(claimId string)
	Exists(claimId string) bool
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/contentstore.zn:17
type FileContentStore struct {
	baseDir string
	claimCounter int
}

func NewFileContentStore(baseDir string) *FileContentStore {
	s := &FileContentStore{baseDir: baseDir, claimCounter: 0}
	err := os.MkdirAll(baseDir, 0o755)
	if err != nil {
		panic(err)
	}
	return s
}

func (s *FileContentStore) Store(data []byte) string {
	claimId := s.generateClaimId()
	path := s.claimPath(claimId)
	err1 := os.WriteFile(path, data, 0o644)
	if err1 != nil {
		panic(err1)
	}
	return claimId
}

func (s *FileContentStore) Retrieve(claimId string) []byte {
	path := s.claimPath(claimId)
	_tryerr2_val, _tryerr2_ret, _tryerr2 := func() ([]byte, bool, error) {
		data, err3 := os.ReadFile(path)
		if err3 != nil {
			return nil, false, err3
		}
		return data, true, nil
		return nil, false, nil
	}()
	if !_tryerr2_ret && _tryerr2 != nil {
		e := _tryerr2
		_ = e
		return make([]byte, 0)
	}
	if _tryerr2_ret {
		return _tryerr2_val
	}
	return nil
}

func (s *FileContentStore) Delete(claimId string) {
	path := s.claimPath(claimId)
	err4 := os.Remove(path)
	if err4 != nil {
		panic(err4)
	}
}

func (s *FileContentStore) Exists(claimId string) bool {
	path := s.claimPath(claimId)
	_, err := os.Stat(path)
	return err == nil
}

func (s *FileContentStore) claimPath(claimId string) string {
	prefix := claimId[0:2]
	dir := s.baseDir + "/" + prefix
	err5 := os.MkdirAll(dir, 0755)
	if err5 != nil {
		panic(err5)
	}
	return dir + "/" + claimId
}

func (s *FileContentStore) generateClaimId() string {
	s.claimCounter = s.claimCounter + 1
	return fmt.Sprintf("claim-%v-%v", time.Now().UnixNano(), s.claimCounter)
}


//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/contentstore.zn:69
type MemoryContentStore struct {
	data map[string][]byte
	counter int
}

func NewMemoryContentStore() *MemoryContentStore {
	return &MemoryContentStore{data: map[string][]byte{}, counter: 0}
}

func (s *MemoryContentStore) Store(content []byte) string {
	s.counter = s.counter + 1
	id := "mem-claim-" + fmt.Sprint(s.counter)
	s.data[id] = content
	return id
}

func (s *MemoryContentStore) Retrieve(claimId string) []byte {
	if func() bool { _, _ok := s.data[claimId]; return _ok }() {
		return s.data[claimId]
	}
	return make([]byte, 0)
}

func (s *MemoryContentStore) Delete(claimId string) {
	delete(s.data, claimId)
}

func (s *MemoryContentStore) Exists(claimId string) bool {
	return func() bool { _, _ok := s.data[claimId]; return _ok }()
}


//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/contentstore.zn:99
func MaybeOffload(store ContentStore, data []byte) Content {
	if len(data) > CONTENT_CLAIM_THRESHOLD {
		claimId := store.Store(data)
		return NewClaim(claimId, int64(len(data)))
	}
	return NewRaw(data)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/contentstore.zn:108
type ResolveResult struct {
	Data []byte
	ErrorMsg string
}

func NewResolveResult(data []byte, errorMsg string) ResolveResult {
	return ResolveResult{Data: data, ErrorMsg: errorMsg}
}

func (s ResolveResult) String() string {
	return fmt.Sprintf("ResolveResult(data=%v, errorMsg=%v)", s.Data, s.ErrorMsg)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/contentstore.zn:114
func Resolve(store ContentStore, c Content) ResolveResult {
	switch _v := c.(type) {
	case Raw:
		bytes := _v.Bytes
		return NewResolveResult(bytes, "")
	case Claim:
		claimId := _v.ClaimId
		return NewResolveResult(store.Retrieve(claimId), "")
	case Records:
		_ = _v
		return NewResolveResult(make([]byte, 0), "cannot resolve Records to raw bytes — use a RecordWriter")
	default:
		_ = _v
		panic("unreachable")
	}
	return ResolveResult{}
}

