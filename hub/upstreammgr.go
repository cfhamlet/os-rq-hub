package hub

import (
	"encoding/json"
	"sync"

	"github.com/cfhamlet/os-rq-pod/pkg/log"
	"github.com/cfhamlet/os-rq-pod/pkg/slicemap"
	"github.com/cfhamlet/os-rq-pod/pod"
)

// UpstreamManager TODO
type UpstreamManager struct {
	hub             *Hub
	upstreams       map[UpstreamID]*Upstream
	statusUpstreams map[UpstreamStatus]*slicemap.Map
	queueBox        *QueueBox
	waitStop        *sync.WaitGroup
	*sync.RWMutex
}

func fromUpstreamStoreMeta(store *UpstreamStoreMeta) (meta *UpstreamMeta) {
	return &UpstreamMeta{
		ID:  store.ID,
		API: store.API,
	}
}

// NewUpstreamManager TODO
func NewUpstreamManager(hub *Hub) *UpstreamManager {
	statusUpstreams := map[UpstreamStatus]*slicemap.Map{}
	for _, status := range UpstreamStatusList {
		statusUpstreams[status] = slicemap.New()
	}
	return &UpstreamManager{
		hub,
		map[UpstreamID]*Upstream{},
		statusUpstreams,
		NewQueueBox(hub),
		&sync.WaitGroup{},
		&sync.RWMutex{},
	}
}

// Start TODO
func (mgr *UpstreamManager) Start() (err error) {
	mgr.Lock()
	defer mgr.Unlock()

	for id := range mgr.upstreams {
		_, err = mgr.startUpstream(id)
		if err != nil {
			break
		}
	}
	mgr.hub.waitStop.Add(1)
	return
}

// Stop TODO
func (mgr *UpstreamManager) Stop() {
	mgr.Lock()

	for id := range mgr.upstreams {
		_, err := mgr.stopUpstream(id)
		if err != nil {
			log.Logger.Warningf("stop upstream fail %s %s", id, err)
		}
	}
	mgr.Unlock()

	mgr.waitStop.Wait()
	mgr.hub.waitStop.Done()
}

// LoadUpstreams TODO
func (mgr *UpstreamManager) LoadUpstreams() (err error) {
	mgr.Lock()
	defer mgr.Unlock()

	log.Logger.Debug("load upstreams start")
	var cursor uint64
	for {
		err = mgr.hub.setStatus(Preparing)
		if err != nil {
			return
		}

		var keys []string
		keys, cursor, err = mgr.hub.Client.HScan(RedisUpstreamsKey, cursor, "*", 1000).Result()
		if err == nil {
			err = mgr.loadUpstreams(keys)
		}
		if err != nil {
			return
		}
		log.Logger.Debugf("loading upstreams %d", len(mgr.upstreams))
		if cursor == 0 {
			break
		}
	}
	log.Logger.Debugf("loading upstreams finish %d", len(mgr.upstreams))

	return
}

func (mgr *UpstreamManager) loadUpstreams(ukeys []string) (err error) {
	for _, u := range ukeys {
		err = mgr.hub.setStatus(Preparing)
		if err != nil {
			break
		}
		var s string
		s, err = mgr.hub.Client.HGet(RedisUpstreamsKey, u).Result()
		if err != nil {
			break
		}
		meta := &UpstreamStoreMeta{}
		err = json.Unmarshal([]byte(s), meta)
		if err != nil {
			log.Logger.Warning("invalid meta %s", s)
		} else {
			status := meta.Status
			mgr.addUpstream(fromUpstreamStoreMeta(meta), status)
		}
	}
	return
}

func (mgr *UpstreamManager) addUpstream(meta *UpstreamMeta, status UpstreamStatus) (err error) {
	_, ok := mgr.upstreams[meta.ID]
	if ok {
		return ExistError(meta.ID)
	}
	upstream := NewUpstream(mgr, meta)
	err = upstream.setStatus(status)
	return
}

// CallByUpstream TODO
type CallByUpstream func(*Upstream) (Result, error)

func (mgr *UpstreamManager) mustExist(id UpstreamID, f CallByUpstream) (result Result, err error) {
	upstream, ok := mgr.upstreams[id]
	if !ok {
		err = NotExistError(id)
		return
	}
	return f(upstream)
}

func (mgr *UpstreamManager) withLock(id UpstreamID, f CallByUpstream) (Result, error) {
	mgr.Lock()
	defer mgr.Unlock()
	return mgr.mustExist(id, f)
}
func (mgr *UpstreamManager) withRLock(id UpstreamID, f CallByUpstream) (Result, error) {
	mgr.RLock()
	defer mgr.RUnlock()
	return mgr.mustExist(id, f)
}

func (mgr *UpstreamManager) startUpstream(id UpstreamID) (Result, error) {
	return mgr.mustExist(id, func(upstream *Upstream) (result Result, err error) {
		err = upstream.Start()
		return
	},
	)
}
func (mgr *UpstreamManager) stopUpstream(id UpstreamID) (Result, error) {
	return mgr.mustExist(id, func(upstream *Upstream) (result Result, err error) {
		err = upstream.Stop()
		return
	},
	)
}

func (mgr *UpstreamManager) setStatus(id UpstreamID, status UpstreamStatus) (Result, error) {
	return mgr.withLock(id,
		func(upstream *Upstream) (result Result, err error) {
			err = upstream.setStatus(status)
			if err == nil {
				result = upstream.Info()
			}
			return
		},
	)
}

// ResumeUpstream TODO
func (mgr *UpstreamManager) ResumeUpstream(id UpstreamID) (result Result, err error) {
	return mgr.withLock(id,
		func(upstream *Upstream) (result Result, err error) {
			err = upstream.setStatus(UpstreamWorking)
			if err == nil {
				result = upstream.Info()
			}
			return
		},
	)
}

// PauseUpstream TODO
func (mgr *UpstreamManager) PauseUpstream(id UpstreamID) (result Result, err error) {
	return mgr.withLock(id,
		func(upstream *Upstream) (result Result, err error) {
			err = upstream.setStatus(UpstreamPaused)
			if err == nil {
				result = upstream.Info()
			}
			return
		},
	)
}

// DeleteUpstream TODO
func (mgr *UpstreamManager) DeleteUpstream(id UpstreamID) (result Result, err error) {
	return mgr.withLock(id,
		func(upstream *Upstream) (result Result, err error) {
			err = upstream.Destory()
			if err == nil {
				result = upstream.Info()
			}
			return
		},
	)
}

// UpstreamInfo TODO
func (mgr *UpstreamManager) UpstreamInfo(id UpstreamID) (result Result, err error) {
	return mgr.withRLock(id,
		func(upstream *Upstream) (result Result, err error) {
			result = upstream.Info()
			return
		},
	)
}

// AddUpstream TODO
func (mgr *UpstreamManager) AddUpstream(meta *UpstreamMeta) (result Result, err error) {
	mgr.Lock()
	defer mgr.Unlock()
	err = mgr.addUpstream(meta, UpstreamWorking)
	if err == nil {
		result, err = mgr.startUpstream(meta.ID)
	}
	return
}

// Info TODO
func (mgr *UpstreamManager) Info() (result Result) {
	mgr.RLock()
	defer mgr.RUnlock()

	st := Result{}
	for _, status := range UpstreamStatusList {
		st[string(status)] = mgr.statusUpstreams[status].Size()
	}
	result = Result{"status": st}
	result["queues"] = len(mgr.queueBox.queueUpstreams)
	return
}

// GetRequest TODO
func (mgr *UpstreamManager) GetRequest(qid pod.QueueID) (Result, error) {
	mgr.RLock()
	defer mgr.RUnlock()
	return mgr.queueBox.GetRequest(qid)
}
