package hub

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/cfhamlet/os-rq-pod/pkg/log"
	"github.com/cfhamlet/os-rq-pod/pkg/slicemap"
	"github.com/cfhamlet/os-rq-pod/pkg/utils"
	"github.com/cfhamlet/os-rq-pod/pod"
)

// UpstreamMap TODO
type UpstreamMap map[UpstreamID]*Upstream

// QueueUpstreamsMap TODO
type QueueUpstreamsMap map[pod.QueueID]UpstreamMap

// UpstreamManager TODO
type UpstreamManager struct {
	hub             *Hub
	upstreams       map[UpstreamID]*Upstream
	statusUpstreams map[UpstreamStatus]*slicemap.Map
	queueUpstreams  QueueUpstreamsMap
	waitStop        *sync.WaitGroup
	*sync.RWMutex
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
		QueueUpstreamsMap{},
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

	scanner := utils.NewScanner(mgr.hub.Client, "hscan", RedisUpstreamsKey, "*", 1000)
	err = scanner.Scan(
		func(keys []string) (err error) {
			isKey := true
			for _, key := range keys {
				if isKey {
					isKey = !isKey
					continue
				}
				isKey = !isKey

				metaStore := NewUpstreamStoreMeta(nil)
				err = json.Unmarshal([]byte(key), metaStore)
				if err != nil {
					log.Logger.Warning("invalid meta", key, err)
					continue
				}
				var upstream *Upstream
				upstream, err = mgr.addUpstream(metaStore)
				if err != nil {
					break
				}
				log.Logger.Infof("load upstream %s %s", upstream.ID, upstream.Status())
			}
			return
		},
	)

	if err == nil {
		log.Logger.Debugf("loading upstreams finish %d", len(mgr.upstreams))
	} else {
		log.Logger.Errorf("loading upstreams finish %d, %s", len(mgr.upstreams), err)
	}

	return
}

func (mgr *UpstreamManager) addUpstream(metaStore *UpstreamStoreMeta) (upstream *Upstream, err error) {
	_, ok := mgr.upstreams[metaStore.ID]
	if ok {
		err = ExistError(metaStore.ID)
		return
	}
	upstream = NewUpstream(mgr, metaStore.UpstreamMeta)
	err = upstream.setStatus(metaStore.Status)
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

func (mgr *UpstreamManager) withLockMustExist(id UpstreamID, f CallByUpstream) (Result, error) {
	mgr.Lock()
	defer mgr.Unlock()
	return mgr.mustExist(id, f)
}

func (mgr *UpstreamManager) withRLockMustExist(id UpstreamID, f CallByUpstream) (Result, error) {
	mgr.RLock()
	defer mgr.RUnlock()
	return mgr.mustExist(id, f)
}

func (mgr *UpstreamManager) startUpstream(id UpstreamID) (Result, error) {
	return mgr.mustExist(id, func(upstream *Upstream) (result Result, err error) {
		err = upstream.Start()
		if err == nil {
			result = upstream.Info()
		}
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

// SetStatus TODO
func (mgr *UpstreamManager) SetStatus(id UpstreamID, status UpstreamStatus) (Result, error) {
	return mgr.withLockMustExist(id,
		func(upstream *Upstream) (result Result, err error) {
			err = upstream.SetStatus(status)
			if err == nil {
				result = upstream.Info()
			}
			return
		},
	)
}

// ResumeUpstream TODO
func (mgr *UpstreamManager) ResumeUpstream(id UpstreamID) (result Result, err error) {
	return mgr.withLockMustExist(id,
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
	return mgr.withLockMustExist(id,
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
	return mgr.withLockMustExist(id,
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
	return mgr.withRLockMustExist(id,
		func(upstream *Upstream) (result Result, err error) {
			result = upstream.Info()
			return
		},
	)
}

// AddUpstream TODO
func (mgr *UpstreamManager) AddUpstream(metaStore *UpstreamStoreMeta) (result Result, err error) {
	mgr.Lock()
	defer mgr.Unlock()
	_, err = mgr.addUpstream(metaStore)
	if err == nil {
		result, err = mgr.startUpstream(metaStore.ID)
	}
	return
}

// Info TODO
func (mgr *UpstreamManager) Info() (result Result) {
	mgr.RLock()
	defer mgr.RUnlock()

	st := Result{}
	for _, status := range UpstreamStatusList {
		st[utils.Text(status)] = mgr.statusUpstreams[status].Size()
	}
	result = Result{"status": st}
	result["queues"] = len(mgr.queueUpstreams)
	return
}

// Queues TODO
func (mgr *UpstreamManager) Queues(k int) (result Result) {
	t := time.Now()
	mgr.RLock()
	defer mgr.RUnlock()

	upstreams := mgr.statusUpstreams[UpstreamWorking]
	l := upstreams.Size()
	total := len(mgr.queueUpstreams)
	var selector QueuesSelector
	if l <= 0 {
		selector = emptySelector
	} else if total <= k {
		selector = NewAllSelector(mgr)
	} else {
		selector = NewRandSelector(mgr, k)
	}
	out := selector.Select()
	return Result{
		"k":         k,
		"queues":    out,
		"total":     total,
		"upstreams": l,
		"_cost_ms_": utils.SinceMS(t),
	}
}

// Upstreams TODO
func (mgr *UpstreamManager) Upstreams(status UpstreamStatus) (result Result, err error) {
	mgr.RLock()
	defer mgr.RUnlock()
	upstreams := mgr.statusUpstreams[status]
	iter := slicemap.NewFastIter(upstreams)
	result = Result{
		"status": utils.Text(status),
		"total":  upstreams.Size(),
	}
	out := []*UpstreamStoreMeta{}
	iter.Iter(func(item slicemap.Item) {
		u := item.(*Upstream)
		m := NewUpstreamStoreMeta(u)
		out = append(out, m)
	},
	)
	result["upstreams"] = out
	return
}

// UpdateUpStreamQueueIDs TODO
func (mgr *UpstreamManager) UpdateUpStreamQueueIDs(id UpstreamID, queues []pod.QueueID) (Result, error) {
	return mgr.withLockMustExist(id,
		func(upstream *Upstream) (result Result, err error) {
			result = upstream.UpdateQueueIDs(queues)
			return
		},
	)
}

// DeleteUpstreamQueueIDs TODO
func (mgr *UpstreamManager) DeleteUpstreamQueueIDs(id UpstreamID, queues []pod.QueueID) (Result, error) {
	return mgr.withLockMustExist(id,
		func(upstream *Upstream) (result Result, err error) {
			result = upstream.DeleteQueueIDs(queues)
			return
		},
	)
}

// GetRequest TODO
func (mgr *UpstreamManager) GetRequest(qid pod.QueueID) (Result, error) {
	mgr.RLock()
	defer mgr.RUnlock()
	return nil, nil
}
