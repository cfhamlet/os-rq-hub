package hub

import (
	"fmt"
	"sync"

	"github.com/cfhamlet/os-rq-pod/pkg/slicemap"
	"github.com/cfhamlet/os-rq-pod/pod"
	"github.com/segmentio/fasthash/fnv1a"
)

// UpstreamID TODO
type UpstreamID string

// ItemID TODO
func (uid UpstreamID) ItemID() uint64 {
	return fnv1a.HashString64(string(uid))
}

// UpstreamStatus TODO
type UpstreamStatus string

// Status enum
const (
	UpstreamPreparing   UpstreamStatus = "preparing"
	UpstreamWorking     UpstreamStatus = "working"
	UpstreamStopping    UpstreamStatus = "stopping"
	UpstreamStopped     UpstreamStatus = "stopped"
	UpstreamRemoving    UpstreamStatus = "removing"
	UpstreamUnavailable UpstreamStatus = "unavailable"
)

// UpstreamStatusList TODO
var UpstreamStatusList = []UpstreamStatus{
	UpstreamPreparing,
	UpstreamWorking,
	UpstreamStopping,
	UpstreamStopped,
	UpstreamUnavailable,
}

// UpstreamMeta TODO
type UpstreamMeta struct {
	ID  UpstreamID `json:"id" binding:"required"`
	API string     `json:"api" binding:"required"`
}

// Upstream TODO
type Upstream struct {
	*UpstreamMeta
	hub      *Hub
	status   UpstreamStatus
	queueIDs *slicemap.Map
	locker   *sync.RWMutex
}

// NewUpstream TODO
func NewUpstream(hub *Hub, meta *UpstreamMeta) *Upstream {
	return &Upstream{
		meta,
		hub,
		UpstreamStopped,
		slicemap.New(),
		&sync.RWMutex{},
	}
}

// UpdateQueuesTask TODO
type UpdateQueuesTask struct {
	upstream *Upstream
}

// ItemID TODO
func (upstream *Upstream) ItemID() uint64 {
	return upstream.ID.ItemID()
}

// Start TODO
func (upstream *Upstream) Start() {
	upstream.locker.Lock()
	defer upstream.locker.Unlock()
	upstream.hub.upstreamMgr.closewait.Add(1)
}

// Stop TODO
func (upstream *Upstream) Stop() {
	upstream.locker.Lock()
	defer upstream.locker.Unlock()
	upstream.hub.upstreamMgr.closewait.Done()
}

// UpstreamManager TODO
type UpstreamManager struct {
	hub             *Hub
	upstreams       map[UpstreamID]*Upstream
	statusUpstreams map[UpstreamStatus]*slicemap.Map
	queueBox        *QueueBox
	closewait       *sync.WaitGroup
	locker          *sync.RWMutex
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

// AddUpstream TODO
func (mgr *UpstreamManager) AddUpstream(meta *UpstreamMeta) (result Result, err error) {
	mgr.locker.Lock()
	defer mgr.locker.Unlock()
	_, ok := mgr.upstreams[meta.ID]
	if ok {
		err = ExistError(meta.ID)
	} else {
		upstream := NewUpstream(mgr.hub, meta)
		mgr.upstreams[meta.ID] = upstream
		mgr.statusUpstreams[upstream.status].Add(upstream)
		return mgr.startUpstream(meta.ID)
	}
	return
}

// DeleteUpstream TODO
func (mgr *UpstreamManager) DeleteUpstream(id UpstreamID) (result Result, err error) {
	mgr.locker.Lock()
	defer mgr.locker.Unlock()
	upstream, ok := mgr.upstreams[id]
	if !ok {
		err = NotExistError(id)
	} else {
		if upstream.status == UpstreamRemoving {
			err = pod.UnavailableError(fmt.Sprintf("%s %s", id, upstream.status))
		} else {
			mgr.setStatus(id, UpstreamRemoving)
			upstream.Stop()
			result = Result{"id": id, "status": upstream.status}
		}
	}
	return
}

// Info TODO
func (mgr *UpstreamManager) Info() (result Result) {
	mgr.locker.RLock()
	defer mgr.locker.RUnlock()

	st := Result{}
	for _, status := range UpstreamStatusList {
		st[string(status)] = mgr.statusUpstreams[status].Size()
	}
	result = Result{"status": st}
	result["queues"] = len(mgr.queueBox.queueUpstreams)
	return
}

func (mgr *UpstreamManager) startUpstream(id UpstreamID) (result Result, err error) {
	upstream, ok := mgr.upstreams[id]
	if !ok {
		err = NotExistError(id)
	} else {
		if upstream.status != UpstreamStopped {
			err = pod.UnavailableError(fmt.Sprintf("%s %s", id, upstream.status))
		} else {
			mgr.setStatus(id, UpstreamPreparing)
			upstream.Start()
			result = Result{"id": id, "status": upstream.status}
		}
	}
	return
}

// StartUpstream TODO
func (mgr *UpstreamManager) StartUpstream(id UpstreamID) (result Result, err error) {
	mgr.locker.Lock()
	defer mgr.locker.Unlock()
	return mgr.startUpstream(id)
}

func (mgr *UpstreamManager) stopUpstream(id UpstreamID) (result Result, err error) {
	upstream, ok := mgr.upstreams[id]
	if !ok {
		err = NotExistError(id)
	} else {
		if upstream.status != UpstreamWorking &&
			upstream.status != UpstreamPreparing &&
			upstream.status != UpstreamUnavailable {
			err = pod.UnavailableError(fmt.Sprintf("%s %s", id, upstream.status))
		} else {
			mgr.setStatus(id, UpstreamStopping)
			upstream.Stop()
			result = Result{"id": id, "status": upstream.status}
		}
	}
	return
}

// StopUpstream TODO
func (mgr *UpstreamManager) StopUpstream(id UpstreamID) (result Result, err error) {
	mgr.locker.Lock()
	defer mgr.locker.Unlock()
	return mgr.stopUpstream(id)
}

func (mgr *UpstreamManager) setStatus(id UpstreamID, status UpstreamStatus) (err error) {
	upstream, ok := mgr.upstreams[id]
	if !ok {
		err = NotExistError(string(id))
	} else {
		oldStatus := upstream.status
		if oldStatus != status {
			mgr.statusUpstreams[oldStatus].Delete(upstream)
			mgr.statusUpstreams[status].Add(upstream)
			upstream.status = status
		}
	}
	return
}

// Queues TODO
func (mgr *UpstreamManager) Queues(k int) (result Result) {
	mgr.locker.RLock()
	defer mgr.locker.RUnlock()

	upstreams := mgr.statusUpstreams[UpstreamWorking]
	l := upstreams.Size()
	total := len(mgr.queueBox.queueUpstreams)
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
	}
}

// GetRequest TODO
func (mgr *UpstreamManager) GetRequest(qid pod.QueueID) (Result, error) {
	mgr.locker.RLock()
	defer mgr.locker.RUnlock()
	return mgr.queueBox.GetRequest(qid)
}

// Stop TODO
func (mgr *UpstreamManager) Stop() {
	mgr.hub.waitStop.Done()
}
