package hub

import (
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
func workUpstreamStatus(status UpstreamStatus) bool {
	return status == UpstreamWorking || status == UpstreamPaused
}

// UpstreamStatus TODO
type UpstreamStatus string

// Status enum
const (
	UpstreamInit        UpstreamStatus = "init"
	UpstreamWorking     UpstreamStatus = "working"
	UpstreamPaused      UpstreamStatus = "paused"
	UpstreamUnavailable UpstreamStatus = "unavailable"

	UpstreamStopping UpstreamStatus = "stopping"
	UpstreamStopped  UpstreamStatus = "stopped"
	UpstreamRemoving UpstreamStatus = "removing"
	UpstreamRemoved  UpstreamStatus = "removed"
)

// UpstreamStatusList TODO
var UpstreamStatusList = []UpstreamStatus{
	UpstreamInit,
	UpstreamWorking,
	UpstreamPaused,
	UpstreamUnavailable,
	UpstreamStopping,
	UpstreamStopped,
	UpstreamRemoving,
	UpstreamRemoved,
}

// UpstreamStoreMeta TODO
type UpstreamStoreMeta struct {
	*UpstreamMeta
	Status UpstreamStatus `json:"status" binding:"required"`
}

// UpstreamMeta TODO
type UpstreamMeta struct {
	ID  UpstreamID `json:"id" binding:"required"`
	API string     `json:"api" binding:"required"`
}

// Upstream TODO
type Upstream struct {
	*UpstreamMeta
	status   UpstreamStatus
	mgr      *UpstreamManager
	queueIDs *slicemap.Map
	qtask    *UpdateQueuesTask
	*sync.RWMutex
}

// NewUpstream TODO
func NewUpstream(mgr *UpstreamManager, meta *UpstreamMeta) *Upstream {
	upstream := &Upstream{
		meta,
		UpstreamInit,
		mgr,
		slicemap.New(),
		nil,
		&sync.RWMutex{},
	}

	return upstream
}

func (upstream *Upstream) setStatus(status UpstreamStatus) (err error) {

	if upstream.status == status {
		return
	}
	e := UnavailableError(upstream.status)
	switch upstream.status {
	case UpstreamInit:
	case UpstreamUnavailable:
		fallthrough
	case UpstreamWorking:
		fallthrough
	case UpstreamPaused:
		switch status {
		case UpstreamInit:
			fallthrough
		case UpstreamStopped:
			fallthrough
		case UpstreamRemoved:
			err = e
		}
	case UpstreamStopping:
		switch status {
		case UpstreamStopped:
		default:
			err = e
		}
	case UpstreamRemoving:
		switch status {
		case UpstreamRemoved:
		default:
			err = e
		}
	case UpstreamStopped:
		fallthrough
	case UpstreamRemoved:
		err = e
	}

	if err != nil {
		return
	}

	mgr := upstream.mgr

	if upstream.status == UpstreamInit && status != UpstreamRemoved {
		mgr.upstreams[upstream.ID] = upstream
	}
	mgr.statusUpstreams[upstream.status].Delete(upstream)
	if status != UpstreamRemoved {
		mgr.statusUpstreams[status].Add(upstream)
	} else {
		delete(mgr.upstreams, upstream.ID)
	}

	upstream.status = status

	return
}

// ItemID TODO
func (upstream *Upstream) ItemID() uint64 {
	return upstream.ID.ItemID()
}

// Start TODO
func (upstream *Upstream) Start() (err error) {
	upstream.Lock()
	defer upstream.Unlock()

	if upstream.qtask != nil {
		err = UnavailableError("already started")
	} else {
		if !workUpstreamStatus(upstream.status) {
			err = upstream.setStatus(UpstreamWorking)
		}
		if err == nil {
			upstream.qtask = NewUpdateQueuesTask(upstream)
			go upstream.qtask.Start()
		}
	}

	return
}

// Destory TODO
func (upstream *Upstream) Destory() (err error) {
	upstream.Lock()
	defer upstream.Unlock()

	if upstream.qtask == nil {
		return
	}
	err = upstream.setStatus(UpstreamRemoving)
	if err == nil {
		go upstream.qtask.Stop()
	}

	return
}

// Stop TODO
func (upstream *Upstream) Stop() (err error) {
	upstream.Lock()
	defer upstream.Unlock()

	if upstream.qtask == nil {
		return
	}
	err = upstream.setStatus(UpstreamStopping)
	if err == nil {
		go upstream.qtask.Stop()
	}

	return
}

// Info TODO
func (upstream *Upstream) Info() (result Result) {
	upstream.RLock()
	defer upstream.RUnlock()
	return Result{
		"id":     upstream.ID,
		"status": upstream.status,
		"queues": upstream.queueIDs.Size(),
	}
}

// Queues TODO
func (mgr *UpstreamManager) Queues(k int) (result Result) {
	mgr.RLock()
	defer mgr.RUnlock()

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
	mgr.RLock()
	defer mgr.RUnlock()
	return mgr.queueBox.GetRequest(qid)
}
