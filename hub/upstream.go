package hub

import (
	"sync"
	"time"

	"github.com/cfhamlet/os-rq-pod/pkg/json"
	"github.com/cfhamlet/os-rq-pod/pkg/request"
	"github.com/cfhamlet/os-rq-pod/pkg/slicemap"
	"github.com/cfhamlet/os-rq-pod/pkg/utils"
	"github.com/cfhamlet/os-rq-pod/pod"
	"github.com/go-redis/redis/v7"
	"github.com/segmentio/fasthash/fnv1a"
)

// UpstreamID TODO
type UpstreamID string

// ItemID TODO
func (id UpstreamID) ItemID() uint64 {
	return fnv1a.HashString64(string(id))
}
func workUpstreamStatus(status UpstreamStatus) bool {
	return status == UpstreamWorking ||
		status == UpstreamPaused ||
		status == UpstreamUnavailable
}

func stopUpstreamStatus(status UpstreamStatus) bool {
	return status == UpstreamStopping ||
		status == UpstreamStopped ||
		status == UpstreamRemoving ||
		status == UpstreamRemoved
}

// UpstreamStoreMeta TODO
type UpstreamStoreMeta struct {
	*UpstreamMeta
	Status UpstreamStatus `json:"status"`
}

// UpstreamMeta TODO
type UpstreamMeta struct {
	ID  UpstreamID `json:"id" binding:"required"`
	API string     `json:"api" binding:"required"`
}

// Upstream TODO
type Upstream struct {
	*UpstreamMeta
	status UpstreamStatus
	mgr    *UpstreamManager
	queues *slicemap.Map
	qtask  *UpdateQueuesTask
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

func saveMeta(client *redis.Client, meta *UpstreamStoreMeta) (err error) {
	var metaJSON []byte
	metaJSON, err = json.Marshal(meta)
	if err == nil {
		_, err = client.HSet(RedisUpstreamsKey, string(meta.ID), string(metaJSON)).Result()
	}
	return
}

func (upstream *Upstream) setStatus(newStatus UpstreamStatus) (err error) {

	oldStatus := upstream.status
	if oldStatus == newStatus {
		return
	}
	e := UnavailableError(oldStatus)
	switch oldStatus {
	case UpstreamInit:
	case UpstreamUnavailable:
		fallthrough
	case UpstreamWorking:
		switch newStatus {
		case UpstreamInit:
			fallthrough
		case UpstreamStopped:
			fallthrough
		case UpstreamRemoved:
			err = e
		}
	case UpstreamPaused:
		switch newStatus {
		case UpstreamInit:
			fallthrough
		case UpstreamUnavailable:
			fallthrough
		case UpstreamStopped:
			fallthrough
		case UpstreamRemoved:
			err = e
		}
	case UpstreamStopping:
		switch newStatus {
		case UpstreamStopped:
		default:
			err = e
		}
	case UpstreamRemoving:
		switch newStatus {
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
	if workUpstreamStatus(newStatus) &&
		newStatus != UpstreamUnavailable &&
		oldStatus != UpstreamUnavailable {
		metaStore := NewUpstreamStoreMeta(upstream)
		metaStore.Status = newStatus
		err = saveMeta(mgr.hub.Client, metaStore)
		if err != nil {
			return
		}
	} else if newStatus == UpstreamRemoved {
		_, err = mgr.hub.Client.HDel(RedisUpstreamsKey, string(upstream.ID)).Result()
	}

	if err != nil {
		return
	}

	upstream.status = newStatus
	mgr.statusUpstreams[oldStatus].Delete(upstream.ItemID())
	if newStatus != UpstreamRemoved {
		mgr.upstreams[upstream.ID] = upstream
		mgr.statusUpstreams[newStatus].Add(upstream)
	} else {
		delete(mgr.upstreams, upstream.ID)
	}

	return
}

// NewUpstreamStoreMeta TODO
func NewUpstreamStoreMeta(upstream *Upstream) *UpstreamStoreMeta {
	if upstream == nil {
		return &UpstreamStoreMeta{}
	}
	return &UpstreamStoreMeta{
		&UpstreamMeta{upstream.ID, upstream.API},
		upstream.status,
	}
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
	return upstream.teardown(UpstreamRemoving)
}

func (upstream *Upstream) teardown(status UpstreamStatus) (err error) {
	upstream.Lock()
	defer upstream.Unlock()

	if upstream.qtask == nil ||
		stopUpstreamStatus(upstream.status) {
		return
	}

	err = upstream.setStatus(status)
	if err == nil {
		go upstream.qtask.Stop()
	}

	return
}

// Stop TODO
func (upstream *Upstream) Stop() (err error) {
	return upstream.teardown(UpstreamStopping)
}

func (upstream *Upstream) info() (result Result) {
	return Result{
		"id":     upstream.ID,
		"api":    upstream.API,
		"status": upstream.status,
		"queues_stats": Result{
			"total": upstream.queues.Size(),
		},
	}
}

// Info TODO
func (upstream *Upstream) Info() (result Result) {
	upstream.RLock()
	defer upstream.RUnlock()
	return upstream.info()
}

// Status TODO
func (upstream *Upstream) Status() UpstreamStatus {
	upstream.RLock()
	defer upstream.RUnlock()
	return upstream.status
}

// SetStatus TODO
func (upstream *Upstream) SetStatus(newStatus UpstreamStatus) error {
	upstream.Lock()
	defer upstream.Unlock()
	return upstream.setStatus(newStatus)
}

// UpdateQueues TODO
func (upstream *Upstream) UpdateQueues(queues []*Queue) (result Result) {
	t := time.Now()
	upstream.Lock()
	defer upstream.Unlock()
	new := 0
	newTotal := 0
	for _, queue := range queues {
		if upstream.queues.Add(queue) {
			new++
			upstreams, ok := upstream.mgr.queueUpstreams[queue.ID]
			if ok {
				upstreams[upstream.ID] = upstream
				continue
			}
			newTotal++
			upstreams = UpstreamMap{
				upstream.ID: upstream,
			}
			upstream.mgr.queueUpstreams[queue.ID] = upstreams
		}
	}
	result = upstream.info()
	result["num"] = len(queues)
	result["new"] = new
	result["new_total"] = newTotal
	result["_cost_ms_"] = utils.SinceMS(t)
	return
}

// IntersectQueueIDs TODO
func (upstream *Upstream) IntersectQueueIDs(queueIDs []pod.QueueID) []pod.QueueID {
	upstream.RLock()
	defer upstream.RUnlock()
	out := []pod.QueueID{}
	for _, qid := range queueIDs {
		exist := upstream.queues.Get(qid.ItemID())
		if exist != nil {
			out = append(out, qid)
		}
	}
	return out
}

// ExistQueueID TODO
func (upstream *Upstream) ExistQueueID(qid pod.QueueID) bool {
	upstream.RLock()
	defer upstream.RUnlock()
	q := upstream.queues.Get(qid.ItemID())
	if q == nil {
		return false
	}
	return true
}

// DeleteIdleQueue TODO
func (upstream *Upstream) DeleteIdleQueue(qid pod.QueueID) (result Result) {
	upstream.Lock()
	defer upstream.Unlock()
	return
}

// DeleteQueues TODO
func (upstream *Upstream) DeleteQueues(queueIDs []pod.QueueID) (result Result) {
	t := time.Now()
	upstream.Lock()
	defer upstream.Unlock()

	deleted := 0
	deletedTotal := 0

	for _, qid := range queueIDs {
		if upstream.queues.Delete(qid.ItemID()) {
			deleted++
			upstreams, ok := upstream.mgr.queueUpstreams[qid]
			if !ok {
				continue
			}
			delete(upstreams, upstream.ID)
			if len(upstreams) <= 0 {
				delete(upstream.mgr.queueUpstreams, qid)
				deletedTotal++
			}
		}
	}
	result = upstream.info()
	result["num"] = len(queueIDs)
	result["deleted"] = deleted
	result["deleted_total"] = deletedTotal
	result["_cost_ms_"] = utils.SinceMS(t)
	return
}

// GetRequest TODO
func (upstream *Upstream) GetRequest(qid pod.QueueID) (req *request.Request, err error) {
	upstream.RLock()
	defer upstream.RUnlock()
	return
}
