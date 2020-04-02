package hub

import (
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/cfhamlet/os-rq-pod/pkg/json"
	"github.com/cfhamlet/os-rq-pod/pkg/log"
	"github.com/cfhamlet/os-rq-pod/pkg/request"
	"github.com/cfhamlet/os-rq-pod/pkg/slicemap"
	"github.com/cfhamlet/os-rq-pod/pkg/sth"
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

// UpstreamStoreMeta TODO
type UpstreamStoreMeta struct {
	*UpstreamMeta
	Status UpstreamStatus `json:"status"`
}

// UnmarshalUpstreamStoreMetaJSON TODO
func UnmarshalUpstreamStoreMetaJSON(b []byte) (storeMeta *UpstreamStoreMeta, err error) {
	storeMeta = NewUpstreamStoreMeta(nil)
	err = json.Unmarshal(b, storeMeta)
	if err == nil {
		var parsedURL *url.URL
		parsedURL, err = url.Parse(storeMeta.API)
		storeMeta.ParsedAPI = parsedURL
	}
	return
}

// UpstreamMeta TODO
type UpstreamMeta struct {
	ID        UpstreamID `json:"id" binding:"required"`
	API       string     `json:"api" binding:"required"`
	ParsedAPI *url.URL
}

// NewUpstreamStoreMeta TODO
func NewUpstreamStoreMeta(upstream *Upstream) *UpstreamStoreMeta {
	if upstream == nil {
		return &UpstreamStoreMeta{}
	}
	return &UpstreamStoreMeta{
		upstream.UpstreamMeta,
		upstream.status,
	}
}

// Upstream TODO
type Upstream struct {
	mgr *UpstreamManager
	*UpstreamMeta
	status UpstreamStatus
	queues *slicemap.Viewer
	client *http.Client
	qtask  *UpdateQueuesTask
}

// NewUpstream TODO
func NewUpstream(mgr *UpstreamManager, meta *UpstreamMeta) *Upstream {
	upstream := &Upstream{
		mgr,
		meta,
		UpstreamInit,
		slicemap.NewViewer(nil),
		&http.Client{},
		nil,
	}

	return upstream
}

func (upstream *Upstream) logFormat(format string, args ...interface{}) string {
	msg := fmt.Sprintf(format, args...)
	return fmt.Sprintf("<upstream %s %s> %s", upstream.ID, upstream.status, msg)
}

func saveMeta(client *redis.Client, meta *UpstreamStoreMeta) (err error) {
	var metaJSON []byte
	metaJSON, err = json.Marshal(meta)
	if err == nil {
		_, err = client.HSet(RedisUpstreamsKey, string(meta.ID), string(metaJSON)).Result()
	}
	return
}

// SetStatus TODO
func (upstream *Upstream) SetStatus(newStatus UpstreamStatus) (err error) {

	oldStatus := upstream.status
	if oldStatus == newStatus {
		return
	}
	e := pod.UnavailableError(oldStatus)
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
	if WorkUpstreamStatus(newStatus) &&
		newStatus != UpstreamUnavailable &&
		oldStatus != UpstreamUnavailable {
		storeMeta := NewUpstreamStoreMeta(upstream)
		storeMeta.Status = newStatus
		err = saveMeta(mgr.core.Client(), storeMeta)
	} else if newStatus == UpstreamRemoved {
		_, err = mgr.core.Client().HDel(RedisUpstreamsKey, string(upstream.ID)).Result()
	}

	if err == nil {
		upstream.status = newStatus
	}

	return
}

// ItemID TODO
func (upstream *Upstream) ItemID() uint64 {
	return upstream.ID.ItemID()
}

// Start TODO
func (upstream *Upstream) Start() (err error) {
	if upstream.qtask != nil {
		err = pod.UnavailableError("already started")
	} else {
		if !WorkUpstreamStatus(upstream.status) {
			err = upstream.mgr.setStatus(upstream, UpstreamWorking)
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
	if upstream.qtask == nil ||
		StopUpstreamStatus(upstream.status) {
		log.Logger.Warningf(upstream.logFormat("can not teardown twice"))
		return
	}

	err = upstream.mgr.setStatus(upstream, status)
	if err == nil {
		go upstream.qtask.Stop()
	} else {
		log.Logger.Errorf(upstream.logFormat("teardown %s", err))
	}

	return
}

// Stop TODO
func (upstream *Upstream) Stop() (err error) {
	return upstream.teardown(UpstreamStopping)
}

// Info TODO
func (upstream *Upstream) Info() (result sth.Result) {
	return sth.Result{
		"id":     upstream.ID,
		"api":    upstream.API,
		"status": upstream.status,
		"queues": sth.Result{
			"total": upstream.queues.Size(),
		},
	}
}

// Status TODO
func (upstream *Upstream) Status() UpstreamStatus {
	return upstream.status
}

// UpdateQueues TODO
func (upstream *Upstream) UpdateQueues(qMetas []*QueueMeta) (result sth.Result) {
	t := time.Now()
	new := 0
	newTotal := 0
	for _, meta := range qMetas {
		iid := meta.ID.ItemID()
		queue := upstream.queues.Get(iid)
		if queue != nil {
			q := queue.(*Queue)
			q.qsize = meta.qsize
			q.updateTime = time.Now()
			continue
		}
		new++
		upstream.queues.Add(NewQueue(upstream, meta))
		upstream.mgr.queueBulk.GetOrAdd(iid,
			func(item slicemap.Item) slicemap.Item {
				if item == nil {
					newTotal++
					pack := NewPack(meta.ID)
					pack.Add(upstream)
					return pack
				}
				pack := item.(*QueueUpstreamsPack)
				pack.Add(upstream)
				return nil
			},
		)
	}
	result = upstream.Info()
	result["num"] = len(qMetas)
	result["new"] = new
	result["new_total"] = newTotal
	result["_cost_ms_"] = utils.SinceMS(t)
	return
}

// ExistQueueID TODO
func (upstream *Upstream) ExistQueueID(qid sth.QueueID) bool {
	return nil != upstream.queues.Get(qid.ItemID())
}

func (upstream *Upstream) deleteQueue(qid sth.QueueID) bool {
	iid := qid.ItemID()
	if upstream.queues.Delete(iid) {
		upstream.mgr.queueBulk.GetAndDelete(iid,
			func(item slicemap.Item) bool {
				pack := item.(*QueueUpstreamsPack)
				pack.Delete(upstream.ItemID())
				return pack.Size() <= 0
			},
		)
	}
	return false
}

func (upstream *Upstream) deleteOutdated(qid sth.QueueID, ts time.Time) bool {
	iid := qid.ItemID()
	return upstream.queues.GetAndDelete(iid,
		func(item slicemap.Item) bool {
			queue := item.(*Queue)
			if ts.Sub(queue.updateTime) > 0 {
				upstream.mgr.queueBulk.GetAndDelete(iid,
					func(item slicemap.Item) bool {
						pack := item.(*QueueUpstreamsPack)
						pack.Delete(upstream.ItemID())
						return pack.Size() <= 0
					},
				)
				return true
			}
			return false
		},
	)
}

// DeleteQueues TODO
func (upstream *Upstream) DeleteQueues(queueIDs []sth.QueueID) (result sth.Result) {
	t := time.Now()

	deleted := 0
	for _, qid := range queueIDs {
		if upstream.deleteQueue(qid) {
			deleted++
		}
	}
	result = upstream.Info()
	result["num"] = len(queueIDs)
	result["deleted"] = deleted
	result["_cost_ms_"] = utils.SinceMS(t)
	return
}

// PopRequest TODO
func (upstream *Upstream) PopRequest(qid sth.QueueID) (req *request.Request, qsize int64, err error) {
	if upstream.status != UpstreamWorking {
		err = pod.UnavailableError(fmt.Sprintf("%s %s", upstream.ID, upstream.status))
		return
	}
	upstream.queues.View(qid.ItemID(),
		func(item slicemap.Item) {
			if item == nil {
				err = pod.NotExistError(qid.String())
				return
			}
			queue := item.(*Queue)
			req, qsize, err = queue.Pop()
		},
	)
	return
}
