package hub

import (
	"math/rand"
	"net/url"
	"sync"
	"time"

	"github.com/cfhamlet/os-rq-pod/pkg/log"
	"github.com/cfhamlet/os-rq-pod/pkg/request"
	"github.com/cfhamlet/os-rq-pod/pkg/slicemap"
	"github.com/cfhamlet/os-rq-pod/pkg/sth"
	"github.com/cfhamlet/os-rq-pod/pkg/utils"
	"github.com/cfhamlet/os-rq-pod/pod"
)

// CallByUpstream TODO
type CallByUpstream func(*Upstream) (sth.Result, error)

// NewPack TODO
func NewPack(qid sth.QueueID) *QueueUpstreamsPack {
	return &QueueUpstreamsPack{qid, slicemap.New()}
}

// UpstreamManager TODO
type UpstreamManager struct {
	core            *Core
	statusUpstreams map[UpstreamStatus]*slicemap.Viewer
	queueBulk       *QueueBulk
	waitStop        *sync.WaitGroup
	rand            *rand.Rand
	*utils.BulkLock
}

// NewUpstreamManager TODO
func NewUpstreamManager(core *Core) *UpstreamManager {
	statusUpstreams := map[UpstreamStatus]*slicemap.Viewer{}
	for _, status := range UpstreamStatusList {
		statusUpstreams[status] = slicemap.NewViewer(nil)
	}
	mgr := &UpstreamManager{
		core,
		statusUpstreams,
		nil,
		&sync.WaitGroup{},
		rand.New(rand.NewSource(time.Now().UnixNano())),
		utils.NewBulkLock(1024),
	}

	qb := NewQueueBulk(mgr, 1024)
	mgr.queueBulk = qb
	return mgr
}

// Load TODO
func (mgr *UpstreamManager) Load() (err error) {

	log.Logger.Debug("load upstreams start")

	scanner := utils.NewScanner(mgr.core.Client(), "hscan", RedisUpstreamsKey, "*", 1000)
	err = scanner.Scan(
		func(keys []string) (err error) {
			isKey := false
			for _, key := range keys {
				isKey = !isKey
				if isKey {
					continue
				}

				storeMeta, err := UnmarshalUpstreamStoreMetaJSON([]byte(key))
				if err != nil {
					log.Logger.Warning("invalid meta", key, err)
					continue
				}
				var upstream *Upstream
				upstream, err = mgr.addUpstream(storeMeta)
				if err != nil {
					break
				}
				log.Logger.Info("load upstream", upstream.ID, upstream.Status())
			}
			return
		},
	)

	loadUpstreams := map[UpstreamStatus]int{}
	for status, upstreams := range mgr.statusUpstreams {
		loadUpstreams[status] = upstreams.Size()
	}

	logf := log.Logger.Infof
	args := []interface{}{loadUpstreams, "finish"}
	if err != nil {
		logf = log.Logger.Errorf
		args[1] = err
	}
	logf("load upstreams %v, %s", args...)

	return
}

func (mgr *UpstreamManager) addUpstream(storeMeta *UpstreamStoreMeta) (upstream *Upstream, err error) {
	iid := storeMeta.ID.ItemID()
	for status := range mgr.statusUpstreams {
		upstreams := mgr.statusUpstreams[status]
		item := upstreams.Get(iid)
		if item != nil {
			err = pod.ExistError(storeMeta.ID)
			return
		}
	}
	upstream = NewUpstream(mgr, storeMeta.UpstreamMeta)
	err = mgr.setStatus(upstream, storeMeta.Status)
	return
}

// AddUpstream TODO
func (mgr *UpstreamManager) AddUpstream(storeMeta *UpstreamStoreMeta) (result sth.Result, err error) {
	iid := storeMeta.ID.ItemID()
	mgr.Lock(iid)
	defer mgr.Unlock(iid)
	if storeMeta.ParsedAPI == nil {
		var parsedURL *url.URL
		parsedURL, err = url.Parse(storeMeta.API)
		if err != nil {
			return
		}
		storeMeta.ParsedAPI = parsedURL
	}
	_, err = mgr.addUpstream(storeMeta)
	if err == nil {
		result, err = mgr.startUpstream(storeMeta.ID)
	}
	return
}

func (mgr *UpstreamManager) startUpstream(id UpstreamID) (sth.Result, error) {
	return mgr.doMustExist(id,
		func(upstream *Upstream) (result sth.Result, err error) {
			err = upstream.Start()
			result = upstream.Info()
			return
		},
	)
}

// Start TODO
func (mgr *UpstreamManager) Start() (err error) {
	for _, upstreams := range mgr.statusUpstreams {
		iter := slicemap.NewBaseIter(upstreams.Map)
		iter.Iter(
			func(item slicemap.Item) bool {
				upstream := item.(*Upstream)
				err = upstream.Start()
				if err != nil {
					return false
				}
				return true
			},
		)
	}
	mgr.core.waitStop.Add(1)
	return
}

// Stop TODO
func (mgr *UpstreamManager) Stop() {
	for status, upstreams := range mgr.statusUpstreams {
		if StopUpstreamStatus(status) {
			continue
		}
		iter := slicemap.NewBaseIter(upstreams.Map)
		iter.Iter(
			func(item slicemap.Item) bool {
				upstream := item.(*Upstream)
				go mgr.withLockMustExist(upstream.ID,
					func(upstream *Upstream) (sth.Result, error) {
						err := upstream.Stop()
						if err != nil {
							log.Logger.Warning("stop upstream fail", upstream.ID, err)
						}
						return nil, err
					}, false)
				return true
			},
		)
	}

	mgr.waitStop.Wait()
	mgr.core.waitStop.Done()
}

func (mgr *UpstreamManager) stopUpstream(id UpstreamID) (sth.Result, error) {
	return mgr.doMustExist(id,
		func(upstream *Upstream) (result sth.Result, err error) {
			err = upstream.Stop()
			result = upstream.Info()
			return
		},
	)
}

// ResumeUpstream TODO
func (mgr *UpstreamManager) ResumeUpstream(id UpstreamID) (result sth.Result, err error) {
	return mgr.SetStatus(id, UpstreamWorking)
}

// PauseUpstream TODO
func (mgr *UpstreamManager) PauseUpstream(id UpstreamID) (result sth.Result, err error) {
	return mgr.SetStatus(id, UpstreamPaused)
}

// DeleteUpstream TODO
func (mgr *UpstreamManager) DeleteUpstream(id UpstreamID) (result sth.Result, err error) {
	return mgr.withLockMustExist(id,
		func(upstream *Upstream) (result sth.Result, err error) {
			err = upstream.Destory()
			result = upstream.Info()
			return
		}, false)
}

// UpstreamInfo TODO
func (mgr *UpstreamManager) UpstreamInfo(id UpstreamID) (result sth.Result, err error) {
	return mgr.withLockMustExist(id,
		func(upstream *Upstream) (result sth.Result, err error) {
			return upstream.Info(), err
		}, true)
}

// Info TODO
func (mgr *UpstreamManager) Info() (result sth.Result) {
	st := sth.Result{}
	for status, upstreams := range mgr.statusUpstreams {
		st[utils.Text(status)] = upstreams.Size()
	}
	result = sth.Result{
		"upstreams": st,
		"queues":    mgr.queueBulk.Size(),
	}
	return
}

// Queues TODO
func (mgr *UpstreamManager) xxQueues(k int) (result sth.Result) {
	t := time.Now()
	out := []sth.Result{}
	total := mgr.queueBulk.Size()
	if mgr.statusUpstreams[UpstreamWorking].Size() <= 0 {
	} else {
		selector := NewRandSelector(mgr, k)
		out = append(out, selector.Select()...)
	}
	return sth.Result{
		"k":         k,
		"queues":    out,
		"count":     len(out),
		"total":     total,
		"_cost_ms_": utils.SinceMS(t),
	}
}

// Queues TODO
func (mgr *UpstreamManager) Queues(k int) (result sth.Result) {
	r, _ := mgr.core.DoWithLockOnWorkStatus(
		func() (interface{}, error) {
			return mgr.xxQueues(k), nil
		}, true, true)
	return r.(sth.Result)
}

// Upstreams TODO
func (mgr *UpstreamManager) Upstreams(status UpstreamStatus) (result sth.Result, err error) {
	upstreams := mgr.statusUpstreams[status]
	iter := slicemap.NewBaseIter(upstreams.Map)
	result = sth.Result{
		"status": utils.Text(status),
		"count":  upstreams.Size(),
		"total":  upstreams.Size(),
	}
	out := []sth.Result{}
	iter.Iter(func(item slicemap.Item) bool {
		upstream := item.(*Upstream)
		out = append(out, upstream.Info())
		return true
	},
	)
	result["upstreams"] = out
	result["queues"] = sth.Result{
		"total": mgr.queueBulk.Size(),
	}
	return
}

// UpdateUpStreamQueues TODO
func (mgr *UpstreamManager) UpdateUpStreamQueues(id UpstreamID, qMetas []*QueueMeta) (sth.Result, error) {
	return mgr.withLockMustExist(id,
		func(upstream *Upstream) (result sth.Result, err error) {
			return upstream.UpdateQueues(qMetas), nil
		}, true)
}

// DeleteQueues TODO
func (mgr *UpstreamManager) DeleteQueues(id UpstreamID, queueIDs []sth.QueueID) (sth.Result, error) {
	return mgr.withLockMustExist(id,
		func(upstream *Upstream) (result sth.Result, err error) {
			return upstream.DeleteQueues(queueIDs), nil
		}, true)
}

// DeleteOutdated TODO
func (mgr *UpstreamManager) DeleteOutdated(qid sth.QueueID, ids []UpstreamID, ts time.Time) {
	for _, id := range ids {
		mgr.withLockMustExist(id,
			func(upstream *Upstream) (sth.Result, error) {
				upstream.deleteOutdated(qid, ts)
				return nil, nil
			}, true)
	}
}

// PopRequest TODO
func (mgr *UpstreamManager) PopRequest(qid sth.QueueID) (req *request.Request, err error) {
	r, e := mgr.core.DoWithLockOnWorkStatus(
		func() (interface{}, error) {
			return mgr.xxPopRequest(qid)
		}, true, true)
	return r.(*request.Request), e
}

func (mgr *UpstreamManager) xxPopRequest(qid sth.QueueID) (req *request.Request, err error) {
	return mgr.queueBulk.PopRequest(qid)
}

func (mgr *UpstreamManager) doMustExist(id UpstreamID, f CallByUpstream) (result sth.Result, err error) {
	iid := id.ItemID()
	var item slicemap.Item
	for status := range mgr.statusUpstreams {
		item = mgr.statusUpstreams[status].Get(iid)
		if item != nil {
			return f(item.(*Upstream))
		}
	}
	err = pod.NotExistError(id)
	return
}

func (mgr *UpstreamManager) setStatus(upstream *Upstream, newStatus UpstreamStatus) (err error) {
	oldStatus := upstream.Status()
	err = upstream.SetStatus(newStatus)
	if err == nil && oldStatus != upstream.Status() {
		mgr.statusUpstreams[oldStatus].Delete(upstream.ItemID())
		if newStatus != UpstreamRemoved {
			mgr.statusUpstreams[newStatus].Add(upstream)
		}
	}
	return
}

// SetStatus TODO
func (mgr *UpstreamManager) SetStatus(id UpstreamID, newStatus UpstreamStatus) (sth.Result, error) {
	return mgr.withLockMustExist(id,
		func(upstream *Upstream) (result sth.Result, err error) {
			err = mgr.setStatus(upstream, newStatus)
			return upstream.Info(), err
		}, false)
}

func (mgr *UpstreamManager) withLockMustExist(id UpstreamID, f CallByUpstream, rLock bool) (result sth.Result, err error) {
	iid := id.ItemID()
	if rLock {
		mgr.RLock(iid)
		defer mgr.RUnlock(iid)
	} else {
		mgr.Lock(iid)
		defer mgr.Unlock(iid)
	}
	return mgr.doMustExist(id, f)
}
