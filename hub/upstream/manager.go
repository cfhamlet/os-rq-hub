package upstream

import (
	"context"
	"math/rand"
	"net/url"
	"sync"
	"time"

	"github.com/cfhamlet/os-rq-hub/hub/global"
	plobal "github.com/cfhamlet/os-rq-pod/pod/global"

	"github.com/cfhamlet/os-rq-pod/pkg/log"
	"github.com/cfhamlet/os-rq-pod/pkg/request"
	"github.com/cfhamlet/os-rq-pod/pkg/serv"
	"github.com/cfhamlet/os-rq-pod/pkg/slicemap"
	"github.com/cfhamlet/os-rq-pod/pkg/sth"
	"github.com/cfhamlet/os-rq-pod/pkg/utils"
	"github.com/go-redis/redis/v7"
)

// CallByUpstream TODO
type CallByUpstream func(*Upstream) (sth.Result, error)

// NewPack TODO
func NewPack(qid sth.QueueID) *QueueUpstreamsPack {
	return &QueueUpstreamsPack{qid, slicemap.New()}
}

// Manager TODO
type Manager struct {
	serv            *serv.Serv
	statusUpstreams map[Status]*slicemap.Viewer
	queueBulk       *QueueBulk
	waitStop        *sync.WaitGroup
	rand            *rand.Rand
	client          *redis.Client
	*utils.BulkLock
}

// OnStart TODO
func (mgr *Manager) OnStart(context.Context) (err error) {
	err = mgr.Load()
	if err == nil {
		err = mgr.Start()
	}
	return
}

// OnStop TODO
func (mgr *Manager) OnStop(context.Context) error {
	mgr.Stop()
	return nil
}

// NewManager TODO
func NewManager(serv *serv.Serv, client *redis.Client) *Manager {
	statusUpstreams := map[Status]*slicemap.Viewer{}
	for _, status := range UpstreamStatusList {
		statusUpstreams[status] = slicemap.NewViewer(nil)
	}
	mgr := &Manager{
		serv,
		statusUpstreams,
		nil,
		&sync.WaitGroup{},
		rand.New(rand.NewSource(time.Now().UnixNano())),
		client,
		utils.NewBulkLock(1024),
	}

	qb := NewQueueBulk(mgr, 1024)
	mgr.queueBulk = qb
	return mgr
}

// Load TODO
func (mgr *Manager) Load() (err error) {

	log.Logger.Info("load upstreams start")

	scanner := utils.NewScanner(mgr.client, "hscan", global.RedisUpstreamsKey, "*", 1000)
	err = scanner.Scan(
		func(keys []string) (err error) {
			isKey := false
			for _, key := range keys {
				err = mgr.serv.SetStatus(serv.Preparing)
				if err != nil {
					return
				}
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

	loadUpstreams := map[Status]int{}
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

func (mgr *Manager) addUpstream(storeMeta *StoreMeta) (upstream *Upstream, err error) {
	iid := storeMeta.ID.ItemID()
	for status := range mgr.statusUpstreams {
		upstreams := mgr.statusUpstreams[status]
		item := upstreams.Get(iid)
		if item != nil {
			err = plobal.ExistError(storeMeta.ID)
			return
		}
	}
	upstream = NewUpstream(mgr, storeMeta.Meta)
	if storeMeta.Status == UpstreamInit {
		err = mgr.setStatus(upstream, UpstreamWorking)
	} else {
		upstream.status = storeMeta.Status
		mgr.statusUpstreams[storeMeta.Status].Add(upstream)
	}
	return
}

// AddUpstream TODO
func (mgr *Manager) AddUpstream(storeMeta *StoreMeta) (result sth.Result, err error) {
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

func (mgr *Manager) startUpstream(id ID) (sth.Result, error) {
	return mgr.doMustExist(id,
		func(upstream *Upstream) (result sth.Result, err error) {
			err = upstream.Start()
			result = upstream.Info()
			return
		},
	)
}

// Start TODO
func (mgr *Manager) Start() (err error) {
	for _, upstreams := range mgr.statusUpstreams {
		iter := slicemap.NewBaseIter(upstreams.Map)
		iter.Iter(
			func(item slicemap.Item) bool {
				upstream := item.(*Upstream)
				err = upstream.Start()
				return err == nil
			},
		)
	}
	return
}

// Stop TODO
func (mgr *Manager) Stop() {
	for status, upstreams := range mgr.statusUpstreams {
		if StopUpstreamStatus(status) {
			continue
		}
		iter := slicemap.NewBaseIter(upstreams.Map)
		iter.Iter(
			func(item slicemap.Item) bool {
				upstream := item.(*Upstream)
				go func() {
					_, _ = mgr.withLockMustExist(upstream.ID,
						func(upstream *Upstream) (sth.Result, error) {
							err := upstream.Stop()
							if err != nil {
								log.Logger.Warning(upstream.logFormat("stop fail %s", err))
							}
							return nil, err
						}, false)
				}()
				return true
			},
		)
	}

	mgr.waitStop.Wait()
}

// ResumeUpstream TODO
func (mgr *Manager) ResumeUpstream(id ID) (result sth.Result, err error) {
	return mgr.SetStatus(id, UpstreamWorking)
}

// PauseUpstream TODO
func (mgr *Manager) PauseUpstream(id ID) (result sth.Result, err error) {
	return mgr.SetStatus(id, UpstreamPaused)
}

// DeleteUpstream TODO
func (mgr *Manager) DeleteUpstream(id ID) (result sth.Result, err error) {
	return mgr.withLockMustExist(id,
		func(upstream *Upstream) (result sth.Result, err error) {
			err = upstream.Destory()
			result = upstream.Info()
			return
		}, false)
}

// UpstreamInfo TODO
func (mgr *Manager) UpstreamInfo(id ID) (result sth.Result, err error) {
	return mgr.withLockMustExist(id,
		func(upstream *Upstream) (result sth.Result, err error) {
			return upstream.Info(), err
		}, true)
}

// Info TODO
func (mgr *Manager) Info() (result sth.Result) {
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
func (mgr *Manager) xxQueues(k int) (result sth.Result) {
	t := time.Now()
	out := []sth.Result{}
	if mgr.statusUpstreams[UpstreamWorking].Size() <= 0 {
	} else {
		selector := NewRandSelector(mgr, k)
		out = append(out, selector.Select()...)
	}
	return sth.Result{
		"k":         k,
		"queues":    out,
		"count":     len(out),
		"total":     mgr.queueBulk.Size(),
		"_cost_ms_": utils.SinceMS(t),
	}
}

// Queues TODO
func (mgr *Manager) Queues(k int) (result sth.Result) {
	r, _ := mgr.serv.DoWithLockOnWorkStatus(
		func() (interface{}, error) {
			return mgr.xxQueues(k), nil
		}, true, true)
	return r.(sth.Result)
}

// Upstreams TODO
func (mgr *Manager) Upstreams(status Status) (result sth.Result, err error) {
	upstreams := mgr.statusUpstreams[status]
	iter := slicemap.NewBaseIter(upstreams.Map)
	result = sth.Result{
		"status": utils.Text(status),
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
	return
}

// AllUpstreams TODO
func (mgr *Manager) AllUpstreams() (result sth.Result, err error) {
	result = sth.Result{}
	total := 0
	out := []sth.Result{}
	for _, upstreams := range mgr.statusUpstreams {
		t := upstreams.Size()
		if t <= 0 {
			continue
		}
		iter := slicemap.NewBaseIter(upstreams.Map)
		iter.Iter(func(item slicemap.Item) bool {
			upstream := item.(*Upstream)
			out = append(out, upstream.Info())
			return true
		})

	}
	result["upstreams"] = out
	result["total"] = total
	return
}

// UpdateUpStreamQueues TODO
func (mgr *Manager) UpdateUpStreamQueues(id ID, qMetas []*QueueMeta) (sth.Result, error) {
	return mgr.withLockMustExist(id,
		func(upstream *Upstream) (result sth.Result, err error) {
			return upstream.UpdateQueues(qMetas), nil
		}, true)
}

// DeleteQueues TODO
func (mgr *Manager) DeleteQueues(id ID, queueIDs []sth.QueueID) (sth.Result, error) {
	return mgr.withLockMustExist(id,
		func(upstream *Upstream) (result sth.Result, err error) {
			return upstream.DeleteQueues(queueIDs), nil
		}, true)
}

// DeleteOutdated TODO
func (mgr *Manager) DeleteOutdated(qid sth.QueueID, ids []ID, ts time.Time) {
	for _, id := range ids {
		_, _ = mgr.withLockMustExist(id,
			func(upstream *Upstream) (sth.Result, error) {
				upstream.deleteOutdated(qid, ts)
				return nil, nil
			}, true)
	}
}

// PopRequest TODO
func (mgr *Manager) PopRequest(qid sth.QueueID) (req *request.Request, err error) {
	r, e := mgr.serv.DoWithLockOnWorkStatus(
		func() (interface{}, error) {
			return mgr.xxPopRequest(qid)
		}, true, true)
	return r.(*request.Request), e
}

func (mgr *Manager) xxPopRequest(qid sth.QueueID) (req *request.Request, err error) {
	return mgr.queueBulk.PopRequest(qid)
}

func (mgr *Manager) doMustExist(id ID, f CallByUpstream) (result sth.Result, err error) {
	iid := id.ItemID()
	var item slicemap.Item
	for status := range mgr.statusUpstreams {
		item = mgr.statusUpstreams[status].Get(iid)
		if item != nil {
			return f(item.(*Upstream))
		}
	}
	err = plobal.NotExistError(id)
	return
}

func (mgr *Manager) setStatus(upstream *Upstream, newStatus Status) (err error) {
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
func (mgr *Manager) SetStatus(id ID, newStatus Status) (sth.Result, error) {
	return mgr.withLockMustExist(id,
		func(upstream *Upstream) (result sth.Result, err error) {
			err = mgr.setStatus(upstream, newStatus)
			return upstream.Info(), err
		}, false)
}

func (mgr *Manager) withLockMustExist(id ID, f CallByUpstream, rLock bool) (result sth.Result, err error) {
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
