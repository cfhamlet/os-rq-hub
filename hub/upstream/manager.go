package upstream

import (
	"context"
	"math/rand"
	"net/url"
	"sync"
	"time"

	"github.com/cfhamlet/os-rq-hub/hub/global"
	plobal "github.com/cfhamlet/os-rq-pod/pod/global"
	"github.com/cfhamlet/os-rq-pod/pod/queuebox"
	"github.com/prep/average"

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

// Manager TODO
type Manager struct {
	serv            *serv.Serv
	statusUpstreams map[Status]*slicemap.Viewer
	queueBulk       *QueueBulk
	waitStop        *sync.WaitGroup
	rand            *rand.Rand
	client          *redis.Client
	reqSpeed        *average.SlidingWindow
	*sync.RWMutex
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
		queuebox.MustNewMinuteWindow(),
		&sync.RWMutex{},
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
	mgr.Lock()
	defer mgr.Unlock()
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
	mgr.RLock()
	toBeStarted := []*Upstream{}
	for _, upstreams := range mgr.statusUpstreams {
		iter := slicemap.NewBaseIter(upstreams.Map)
		iter.Iter(
			func(item slicemap.Item) bool {
				upstream := item.(*Upstream)
				toBeStarted = append(toBeStarted, upstream)
				return true
			},
		)
	}
	mgr.RUnlock()
	for _, upstream := range toBeStarted {
		err = upstream.Start()
		if err != nil {
			break
		}
	}
	return
}

// Stop TODO
func (mgr *Manager) Stop() {
	mgr.RLock()
	toBeStopped := []*Upstream{}
	for status, upstreams := range mgr.statusUpstreams {
		if StopUpstreamStatus(status) {
			continue
		}
		iter := slicemap.NewBaseIter(upstreams.Map)
		iter.Iter(
			func(item slicemap.Item) bool {
				upstream := item.(*Upstream)
				toBeStopped = append(toBeStopped, upstream)
				return true
			},
		)
	}
	mgr.RUnlock()

	for _, upstream := range toBeStopped {
		err := upstream.Stop()
		if err != nil {
			log.Logger.Warning(upstream.logFormat("stop fail %s", err))
		}
	}
	mgr.reqSpeed.Stop()
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

// Queues TODO
func (mgr *Manager) xxQueues(k int) (result sth.Result) {
	mgr.RLock()
	defer mgr.RUnlock()
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
	mgr.RLock()
	defer mgr.RUnlock()
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

// Info TODO
func (mgr *Manager) Info() (result sth.Result, err error) {
	mgr.RLock()
	defer mgr.RUnlock()
	result = sth.Result{}
	total := 0
	out := sth.Result{}
	for status, upstreams := range mgr.statusUpstreams {
		size := upstreams.Size()
		out[utils.Text(status)] = size
		total += size
	}
	result["upstreams"] = out
	result["total"] = total
	result["queues"] = mgr.queueBulk.Size()
	result["speed_5s"] = queuebox.WindowTotal(mgr.reqSpeed, 5)
	return
}

// AllUpstreams TODO
func (mgr *Manager) AllUpstreams() (result sth.Result, err error) {
	mgr.RLock()
	defer mgr.RUnlock()
	result = sth.Result{}
	total := 0
	out := sth.Result{}
	for status, upstreams := range mgr.statusUpstreams {
		s := []sth.Result{}
		t := upstreams.Size()
		if t <= 0 {
			continue
		}
		iter := slicemap.NewBaseIter(upstreams.Map)
		iter.Iter(func(item slicemap.Item) bool {
			upstream := item.(*Upstream)
			s = append(s, upstream.Info())
			total++
			return true
		})
		out[utils.Text(status)] = s
	}
	result["upstreams"] = out
	result["total"] = total
	result["queues"] = mgr.queueBulk.Size()
	result["speed_5s"] = queuebox.WindowTotal(mgr.reqSpeed, 5)
	return
}

// UpdateQueues TODO
func (mgr *Manager) UpdateQueues(id ID, qMetas []*QueueMeta) (sth.Result, error) {
	return mgr.withLockMustExist(id,
		func(upstream *Upstream) (result sth.Result, err error) {
			uNew := 0
			gNew := 0
			t := time.Now()
			for _, qMeta := range qMetas {
				u, g := mgr.queueBulk.UpdateUpstream(upstream, qMeta)
				uNew += u
				gNew += g
			}
			return sth.Result{"new": uNew, "new_global": gNew, "_cost_ms_": utils.SinceMS(t)}, nil
		}, true)
}

// DeleteQueues TODO
func (mgr *Manager) DeleteQueues(id ID, queueIDs []sth.QueueID, ts *time.Time) (sth.Result, error) {
	return mgr.withLockMustExist(id,
		func(upstream *Upstream) (result sth.Result, err error) {
			return mgr.queueBulk.ClearUpstream(id, queueIDs, ts), nil
		}, true)
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
	req, err = mgr.queueBulk.PopRequest(qid)
	if err == nil {
		mgr.reqSpeed.Add(1)
	}
	return
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
	if rLock {
		mgr.RLock()
		defer mgr.RUnlock()
	} else {
		mgr.Lock()
		defer mgr.Unlock()
	}
	return mgr.doMustExist(id, f)
}
