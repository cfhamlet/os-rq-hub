package hub

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/cfhamlet/os-rq-pod/pkg/log"
	"github.com/cfhamlet/os-rq-pod/pkg/request"
	"github.com/cfhamlet/os-rq-pod/pkg/utils"
	"github.com/cfhamlet/os-rq-pod/pod"
	"github.com/go-redis/redis/v7"
	"github.com/shirou/gopsutil/process"
	"github.com/spf13/viper"
)

// Result TODO
type Result map[string]interface{}

// ResultAndErrorFunc TODO
type ResultAndErrorFunc func() (Result, error)

// Hub TODO
type Hub struct {
	Client  *redis.Client
	Process *process.Process

	conf   *viper.Viper
	status Status
	*sync.RWMutex

	waitStop      *sync.WaitGroup
	upstreamMgr   *UpstreamManager
	downstreamMgr *DownstreamManager
}

// NewHub TODO
func NewHub(conf *viper.Viper, client *redis.Client) (hub *Hub, err error) {

	proc, err := utils.NewProcess()
	if err != nil {
		return
	}

	hub = &Hub{
		client,
		proc,
		conf,
		Init,
		&sync.RWMutex{},
		&sync.WaitGroup{},
		nil,
		nil,
	}

	return
}

func (hub *Hub) withLockOnWorkStatus(f ResultAndErrorFunc) (result Result, err error) {
	return hub.withLockRLockOnWorkStatus(f, true)
}
func (hub *Hub) withRLockOnWorkStatus(f ResultAndErrorFunc) (result Result, err error) {
	return hub.withLockRLockOnWorkStatus(f, false)
}
func (hub *Hub) withLockRLockOnWorkStatus(f ResultAndErrorFunc, lock bool) (result Result, err error) {
	if lock {
		hub.Lock()
		defer hub.Unlock()
	} else {
		hub.RLock()
		defer hub.RUnlock()
	}
	if !workStatus(hub.status) {
		err = UnavailableError(hub.status)
		return
	}
	return f()
}

func workStatus(status Status) bool {
	return status == Working || status == Paused
}

func stopStatus(status Status) bool {
	return status == Stopping || status == Stopped
}

func (hub *Hub) startUpstreamMgr() (err error) {
	upstreamMgr := NewUpstreamManager(hub)
	err = upstreamMgr.LoadUpstreams()

	hub.Lock()
	defer hub.Unlock()

	if err == nil {
		err = hub.setStatus(Preparing)
		if err == nil {
			hub.upstreamMgr = upstreamMgr
			err = hub.upstreamMgr.Start()
		}
	}
	return
}

func (hub *Hub) startDownstreamMgr() (err error) {
	hub.Lock()
	defer hub.Unlock()

	err = hub.setStatus(Preparing)
	if err == nil {
		hub.downstreamMgr = NewDownstreamManager(hub)
		err = hub.downstreamMgr.Start()
	}

	return
}

// OnStart TODO
func (hub *Hub) OnStart() (err error) {
	hub.Lock()
	err = hub.setStatus(Preparing)
	hub.Unlock()
	if err != nil {
		return
	}

	err = hub.startUpstreamMgr()
	if err == nil {
		err = hub.startDownstreamMgr()
	}

	hub.Lock()
	defer hub.Unlock()
	if err == nil {
		err = hub.setStatus(Working)
	}

	switch err.(type) {
	case UnavailableError:
		if stopStatus(hub.status) {
			log.Logger.Warning("stop when starting")
			return nil
		}
	}

	return
}

// OnStop TODO
func (hub *Hub) OnStop() (err error) {
	hub.Lock()
	defer hub.Unlock()

	err = hub.setStatus(Stopping)
	if err == nil {
		if hub.downstreamMgr != nil {
			hub.downstreamMgr.Stop()
		}
		if hub.upstreamMgr != nil {
			hub.upstreamMgr.Stop()
		}
		hub.waitStop.Wait()
		err = hub.setStatus(Stopped)
	}
	if err == nil {
		log.Logger.Info("hub stopped")
	} else {
		log.Logger.Errorf("hub stopped fail %s", err)
	}
	return
}

// Resume TODO
func (hub *Hub) Resume() (Result, error) {
	return hub.withLockOnWorkStatus(
		func() (result Result, err error) {
			err = hub.setStatus(Working)
			if err == nil {
				result = hub.metaInfo()
			}
			return
		},
	)
}

// Pause TODO
func (hub *Hub) Pause() (Result, error) {
	return hub.withLockOnWorkStatus(
		func() (result Result, err error) {
			err = hub.setStatus(Paused)
			if err == nil {
				result = hub.metaInfo()
			}
			return
		},
	)
}

func (hub *Hub) metaInfo() Result {
	return Result{
		"status": hub.status,
		"process": Result{
			"pid":    os.Getpid(),
			"memory": utils.MemoryInfo(hub.Process),
			"cpu": Result{
				"percent": utils.CPUPercent(hub.Process),
			},
		},
		"upstreams_info": hub.upstreamMgr.Info(),
		"qeueus_stats": Result{
			"total": hub.upstreamMgr.QueuesNum(),
		},
	}
}

// Info TODO
func (hub *Hub) Info() (result Result, err error) {
	hub.RLock()
	defer hub.RUnlock()

	result = hub.metaInfo()

	t := time.Now()
	memoryInfo, err := hub.Client.Info("memory").Result()
	r := Result{"_cost_ms_": utils.SinceMS(t)}

	if err == nil {
		k, v := utils.ParseRedisInfo(memoryInfo, "used_memory_rss")
		if k != "" {
			r[k] = v
		}
	} else {
		err = fmt.Errorf("redis error %w", err)
	}
	result["redis"] = r

	return
}

// UpstreamInfo TODO
func (hub *Hub) UpstreamInfo(id UpstreamID) (result Result, err error) {
	return hub.withRLockOnWorkStatus(
		func() (Result, error) {
			return hub.upstreamMgr.UpstreamInfo(id)
		},
	)
}

// Queues TODO
func (hub *Hub) Queues(k int) (Result, error) {
	return hub.withRLockOnWorkStatus(
		func() (result Result, err error) {
			if hub.status != Working {
				err = UnavailableError(hub.status)
			} else {
				result = hub.downstreamMgr.Queues(k)
			}
			return
		},
	)
}

// PauseUpstream TODO
func (hub *Hub) PauseUpstream(id UpstreamID) (result Result, err error) {
	return hub.withRLockOnWorkStatus(
		func() (Result, error) {
			return hub.upstreamMgr.PauseUpstream(id)
		},
	)
}

// ResumeUpstream TODO
func (hub *Hub) ResumeUpstream(id UpstreamID) (result Result, err error) {
	return hub.withRLockOnWorkStatus(
		func() (Result, error) {
			return hub.upstreamMgr.ResumeUpstream(id)
		},
	)
}

// DeleteUpstream TODO
func (hub *Hub) DeleteUpstream(id UpstreamID) (result Result, err error) {
	return hub.withRLockOnWorkStatus(
		func() (Result, error) {
			return hub.upstreamMgr.DeleteUpstream(id)
		},
	)
}

// AddUpstream TODO
func (hub *Hub) AddUpstream(storeMeta *UpstreamStoreMeta) (Result, error) {
	return hub.withRLockOnWorkStatus(
		func() (Result, error) {
			storeMeta.Status = UpstreamWorking
			return hub.upstreamMgr.AddUpstream(storeMeta)
		},
	)
}

// GetRequest TODO
func (hub *Hub) GetRequest(qid pod.QueueID) (req *request.Request, err error) {
	hub.RLock()
	defer hub.RUnlock()
	if hub.status != Working {
		err = UnavailableError(hub.status)
		return
	}
	return hub.downstreamMgr.GetRequest(qid)
}

func (hub *Hub) setStatus(newStatus Status) (err error) {
	oldStatus := hub.status

	if oldStatus == newStatus {
		return
	}
	e := UnavailableError(utils.Text(oldStatus))
	switch hub.status {
	case Init:
		switch newStatus {
		case Preparing:
		default:
			err = e
		}
	case Preparing:
		switch newStatus {
		case Working:
			fallthrough
		case Paused:
			fallthrough
		case Stopping:
		default:
			err = e
		}
	case Working:
		switch newStatus {
		case Paused:
			fallthrough
		case Stopping:
		default:
			err = e
		}
	case Paused:
		switch newStatus {
		case Working:
			fallthrough
		case Stopping:
		default:
			err = e
		}
	case Stopping:
		switch newStatus {
		case Stopped:
		default:
			err = e
		}
	case Stopped:
		err = e
	}
	if err == nil {
		hub.status = newStatus
	}
	return
}

// Upstreams TODO
func (hub *Hub) Upstreams(status UpstreamStatus) (Result, error) {
	hub.RLock()
	defer hub.RUnlock()
	return hub.upstreamMgr.Upstreams(status)
}
