package hub

import (
	"fmt"
	"sync"
	"time"

	"github.com/cfhamlet/os-rq-pod/pkg/utils"
	"github.com/cfhamlet/os-rq-pod/pod"
	"github.com/go-redis/redis/v7"
	"github.com/shirou/gopsutil/process"
	"github.com/spf13/viper"
)

// Status type
type Status string

// Status enum
const (
	Working   Status = "working"
	Paused    Status = "paused"
	Preparing Status = "preparing"
	Stopping  Status = "stopping"
	Stopped   Status = "stopped"
)

// Result TODO
type Result map[string]interface{}

// Hub TODO
type Hub struct {
	Client  *redis.Client
	Process *process.Process

	conf          *viper.Viper
	status        Status
	locker        *sync.RWMutex
	waitStop      *sync.WaitGroup
	upstreamMgr   *UpstreamManager
	downstreamMgr *DownstreamManager
}

// OnStart TODO
func (hub *Hub) OnStart() (err error) {
	hub.locker.Lock()
	defer hub.locker.Unlock()

	hub.setStatus(Preparing)
	hub.waitStop.Add(2)

	err = hub.start()
	return
}

// Resume TODO
func (hub *Hub) Resume() (result Result, err error) {
	hub.locker.Lock()
	defer hub.locker.Unlock()

	if hub.status == Working {
		result = hub.metaInfo()
	} else if hub.status == Paused {
		err = hub.start()
		if err == nil {
			result = hub.metaInfo()
		}
	} else {
		err = UnavailableError(hub.status)
	}

	return
}

func (hub *Hub) start() (err error) {
	if hub.status == Stopping || hub.status == Stopped {
		err = UnavailableError(hub.status)
	} else if hub.status == Working {
	} else {
		hub.setStatus(Working)
	}
	return
}

// Pause TODO
func (hub *Hub) Pause() (result Result, err error) {
	hub.locker.Lock()
	defer hub.locker.Unlock()

	if hub.status == Paused || hub.status == Working {
		if hub.status == Working {
			hub.setStatus(Paused)
		}
		result = hub.metaInfo()
	} else {
		err = UnavailableError(hub.status)
	}
	return
}

// OnStop TODO
func (hub *Hub) OnStop() (err error) {
	hub.locker.Lock()
	defer hub.locker.Unlock()

	if hub.status == Stopping || hub.status == Stopped {
		return
	}

	hub.setStatus(Stopping)
	hub.downstreamMgr.Stop()
	hub.upstreamMgr.Stop()
	hub.waitStop.Wait()
	hub.setStatus(Stopped)
	return
}

// NewHub TODO
func NewHub(conf *viper.Viper, client *redis.Client) (hub *Hub, err error) {

	proc, err := utils.NewProcess()
	if err != nil {
		return
	}

	hub = &Hub{
		client, proc, conf,
		Stopped, &sync.RWMutex{}, &sync.WaitGroup{},
		nil, nil,
	}

	upMgr := NewUpstreamManager(hub)
	downMgr := NewDownstreamManager(hub)

	hub.upstreamMgr = upMgr
	hub.downstreamMgr = downMgr

	return
}

func (hub *Hub) metaInfo() (result Result) {
	result = Result{}
	result["status"] = hub.status

	v, _ := hub.Process.MemoryInfo()
	c, _ := hub.Process.CPUPercent()
	result["process"] = Result{"memory": v, "cpu": Result{"percent": c}}
	result["upstreams"] = hub.upstreamMgr.Info()
	return
}

// Info TODO
func (hub *Hub) Info() (result Result, err error) {
	hub.locker.RLock()
	defer hub.locker.RUnlock()

	result = hub.metaInfo()

	t := time.Now()
	memoryInfo, err := hub.Client.Info("memory").Result()

	if err == nil {
		r := Result{"latency_ms": float64(time.Since(t)) / 1000000}
		k, v := utils.ParseRedisInfo(memoryInfo, "used_memory_rss")
		if k != "" {
			r[k] = v
		}
		result["redis"] = r
	} else {
		err = fmt.Errorf("redis error %w", err)
	}

	return
}

func (hub *Hub) setStatus(status Status) {
	hub.status = status
}

// Queues TODO
func (hub *Hub) Queues(k int) (result Result, err error) {
	hub.locker.RLock()
	defer hub.locker.RUnlock()
	if hub.status != Working && hub.status != Paused {
		err = UnavailableError(hub.status)
	} else {
		result = hub.downstreamMgr.Queues(k)
	}
	return
}

// AddUpstream TODO
func (hub *Hub) AddUpstream(meta *UpstreamMeta) (result Result, err error) {
	hub.locker.RLock()
	defer hub.locker.RUnlock()

	if hub.status != Working && hub.status != Paused {
		err = UnavailableError(hub.status)
	} else {
		result, err = hub.upstreamMgr.AddUpstream(meta)
	}
	return
}

// GetRequest TODO
func (hub *Hub) GetRequest(qid pod.QueueID) (result Result, err error) {
	hub.locker.RLock()
	defer hub.locker.RUnlock()
	if hub.status != Working {
		err = UnavailableError(hub.status)
	} else {
		result, err = hub.downstreamMgr.GetRequest(qid)
	}
	return
}
