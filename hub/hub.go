package hub

import (
	"fmt"
	"sync"
	"time"

	"github.com/cfhamlet/os-rq-pod/pkg/utils"
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
	stLocker      *sync.RWMutex
	upstreamMgr   *UpstreamManager
	downstreamMgr *DownstreamManager
}

// OnStart TODO
func (hub *Hub) OnStart() (err error) {
	_, err = hub.setStatus(Working)
	return
}

// OnStop TODO
func (hub *Hub) OnStop() (err error) {
	_, err = hub.setStatus(Stopping)
	if err != nil {
		return
	}
	_, err = hub.setStatus(Stopped)
	if err != nil {
		return
	}
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
		Preparing, &sync.RWMutex{},
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

	return
}

// Info TODO
func (hub *Hub) Info() (result Result, err error) {
	hub.stLocker.RLock()
	defer hub.stLocker.RUnlock()

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

func (hub *Hub) setStatus(status Status) (Result, error) {
	hub.stLocker.Lock()
	defer hub.stLocker.Unlock()

	hub.status = status
	return hub.metaInfo(), nil
}

// Queues TODO
func (hub *Hub) Queues(k int) (result Result) {
	return hub.downstreamMgr.Queues(k)
}
