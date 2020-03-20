package hub

import (
	"sync"

	"github.com/cfhamlet/os-rq-pod/pod"
)

// Downstream TODO
type Downstream struct {
}

// DownstreamManager TODO
type DownstreamManager struct {
	hub *Hub
	*sync.RWMutex
}

// NewDownstreamManager TODO
func NewDownstreamManager(hub *Hub) *DownstreamManager {
	return &DownstreamManager{
		hub,
		&sync.RWMutex{},
	}
}

// Start TODO
func (mgr *DownstreamManager) Start() (err error) {
	mgr.Lock()
	defer mgr.Unlock()

	mgr.hub.waitStop.Add(1)
	return
}

// Stop TODO
func (mgr *DownstreamManager) Stop() {
	mgr.hub.waitStop.Done()
}

// Queues TODO
func (mgr *DownstreamManager) Queues(k int) Result {
	return mgr.hub.upstreamMgr.Queues(k)
}

// GetRequest TODO
func (mgr *DownstreamManager) GetRequest(qid pod.QueueID) (Result, error) {
	return mgr.hub.upstreamMgr.GetRequest(qid)
}
