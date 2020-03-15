package hub

import "github.com/cfhamlet/os-rq-pod/pod"

// Downstream TODO
type Downstream struct {
}

// DownstreamManager TODO
type DownstreamManager struct {
	hub *Hub
}

// NewDownstreamManager TODO
func NewDownstreamManager(hub *Hub) *DownstreamManager {
	return &DownstreamManager{hub}
}

// Queues TODO
func (mgr *DownstreamManager) Queues(k int) Result {
	return mgr.hub.upstreamMgr.Queues(k)
}

// GetRequest TODO
func (mgr *DownstreamManager) GetRequest(qid pod.QueueID) (Result, error) {
	return mgr.hub.upstreamMgr.GetRequest(qid)
}
