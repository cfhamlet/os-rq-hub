package hub

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
