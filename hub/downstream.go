package hub

import (
	"sync"
)

// Downstream TODO
type Downstream struct {
}

// DownstreamManager TODO
type DownstreamManager struct {
	core *Core
	*sync.RWMutex
}

// NewDownstreamManager TODO
func NewDownstreamManager(core *Core) *DownstreamManager {
	return &DownstreamManager{
		core,
		&sync.RWMutex{},
	}
}

// Start TODO
func (mgr *DownstreamManager) Start() (err error) {
	mgr.Lock()
	defer mgr.Unlock()

	mgr.core.waitStop.Add(1)
	return
}

// Stop TODO
func (mgr *DownstreamManager) Stop() {
	mgr.core.waitStop.Done()
}
