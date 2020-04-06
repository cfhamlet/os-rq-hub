package hub

import (
	"sync"

	"github.com/cfhamlet/os-rq-pod/pkg/serv"
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
func (core *Core) initDownstreamMgr() error {
	_, err := core.DoWithLock(
		func() (interface{}, error) {
			err := core.SetStatus(serv.Preparing, false)
			if err == nil {
				core.DownstreamMgr = NewDownstreamManager(core)
				err = core.DownstreamMgr.Start()
			}
			return nil, err
		}, false)

	return err
}

// Setup TODO
func (mgr *DownstreamManager) Setup() error {
	_, err := mgr.core.DoWithLock(
		func() (interface{}, error) {
			err := mgr.core.SetStatus(serv.Preparing, false)
			if err == nil {
				mgr.core.DownstreamMgr = mgr
				err = mgr.Start()
			}
			return nil, err
		}, false)

	return err
}

// Cleanup TODO
func (mgr *DownstreamManager) Cleanup() error {
	mgr.Stop()
	return nil
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
