package hub

import (
	"context"
	"fmt"
	"time"

	"github.com/cfhamlet/os-rq-pod/pkg/log"
)

type operate func()

// StopCtx TODO
type StopCtx struct {
	ctx  context.Context
	stop context.CancelFunc
}

// NewStopCtx TODO
func NewStopCtx() *StopCtx {
	ctx, cancel := context.WithCancel(context.Background())
	return &StopCtx{ctx, cancel}
}

// Done TODO
func (c *StopCtx) Done() <-chan struct{} {
	return c.ctx.Done()
}

// Stop TODO
func (c *StopCtx) Stop() {
	c.stop()
}

// UpdateQueuesTask TODO
type UpdateQueuesTask struct {
	upstream   *Upstream
	operations []operate
	hardStop   *StopCtx
	softStop   *StopCtx
}

// NewUpdateQueuesTask TODO
func NewUpdateQueuesTask(upstream *Upstream) *UpdateQueuesTask {
	return &UpdateQueuesTask{
		upstream,
		[]operate{},
		NewStopCtx(),
		NewStopCtx(),
	}
}

func (task *UpdateQueuesTask) updateQueues() {
	fmt.Println(task.upstream.ID)
}

func (task *UpdateQueuesTask) sleep() {
	select {
	case <-task.hardStop.Done():
	case <-time.After(time.Second):
	}
}

func (task *UpdateQueuesTask) run() {
	task.upstream.mgr.waitStop.Add(1)
	for {
		for _, call := range task.operations {
			if stopUpstreamStatus(task.upstream.status) {
				goto Done
			}
			call()
		}
	}
Done:
	task.clear()
	task.softStop.Stop()
	task.upstream.mgr.waitStop.Done()
}

func (task *UpdateQueuesTask) clear() {
	if task.upstream.status == UpstreamRemoving {
		result, err := task.upstream.mgr.SetStatus(task.upstream.ID, UpstreamRemoved)
		log.Logger.Infof("delete upstream %v %v", result, err)
	} else if task.upstream.status == UpstreamStopping {
		result, err := task.upstream.mgr.SetStatus(task.upstream.ID, UpstreamStopped)
		log.Logger.Infof("stop upstream %v %v", result, err)
	}
}

// Start TODO
func (task *UpdateQueuesTask) Start() {
	if len(task.operations) > 0 {
		return
	}
	task.operations = append(task.operations,
		task.updateQueues,
		task.sleep,
	)
	task.run()
}

// Stop TODO
func (task *UpdateQueuesTask) Stop() {
	task.hardStop.Stop()
	select {
	case <-task.softStop.Done():
	case <-time.After(time.Second * 10):
		task.softStop.Stop()
	}
}
