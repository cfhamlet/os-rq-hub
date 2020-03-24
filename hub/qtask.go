package hub

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
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
	quickStop  *StopCtx
	waitStop   *StopCtx
	client     *http.Client
}

// NewUpdateQueuesTask TODO
func NewUpdateQueuesTask(upstream *Upstream) *UpdateQueuesTask {
	return &UpdateQueuesTask{
		upstream,
		[]operate{},
		NewStopCtx(),
		NewStopCtx(),
		&http.Client{},
	}
}

func (task *UpdateQueuesTask) getQueues() (result Result, err error) {
	upstream := task.upstream
	status := upstream.Status()
	if status == UpstreamPaused {
		log.Logger.Warningf("upstream %s paused", upstream.ID)
		return
	}
	req, err := http.NewRequestWithContext(
		task.waitStop.ctx,
		"POST",
		upstream.API,
		nil,
	)
	if err != nil {
		log.Logger.Errorf("upstream %s new request %s", upstream.ID, err)
		_, _ = upstream.mgr.SetStatus(upstream.ID, UpstreamUnavailable)
		return
	}

	resp, err := task.client.Do(req)
	if err != nil {
		log.Logger.Errorf("upstream %s response %s", upstream.ID, err)
		_, _ = upstream.mgr.SetStatus(upstream.ID, UpstreamUnavailable)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Logger.Errorf("upstream %s code %d", upstream.ID, resp.StatusCode)
		_, _ = upstream.mgr.SetStatus(upstream.ID, UpstreamUnavailable)
		return
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Logger.Errorf("upstream %s read %s", upstream.ID, err)
		_, _ = upstream.mgr.SetStatus(upstream.ID, UpstreamUnavailable)
		return
	}

	result = Result{}
	err = json.Unmarshal(body, &result)
	if err != nil {
		log.Logger.Errorf("upstream %s parse %s", upstream.ID, err)
		_, _ = upstream.mgr.SetStatus(upstream.ID, UpstreamUnavailable)
		return
	}
	return
}

func (task *UpdateQueuesTask) updateQueues() {
	result, err := task.getQueues()
	fmt.Println(result, err)
}

func (task *UpdateQueuesTask) sleep() {
	select {
	case <-task.quickStop.Done():
	case <-time.After(time.Second):
	}
}

func (task *UpdateQueuesTask) run() {
	task.upstream.mgr.waitStop.Add(1)
	defer task.upstream.mgr.waitStop.Done()

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
	task.waitStop.Stop()
}

func (task *UpdateQueuesTask) clear() {
	opt := "stop"
	status := UpstreamStopped
	if task.upstream.status == UpstreamRemoving {
		opt = "delete"
		status = UpstreamRemoved
	}
	result, err := task.upstream.mgr.SetStatus(task.upstream.ID, status)
	log.Logger.Infof("upstream %s %s %v %v", task.upstream.ID, opt, result, err)
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
	task.quickStop.Stop()
	select {
	case <-task.waitStop.Done():
	case <-time.After(time.Second * 10):
		task.waitStop.Stop()
	}
}
