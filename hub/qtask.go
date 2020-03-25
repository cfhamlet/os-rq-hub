package hub

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/cfhamlet/os-rq-pod/pkg/log"
	"github.com/cfhamlet/os-rq-pod/pod"
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

// APIError TODO
type APIError struct {
	reason string
	err    error
}

func (e APIError) Error() string {
	return fmt.Sprintf("%s %s", e.reason, e.err)
}

// apiPath TODO
func (task *UpdateQueuesTask) apiPath(path string) string {
	base, err := url.Parse(task.upstream.API)
	if err != nil {
		return ""
	}
	u, err := url.Parse(path)

	return base.ResolveReference(u).String()
}

func (task *UpdateQueuesTask) getQueues() (queueIDs []pod.QueueID, err error) {
	req, err := http.NewRequestWithContext(
		task.waitStop.ctx,
		"POST",
		task.apiPath("queues/"),
		nil,
	)
	if err != nil {
		err = APIError{"new request", err}
		return
	}

	resp, err := task.client.Do(req)
	if err != nil {
		err = APIError{"response", err}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		err = APIError{fmt.Sprintf("http code %d", resp.StatusCode), nil}
		return
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		err = APIError{"read", err}
		return
	}

	result := Result{}
	err = json.Unmarshal(body, &result)
	if err != nil {
		return
	}
	queueIDs, err = task.queueIDsFromResult(result)
	return
}

func (task *UpdateQueuesTask) queueIDsFromResult(result Result) (queueIDs []pod.QueueID, err error) {
	queueIDs = []pod.QueueID{}
	qs, ok := result["queues"]
	if !ok {
		err = fmt.Errorf(`"queues" not exist in %s`, result)
		return
	}
	ql := qs.([]interface{})
	total := 0
	new := 0
	for _, qt := range ql {
		total++
		qr := qt.(map[string]interface{})
		q, ok := qr["qid"]
		if !ok {
			err = fmt.Errorf(`"queues" not exist in %s`, q)
			break
		}
		s := q.(string)
		qid, err := pod.QueueIDFromString(s)
		if err != nil {
			break
		}
		if !task.upstream.ExistQueueID(qid) {
			queueIDs = append(queueIDs, qid)
			new++
		}
	}
	if err == nil {
		log.Logger.Debugf(task.logMsg("parse queues total: %d new: %d", total, new))
	}
	return
}

func (task *UpdateQueuesTask) logMsg(format string, args ...interface{}) string {
	msg := fmt.Sprintf(format, args...)
	return fmt.Sprintf("<upstream %s> %s", task.upstream.ID, msg)
}

func (task *UpdateQueuesTask) updateQueues() {
	upstream := task.upstream
	status := upstream.Status()
	if status == UpstreamPaused {
		log.Logger.Warningf(task.logMsg("paused"))
		return
	}
	queueIDs, err := task.getQueues()
	if err != nil {
		switch err.(type) {
		case APIError:
			_, _ = upstream.mgr.SetStatus(upstream.ID, UpstreamUnavailable)
		}
		log.Logger.Error(task.logMsg("%s", err))
		return
	} else if len(queueIDs) <= 0 {
		log.Logger.Warning(task.logMsg("0 queues"))
		return
	}
	var result Result
	result, err = upstream.mgr.UpdateUpStreamQueueIDs(upstream.ID, queueIDs)
	if err != nil {
		log.Logger.Errorf(task.logMsg("%s", err))
	} else {
		log.Logger.Debugf(task.logMsg("%v", result))
	}
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
	log.Logger.Info(task.logMsg("%s %v %v", opt, result, err))
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
