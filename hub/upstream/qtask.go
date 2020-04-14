package upstream

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/cfhamlet/os-rq-pod/pkg/json"
	"github.com/cfhamlet/os-rq-pod/pkg/log"
	"github.com/cfhamlet/os-rq-pod/pkg/slicemap"
	"github.com/cfhamlet/os-rq-pod/pkg/sth"
	"github.com/cfhamlet/os-rq-pod/pod/queuebox"
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

// APIError TODO
type APIError struct {
	which string
	err   error
}

func (e APIError) Error() string {
	return fmt.Sprintf("%s %s", e.which, e.err)
}

// apiPath TODO
func (task *UpdateQueuesTask) apiPath(path string) (api string, err error) {
	u, err := url.Parse(path)
	if err == nil {
		api = task.upstream.ParsedAPI.ResolveReference(u).String()
	}

	return
}

func (task *UpdateQueuesTask) getQueueMetas() (qMetas []*QueueMeta, err error) {
	var apiPath string
	apiPath, err = task.apiPath("queues/")
	if err != nil {
		return
	}
	req, err := http.NewRequestWithContext(
		task.waitStop.ctx,
		"POST",
		apiPath,
		nil,
	)
	if err != nil {
		err = APIError{"new request", err}
		return
	}

	resp, err := task.upstream.mgr.HTTPClient().Do(req)
	if err != nil {
		err = APIError{"response", err}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		err = APIError{fmt.Sprintf("http code %d", resp.StatusCode), nil}
		return
	}
	var body []byte
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		err = APIError{"read", err}
		return
	}

	result := sth.Result{}
	err = json.Unmarshal(body, &result)
	if err != nil {
		return
	}
	qMetas, err = task.queuesFromResult(result)
	return
}

func (task *UpdateQueuesTask) queuesFromResult(result sth.Result) (qMetas []*QueueMeta, err error) {
	qMetas = []*QueueMeta{}
	qs, ok := result["queues"]
	if !ok {
		err = fmt.Errorf(`"queues" not exist in %s`, result)
		return
	}
	ql := qs.([]interface{})
	num := 0
	new := 0
	for _, qt := range ql {
		num++
		qr := qt.(map[string]interface{})
		q, ok := qr["qid"]
		if !ok {
			err = fmt.Errorf(`"queues" not exist in %s`, q)
			break
		}
		s := q.(string)
		qid, err := queuebox.QueueIDFromString(s)
		if err != nil {
			break
		}
		var qsize int64 = 10
		qz, ok := qr["qsize"]
		if ok {
			qsize = int64(qz.(float64))
		}

		if !task.upstream.ExistQueue(qid) {
			qMetas = append(qMetas, NewQueueMeta(qid, qsize))
			new++
		}
	}
	if err == nil {
		log.Logger.Debugf(task.upstream.logFormat("parse queues num: %d new: %d", num, new))
	}
	return
}

func (task *UpdateQueuesTask) updateQueues() {
	upstream := task.upstream
	status := upstream.Status()
	if status == UpstreamPaused {
		log.Logger.Warningf(task.upstream.logFormat("paused"))
		return
	}
	qMetas, err := task.getQueueMetas()
	if err != nil {
		switch err.(type) {
		case APIError:
			if upstream.Status() != UpstreamUnavailable {
				_, _ = upstream.mgr.SetStatus(upstream.ID, UpstreamUnavailable)
			}
		}
		log.Logger.Error(task.upstream.logFormat("%s", err))
		return
	}
	if status == UpstreamUnavailable {
		_, _ = upstream.mgr.SetStatus(upstream.ID, UpstreamWorking)
	}
	if len(qMetas) <= 0 {
		log.Logger.Warning(task.upstream.logFormat("0 queues"))
		return
	}

	var result sth.Result
	result, err = upstream.mgr.UpdateQueues(upstream.ID, qMetas)
	var logf func(args ...interface{}) = log.Logger.Debug
	args := []interface{}{result}
	if err != nil {
		logf = log.Logger.Error
		args[0] = err
	}

	logf(task.upstream.logFormat("%v", args...))
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
			if StopUpstreamStatus(task.upstream.status) {
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
	upstream := task.upstream
	status := UpstreamStopped
	if upstream.Status() == UpstreamRemoving {
		log.Logger.Debug(task.upstream.logFormat("start clearing queues %d",
			upstream.queues.Size()))
		for {
			if upstream.queues.Size() <= 0 {
				break
			}
			toBeDeleted := []sth.QueueID{}
			iter := slicemap.NewBaseIter(upstream.queues.Map)
			iter.Iter(
				func(item slicemap.Item) bool {
					queue := item.(*Queue)
					toBeDeleted = append(toBeDeleted, queue.ID)
					return len(toBeDeleted) < 100
				},
			)
			_, _ = upstream.mgr.DeleteQueues(upstream.ID, toBeDeleted, nil)
		}
		status = UpstreamRemoved
		log.Logger.Debug(task.upstream.logFormat("clear finished"))
	}
	result, err := upstream.mgr.SetStatus(upstream.ID, status)
	log.Logger.Info(task.upstream.logFormat("stop %v %v", result, err))
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
	}
	task.waitStop.Stop()
}
