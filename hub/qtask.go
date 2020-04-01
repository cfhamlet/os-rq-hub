package hub

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
		qid, err := pod.QueueIDFromString(s)
		if err != nil {
			break
		}
		var qsize int64 = 10
		qz, ok := qr["qsize"]
		if ok {
			qsize = int64(qz.(float64))
		}

		if !task.upstream.ExistQueueID(qid) {
			qMetas = append(qMetas, NewQueueMeta(qid, qsize))
			new++
		}
	}
	if err == nil {
		log.Logger.Debugf(task.logFormat("parse queues num: %d new: %d", num, new))
	}
	return
}

func (task *UpdateQueuesTask) logFormat(format string, args ...interface{}) string {
	msg := fmt.Sprintf(format, args...)
	return fmt.Sprintf("<upstream %s> %s", task.upstream.ID, msg)
}

func (task *UpdateQueuesTask) updateQueues() {
	upstream := task.upstream
	status := upstream.Status()
	if status == UpstreamPaused {
		log.Logger.Warningf(task.logFormat("paused"))
		return
	}
	queues, err := task.getQueueMetas()
	if err != nil {
		switch err.(type) {
		case APIError:
			_, _ = upstream.mgr.SetStatus(upstream.ID, UpstreamUnavailable)
		}
		log.Logger.Error(task.logFormat("%s", err))
		return
	}
	if status == UpstreamUnavailable {
		_, _ = upstream.mgr.SetStatus(upstream.ID, UpstreamWorking)
	}
	if len(queues) <= 0 {
		log.Logger.Warning(task.logFormat("0 queues"))
		return
	}

	var result sth.Result
	result, err = upstream.mgr.UpdateUpStreamQueues(upstream.ID, queues)
	if err != nil {
		log.Logger.Error(task.logFormat("%v", err))
	} else {
		log.Logger.Debug(task.logFormat("%v", result))
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
	opt := "stop"
	status := UpstreamStopped
	if upstream.Status() == UpstreamRemoving {
		log.Logger.Debug(task.logFormat("start clearing queues %d",
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
					if len(toBeDeleted) >= 100 {
						return false
					}
					return true
				},
			)
			_, _ = upstream.mgr.DeleteQueues(upstream.ID, toBeDeleted)
		}
		status = UpstreamRemoved
		log.Logger.Debug(task.logFormat("clear finished"))
	}
	result, err := upstream.mgr.SetStatus(upstream.ID, status)
	log.Logger.Info(task.logFormat("%s %v %v", opt, result, err))
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
