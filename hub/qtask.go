package hub

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
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
}

func (e APIError) Error() string {
	return e.reason
}

func (task *UpdateQueuesTask) getQueues() (queueIDs []pod.QueueID, err error) {
	upstream := task.upstream
	req, err := http.NewRequestWithContext(
		task.waitStop.ctx,
		"POST",
		upstream.API,
		nil,
	)
	if err != nil {
		err = APIError{fmt.Sprintf("new request %s", err)}
		return
	}

	resp, err := task.client.Do(req)
	if err != nil {
		err = APIError{fmt.Sprintf("response %s", err)}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		err = APIError{fmt.Sprintf("http code %s", err)}
		return
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		err = APIError{fmt.Sprintf("read %s", err)}
		return
	}

	result := Result{}
	err = json.Unmarshal(body, &result)
	if err != nil {
		return
	}
	queueIDs, err = queueIDsFromResult(result)
	return
}

func queueIDsFromResult(result Result) (queueIDs []pod.QueueID, err error) {
	queueIDs = []pod.QueueID{}
	qs, ok := result["queues"]
	if !ok {
		err = NotExistError(fmt.Sprintf(`"queues" not exist in %s`, result))
		return
	}
	ql := qs.([]interface{})
	for _, qt := range ql {
		qr := qt.(map[string]interface{})
		q, ok := qr["qid"]
		if !ok {
			err = NotExistError(fmt.Sprintf(`"queues" not exist in %s`, q))
			break
		}
		s := q.(string)
		qid, err := pod.QueueIDFromString(s)
		if err != nil {
			break
		}
		queueIDs = append(queueIDs, qid)
	}
	return
}

func (task *UpdateQueuesTask) updateQueues() {
	upstream := task.upstream
	status := upstream.Status()
	if status == UpstreamPaused {
		log.Logger.Warningf("<upstream %s> paused", upstream.ID)
		return
	}
	queueIDs, err := task.getQueues()
	if err != nil {
		switch err.(type) {
		case APIError:
			_, _ = upstream.mgr.SetStatus(upstream.ID, UpstreamUnavailable)
		}
		log.Logger.Errorf("<upstream %s> %s", upstream.ID, err)
		return
	} else if len(queueIDs) <= 0 {
		log.Logger.Warningf("<upstream %s> zero queues %s", upstream.ID)
		return
	}
	fmt.Println(queueIDs, err)
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
	log.Logger.Infof("<upstream %s> %s %v %v", task.upstream.ID, opt, result, err)
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
