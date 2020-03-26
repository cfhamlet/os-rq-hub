package hub

import (
	"fmt"
	"net/url"
	"sync/atomic"

	"github.com/cfhamlet/os-rq-pod/pkg/log"
	"github.com/cfhamlet/os-rq-pod/pkg/request"
	"github.com/cfhamlet/os-rq-pod/pod"
)

// QueueMeta TODO
type QueueMeta struct {
	ID    pod.QueueID
	qsize int64
}

// Queue TODO
type Queue struct {
	upstream *Upstream
	*QueueMeta
	dequeuing   int64
	apiEndpoint *url.URL
}

// NewQueueMeta TODO
func NewQueueMeta(qid pod.QueueID, qsize int64) *QueueMeta {
	return &QueueMeta{qid, qsize}
}

func (queue *Queue) apiPath() string {
	return queue.upstream.parsedAPI.ResolveReference(queue.apiEndpoint).String()
}

// NewQueue TODO
func NewQueue(upstream *Upstream, meta *QueueMeta) *Queue {
	endpoint, err := url.Parse(fmt.Sprintf("queue/?k%s", meta.ID))
	if err != nil {
		panic(err)
	}
	return &Queue{upstream, meta, 0, endpoint}
}

// ItemID TODO
func (queue *Queue) ItemID() uint64 {
	return queue.ID.ItemID()
}
func (queue *Queue) incrDequeuing(n int64) int64 {
	return atomic.AddInt64(&(queue.dequeuing), n)
}
func (queue *Queue) decrDequeuing(n int64) int64 {
	return atomic.AddInt64(&(queue.dequeuing), 0-n)
}

func (queue *Queue) incr(n int64) int64 {
	return atomic.AddInt64(&(queue.qsize), n)
}

func (queue *Queue) decr(n int64) int64 {
	return atomic.AddInt64(&(queue.qsize), 0-n)
}

func (queue *Queue) updateOutput(n int64) int64 {
	return queue.decr(n)
}

// QueueSize TODO
func (queue *Queue) QueueSize() int64 {
	return atomic.LoadInt64(&queue.qsize)
}

func (queue *Queue) getRequest() (req *request.Request, err error) {
	return
}

// Idle TODO
func (queue *Queue) Idle() bool {
	return queue.QueueSize() <= 0 ||
		queue.upstream.status != UpstreamWorking
}

// Get TODO
func (queue *Queue) Get() (req *request.Request, qsize int64, err error) {
	dequeuing := queue.incrDequeuing(1)
	defer queue.decrDequeuing(1)
	qsize = queue.QueueSize()

	status := queue.upstream.status
	if status != UpstreamWorking {
		err = UnavailableError(fmt.Sprintf("%s %s", queue.upstream.ID, status))
		return
	}

	if dequeuing > qsize || dequeuing > 198405 {
		msg := fmt.Sprintf("<upstream %s> %s qsize %d, dequeuing %d",
			queue.upstream.ID, queue.ID, qsize, dequeuing)
		log.Logger.Debug(msg)
		err = UnavailableError(msg)
		return
	}

	req, err = queue.getRequest()
	if err == nil {
		qsize = queue.updateOutput(1)
	}
	return
}
