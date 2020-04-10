package upstream

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/cfhamlet/os-rq-pod/pkg/json"
	"github.com/cfhamlet/os-rq-pod/pkg/request"
	"github.com/cfhamlet/os-rq-pod/pkg/sth"
	"github.com/cfhamlet/os-rq-pod/pod/global"
)

// QueueMeta TODO
type QueueMeta struct {
	ID    sth.QueueID
	qsize int64
}

// Queue TODO
type Queue struct {
	upstream *Upstream
	*QueueMeta
	dequeuing   int64
	apiEndpoint *url.URL
	updateTime  time.Time
}

// NewQueueMeta TODO
func NewQueueMeta(qid sth.QueueID, qsize int64) *QueueMeta {
	return &QueueMeta{qid, qsize}
}

// NewQueue TODO
func NewQueue(upstream *Upstream, meta *QueueMeta) *Queue {
	endpoint, err := url.Parse(fmt.Sprintf("queue/pop/?q=%s", meta.ID))
	if err != nil {
		panic(err)
	}
	return &Queue{upstream, meta, 0, endpoint, time.Now()}
}

// ItemID TODO
func (queue *Queue) ItemID() uint64 {
	return queue.ID.ItemID()
}

func (queue *Queue) apiPath() string {
	return queue.upstream.ParsedAPI.ResolveReference(queue.apiEndpoint).String()
}

func (queue *Queue) incrDequeuing(n int64) int64 {
	return atomic.AddInt64(&(queue.dequeuing), n)
}

func (queue *Queue) decrDequeuing(n int64) int64 {
	return atomic.AddInt64(&(queue.dequeuing), 0-n)
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
	r, err := http.NewRequest(
		"POST",
		queue.apiPath(),
		nil,
	)
	if err != nil {
		err = APIError{"new request", err}
		return
	}
	resp, err := queue.upstream.client.Do(r)
	if err != nil {
		err = APIError{"response", err}
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		var body []byte
		body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			err = APIError{"read", err}
			return
		}
		req = &request.Request{}
		err = json.Unmarshal(body, &req)
	} else if resp.StatusCode == 404 {
		err = global.NotExistError(queue.ID.String())
	} else {
		err = global.UnavailableError(queue.ID.String())
	}
	return
}

// Idle TODO
func (queue *Queue) Idle() bool {
	return queue.QueueSize() <= 0 ||
		queue.upstream.status != UpstreamWorking
}

// Pop TODO
func (queue *Queue) Pop() (req *request.Request, qsize int64, err error) {
	dequeuing := queue.incrDequeuing(1)
	defer queue.decrDequeuing(1)
	qsize = queue.QueueSize()

	if dequeuing > qsize || dequeuing > 1984 {
		msg := fmt.Sprintf("%s qsize %d, dequeuing %d", queue.ID, qsize, dequeuing)
		err = global.UnavailableError(msg)
		return
	}

	req, err = queue.getRequest()
	if err == nil {
		qsize = queue.updateOutput(1)
	}
	return
}
