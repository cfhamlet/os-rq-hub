package hub

import (
	"math/rand"
	"sync"
	"time"

	"github.com/cfhamlet/os-rq-pod/pod"
)

// UpstreamID TODO
type UpstreamID struct {
}

// UpstreamStatus TODO
type UpstreamStatus string

// Status enum
const (
	UpstreamPreparing   UpstreamStatus = "preparing"
	UpstreamWorking     UpstreamStatus = "working"
	UpstreamPaused      UpstreamStatus = "paused"
	UpstreamUnavailable UpstreamStatus = "unavailable"
)

// UpstreamStatusList TODO
var UpstreamStatusList = []UpstreamStatus{
	UpstreamPreparing,
	UpstreamWorking,
	UpstreamPaused,
	UpstreamUnavailable,
}

// Upstream TODO
type Upstream struct {
	ID          UpstreamID
	hub         *Hub
	api         string
	status      UpstreamStatus
	queueIdxMap map[pod.QueueID]int
	queueIDs    []pod.QueueID
	locker      *sync.RWMutex
}

// IdxUpstream TODO
type IdxUpstream struct {
	Idx int
	*Upstream
}

// NewUpstream TODO
func NewUpstream(hub *Hub, api string) *Upstream {
	return &Upstream{
		UpstreamID{},
		hub,
		api,
		UpstreamPreparing,
		map[pod.QueueID]int{},
		[]pod.QueueID{},
		&sync.RWMutex{},
	}
}

// QueuesSelector TODO
type QueuesSelector interface {
	Queues(int) []Result
}

// EmptySelector TODO
type EmptySelector struct {
}

// Queues TODO
func (selector *EmptySelector) Queues(k int) []Result {
	return []Result{}
}

var emptySelector = &EmptySelector{}

// UpstreamSelector TODO
type UpstreamSelector struct {
	stream *Upstream
}

// NewUpstreamSelector TODO
func NewUpstreamSelector(stream *Upstream) *UpstreamSelector {
	return &UpstreamSelector{stream}
}

// Queues TODO
func (selector *UpstreamSelector) Queues(k int) []Result {
	queueIDs := selector.stream.queueIDs
	l := len(queueIDs)
	var out []Result
	if k > l {
		k = l
	}
	out = make([]Result, 0, k)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	s := r.Perm(l)
	for _, i := range s {
		qid := queueIDs[i]
		out = append(out, Result{"qid": qid})
		if len(out) >= k {
			break
		}
	}
	return out
}

// AllSelector TODO
type AllSelector struct {
	mgr *UpstreamManager
}

// NewAllSelector TODO
func NewAllSelector(mgr *UpstreamManager) *AllSelector {
	return &AllSelector{mgr}
}

// Queues TODO
func (selector *AllSelector) Queues(k int) []Result {
	mgr := selector.mgr
	out := make([]Result, 0, len(mgr.queueBox.queueUpstreamsMap))
	for qid := range mgr.queueBox.queueUpstreamsMap {
		r := Result{"qid": qid}
		out = append(out, r)
	}
	return out
}

// RandSelectUnit TODO
type RandSelectUnit struct {
	total int
	start int
	cur   int
	count int
}

// RandSelector TODO
type RandSelector struct {
	mgr         *UpstreamManager
	r           *rand.Rand
	selected    map[pod.QueueID]bool
	selectUnits map[UpstreamID]*RandSelectUnit
}

// NewRandSelector TODO
func NewRandSelector(mgr *UpstreamManager) *RandSelector {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &RandSelector{
		mgr,
		r,
		map[pod.QueueID]bool{},
		map[UpstreamID]*RandSelectUnit{},
	}
}

// Queues TODO
func (selector *RandSelector) Queues(k int) []Result {
	upstreams := selector.mgr.statusUpstreamsMap[UpstreamWorking]
	l := len(upstreams)
	choices := make([]*Upstream, l)
	copy(upstreams, choices)

	i := selector.r.Intn(l)
	out := make([]Result, 0, k)
	for ; len(out) < k && len(choices) > 0; i++ {
		i = i % l
		n := (k - len(out)) / l
		if n <= 0 {
			n = 1
		}
		stream := choices[i]

		if selector.fill(stream, &out, n) {
			choices = append(choices[:i], choices[i+1:]...)
			i--
		}

	}
	return out
}

func (selector *RandSelector) fill(stream *Upstream, out *[]Result, n int) bool {
	unit, ok := selector.selectUnits[stream.ID]
	l := len(stream.queueIDs)
	if !ok {
		s := selector.r.Intn(l)
		unit = &RandSelectUnit{l, s, s, 0}
		selector.selectUnits[stream.ID] = unit
	}
	for i := 0; i < n && unit.count < unit.total; i++ {
		qid := stream.queueIDs[unit.cur]
		_, ok := selector.selected[qid]
		if !ok {
			selector.selected[qid] = true
			*out = append(*out, Result{"qid": qid})
		}

		unit.cur++
		if unit.cur >= unit.total {
			unit.cur = 0
		}
		unit.count++
	}

	if unit.count >= unit.total {
		return true
	}

	return false
}

// StatusUpstreamsMap TODO
type StatusUpstreamsMap map[UpstreamStatus][]*Upstream

// IdxUpstreamMap TODO
type IdxUpstreamMap map[UpstreamID]*IdxUpstream

// UpstreamManager TODO
type UpstreamManager struct {
	hub                *Hub
	idxUpstreamMap     IdxUpstreamMap
	statusUpstreamsMap StatusUpstreamsMap
	queueBox           *QueueBox
	locker             *sync.RWMutex
}

// NewUpstreamManager TODO
func NewUpstreamManager(hub *Hub) *UpstreamManager {
	statusUpstreamsMap := StatusUpstreamsMap{}
	for _, status := range UpstreamStatusList {
		statusUpstreamsMap[status] = []*Upstream{}
	}
	return &UpstreamManager{
		hub,
		IdxUpstreamMap{},
		statusUpstreamsMap,
		NewQueueBox(hub),
		&sync.RWMutex{},
	}
}

// AddUpstream TODO
func (mgr *UpstreamManager) AddUpstream() error {
	return nil
}

func (mgr *UpstreamManager) setStatus() error {
	return nil
}

// Queues TODO
func (mgr *UpstreamManager) Queues(k int) (result Result) {
	mgr.locker.RLock()
	defer mgr.locker.RUnlock()

	upstreams := mgr.statusUpstreamsMap[UpstreamWorking]
	l := len(upstreams)
	total := len(mgr.queueBox.queueUpstreamsMap)
	var selector QueuesSelector
	if l <= 0 {
		selector = emptySelector
	} else if total < k {
		selector = NewAllSelector(mgr)
	} else if l == 1 {
		selector = NewUpstreamSelector(upstreams[0])
	} else {
		selector = NewRandSelector(mgr)
	}
	out := selector.Queues(k)
	return Result{
		"k":         k,
		"queues":    out,
		"total":     total,
		"upstreams": l,
	}
}
