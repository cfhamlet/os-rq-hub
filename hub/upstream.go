package hub

import (
	"math/rand"
	"sync"
	"time"

	"github.com/cfhamlet/os-rq-pod/pkg/slicemap"
	"github.com/cfhamlet/os-rq-pod/pod"
	"github.com/segmentio/fasthash/fnv1a"
)

// UpstreamID TODO
type UpstreamID string

// ItemID TODO
func (uid UpstreamID) ItemID() uint64 {
	return fnv1a.HashString64(string(uid))
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

// UpstreamMeta TODO
type UpstreamMeta struct {
	ID  UpstreamID
	api string
}

// Upstream TODO
type Upstream struct {
	*UpstreamMeta
	hub      *Hub
	status   UpstreamStatus
	queueIDs *slicemap.Map
	locker   *sync.RWMutex
}

// NewUpstream TODO
func NewUpstream(hub *Hub, meta *UpstreamMeta) *Upstream {
	return &Upstream{
		meta,
		hub,
		UpstreamPreparing,
		slicemap.New(),
		&sync.RWMutex{},
	}
}

// ItemID TODO
func (stream *Upstream) ItemID() uint64 {
	return stream.ID.ItemID()
}

// QueuesSelector TODO
type QueuesSelector interface {
	Queues() []Result
}

// EmptySelector TODO
type EmptySelector struct {
}

// Queues TODO
func (selector *EmptySelector) Queues() []Result {
	return []Result{}
}

var emptySelector = &EmptySelector{}

// AllSelector TODO
type AllSelector struct {
	mgr *UpstreamManager
}

// NewAllSelector TODO
func NewAllSelector(mgr *UpstreamManager) *AllSelector {
	return &AllSelector{mgr}
}

// Queues TODO
func (selector *AllSelector) Queues() []Result {
	mgr := selector.mgr
	out := make([]Result, 0, len(mgr.queueBox.queueUpstreamsMap))
	for qid := range mgr.queueBox.queueUpstreamsMap {
		r := Result{"qid": qid}
		out = append(out, r)
	}
	return out
}

// CycleCountIter TODO
type CycleCountIter struct {
	iter  *slicemap.CycleIter
	count int
}

// NewCycleCountIter TODO
func NewCycleCountIter(m *slicemap.Map, start, steps int) *CycleCountIter {
	return &CycleCountIter{slicemap.NewCycleIter(m, start, steps), 0}
}

// Iter TODO
func (iter *CycleCountIter) Iter(f func(slicemap.Item)) {
	iter.iter.Iter(
		func(item slicemap.Item) {
			f(item)
			iter.count++
		},
	)
}

// Break TODO
func (iter *CycleCountIter) Break() {
	iter.iter.Break()
}

// RandSelector TODO
type RandSelector struct {
	mgr       *UpstreamManager
	r         *rand.Rand
	k         int
	selected  map[pod.QueueID]bool
	iterators map[UpstreamID]*CycleCountIter
	out       []Result
}

// NewRandSelector TODO
func NewRandSelector(mgr *UpstreamManager, k int) *RandSelector {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &RandSelector{
		mgr,
		r,
		k,
		map[pod.QueueID]bool{},
		map[UpstreamID]*CycleCountIter{},
		make([]Result, 0, k),
	}
}

// Queues TODO
func (selector *RandSelector) Queues() []Result {
	upstreams := selector.mgr.statusUpstreams[UpstreamWorking]
	l := upstreams.Size()
	choices := make([]*Upstream, 0, l)

	start := selector.r.Intn(l)
	iterator := slicemap.NewCycleIter(upstreams, start, l)

	iterator.Iter(
		func(item slicemap.Item) {
			n := selector.k / l
			if n <= 0 {
				n = 1
			}
			stream := item.(*Upstream)
			if !selector.fill(stream, n) {
				choices = append(choices, stream)
				l--
			}
			if selector.k <= 0 {
				iterator.Break()
			}
		},
	)

	l = len(choices)
	for i := 0; selector.k > 0 && l > 0; i++ {
		n := selector.k / l
		if n <= 0 {
			n = 1
		}
		stream := choices[i]
		if selector.fill(stream, n) {
			choices = append(choices[:i], choices[i+1:]...)
			i--
			l--
		}

	}
	return selector.out
}

func (selector *RandSelector) fill(stream *Upstream, n int) bool {

	iterator, ok := selector.iterators[stream.ID]
	l := stream.queueIDs.Size()
	if !ok {
		iterator = NewCycleCountIter(stream.queueIDs, selector.r.Intn(l), l)
	}

	iterator.Iter(
		func(item slicemap.Item) {
			qid := item.(pod.QueueID)
			_, ok := selector.selected[qid]
			if !ok {
				selector.out = append(selector.out, Result{"qid": qid})
				selector.k--
			}
			n--
			if n <= 0 || iterator.count+1 >= l {
				iterator.Break()
			}
		},
	)

	if iterator.count >= l {
		return true
	}
	selector.iterators[stream.ID] = iterator
	return false
}

// UpstreamManager TODO
type UpstreamManager struct {
	hub             *Hub
	upstreamMap     map[UpstreamID]*Upstream
	statusUpstreams map[UpstreamStatus]*slicemap.Map
	queueBox        *QueueBox
	locker          *sync.RWMutex
}

// NewUpstreamManager TODO
func NewUpstreamManager(hub *Hub) *UpstreamManager {
	statusUpstreams := map[UpstreamStatus]*slicemap.Map{}
	for _, status := range UpstreamStatusList {
		statusUpstreams[status] = slicemap.New()
	}
	return &UpstreamManager{
		hub,
		map[UpstreamID]*Upstream{},
		statusUpstreams,
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

	upstreams := mgr.statusUpstreams[UpstreamWorking]
	l := upstreams.Size()
	total := len(mgr.queueBox.queueUpstreamsMap)
	var selector QueuesSelector
	if l <= 0 {
		selector = emptySelector
	} else if total <= k {
		selector = NewAllSelector(mgr)
	} else {
		selector = NewRandSelector(mgr, k)
	}
	out := selector.Queues()
	return Result{
		"k":         k,
		"queues":    out,
		"total":     total,
		"upstreams": l,
	}
}
