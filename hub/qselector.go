package hub

import (
	"math/rand"
	"time"

	"github.com/cfhamlet/os-rq-pod/pkg/slicemap"
	"github.com/cfhamlet/os-rq-pod/pod"
)

// QueuesSelector TODO
type QueuesSelector interface {
	Select() []Result
}

// EmptySelector TODO
type EmptySelector struct {
}

// Select TODO
func (selector *EmptySelector) Select() []Result {
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

// Select TODO
func (selector *AllSelector) Select() []Result {
	mgr := selector.mgr
	out := make([]Result, 0, len(mgr.queueUpstreams))
	for qid, upstreams := range mgr.queueUpstreams {
		r := Result{"qid": qid}
		var qsize int64 = 0
		iter := slicemap.NewFastIter(upstreams)
		iter.Iter(
			func(item slicemap.Item) {
				upstream := item.(*Upstream)
				queue := upstream.queues.Get(qid.ItemID())
				if queue != nil {
					qsize += (queue.(*Queue)).qsize
				}
			},
		)
		r["qsize"] = qsize
		out = append(out, r)
	}
	return out
}

// CycleCountIter TODO
type CycleCountIter struct {
	cycle *slicemap.CycleIter
	count int
}

// NewCycleCountIter TODO
func NewCycleCountIter(m *slicemap.Map, start, steps int) *CycleCountIter {
	return &CycleCountIter{slicemap.NewCycleIter(m, start, steps), 0}
}

// Iter TODO
func (iter *CycleCountIter) Iter(f func(slicemap.Item)) {
	iter.cycle.Iter(
		func(item slicemap.Item) {
			f(item)
			iter.count++
		},
	)
}

// Break TODO
func (iter *CycleCountIter) Break() {
	iter.cycle.Break()
}

// RandSelector TODO
type RandSelector struct {
	mgr       *UpstreamManager
	r         *rand.Rand
	k         int
	selected  map[pod.QueueID]Result
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
		map[pod.QueueID]Result{},
		map[UpstreamID]*CycleCountIter{},
		make([]Result, 0, k),
	}
}

// Select TODO
func (selector *RandSelector) Select() []Result {
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
			upstream := item.(*Upstream)
			if !selector.choice(upstream, n) {
				choices = append(choices, upstream)
				l--
			}
			if selector.k <= 0 || l <= 0 {
				iterator.Break()
			}
		},
	)

	l = len(choices)
	for i := 0; selector.k > 0 && l > 0; i++ {
		i = i % l
		n := selector.k / l
		if n <= 0 {
			n = 1
		}
		stream := choices[i]
		if selector.choice(stream, n) {
			choices = append(choices[:i], choices[i+1:]...)
			i--
			l--
		}
	}
	return selector.out
}

func (selector *RandSelector) choice(upstream *Upstream, n int) bool {

	iterator, ok := selector.iterators[upstream.ID]
	l := upstream.queues.Size()
	if l <= 0 {
		return true
	}
	if !ok {
		iterator = NewCycleCountIter(upstream.queues, selector.r.Intn(l), n)
	} else {
		iterator.cycle.SetSteps(n)
	}

	iterator.Iter(
		func(item slicemap.Item) {
			queue := item.(*Queue)
			result, ok := selector.selected[queue.ID]
			if !ok {
				result = Result{"qid": queue.ID, "qsize": queue.qsize}
				selector.selected[queue.ID] = result
				selector.out = append(selector.out, result)
				selector.k--
			} else {
				result["qsize"] = queue.qsize + result["qsize"].(int64)
			}
			if iterator.count+1 >= l {
				iterator.Break()
			}
		},
	)

	if iterator.count >= l {
		delete(selector.iterators, upstream.ID)
		return true
	}
	selector.iterators[upstream.ID] = iterator
	return false
}
