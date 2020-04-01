package hub

import (
	"math/rand"
	"time"

	"github.com/cfhamlet/os-rq-pod/pkg/slicemap"
	"github.com/cfhamlet/os-rq-pod/pkg/sth"
)

// QueuesSelector TODO
type QueuesSelector interface {
	Select() []sth.Result
}

// CycleCountIter TODO
type CycleCountIter struct {
	cycle *slicemap.CycleStepIter
	count int
}

// NewCycleCountIter TODO
func NewCycleCountIter(m *slicemap.Map, start, steps int) *CycleCountIter {
	return &CycleCountIter{slicemap.NewCycleStepIter(m, start, steps), 0}
}

// Iter TODO
func (iter *CycleCountIter) Iter(f slicemap.IterFunc) {
	iter.cycle.Iter(
		func(item slicemap.Item) bool {
			r := f(item)
			iter.count++
			return r
		},
	)
}

// RandSelector TODO
type RandSelector struct {
	mgr       *UpstreamManager
	r         *rand.Rand
	k         int
	selected  map[sth.QueueID]sth.Result
	iterators map[UpstreamID]*CycleCountIter
	out       []sth.Result
}

// NewRandSelector TODO
func NewRandSelector(mgr *UpstreamManager, k int) *RandSelector {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &RandSelector{
		mgr,
		r,
		k,
		map[sth.QueueID]sth.Result{},
		map[UpstreamID]*CycleCountIter{},
		make([]sth.Result, 0, k),
	}
}

// Select TODO
func (selector *RandSelector) Select() []sth.Result {
	upstreams := selector.mgr.statusUpstreams[UpstreamWorking]
	l := upstreams.Size()
	choices := make([]*Upstream, 0)

	start := selector.r.Intn(l)
	iterator := slicemap.NewCycleIter(upstreams.Map, start)

	iterator.Iter(
		func(item slicemap.Item) bool {
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
				return false
			}
			return true
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
		iterator = NewCycleCountIter(upstream.queues.Map, selector.r.Intn(l), n)
	} else {
		iterator.cycle.SetSteps(n)
	}

	iterator.Iter(
		func(item slicemap.Item) bool {
			queue := item.(*Queue)
			result, ok := selector.selected[queue.ID]
			if !ok {
				result = sth.Result{"qid": queue.ID, "qsize": queue.qsize}
				selector.selected[queue.ID] = result
				selector.out = append(selector.out, result)
				selector.k--
			} else {
				result["qsize"] = queue.qsize + result["qsize"].(int64)
			}
			if iterator.count+1 >= l {
				return false
			}
			return true
		},
	)

	if iterator.count >= l {
		delete(selector.iterators, upstream.ID)
		return true
	}
	selector.iterators[upstream.ID] = iterator
	return false
}
