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
	out := make([]Result, 0, len(mgr.queueBox.queueUpstreams))
	for qid := range mgr.queueBox.queueUpstreams {
		r := Result{"qid": qid}
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
	l := upstream.queueIDs.Size()
	if !ok {
		iterator = NewCycleCountIter(upstream.queueIDs, selector.r.Intn(l), n)
	} else {
		iterator.cycle.SetSteps(n)
	}

	iterator.Iter(
		func(item slicemap.Item) {
			qid := item.(pod.QueueID)
			_, ok := selector.selected[qid]
			if !ok {
				selector.out = append(selector.out, Result{"qid": qid})
				selector.k--
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
