package hub

import (
	"sync/atomic"
	"time"

	"github.com/cfhamlet/os-rq-pod/pkg/request"
	"github.com/cfhamlet/os-rq-pod/pkg/slicemap"
	"github.com/cfhamlet/os-rq-pod/pkg/sth"
	"github.com/cfhamlet/os-rq-pod/pod"
)

// QueueUpstreamsPack TODO
type QueueUpstreamsPack struct {
	sth.QueueID
	*slicemap.Map
}

// QueueBulk TODO
type QueueBulk struct {
	mgr  *UpstreamManager
	bulk []*slicemap.Viewer
	size int64
}

// NewQueueBulk TODO
func NewQueueBulk(mgr *UpstreamManager, bulkSize int) *QueueBulk {
	bulk := make([]*slicemap.Viewer, 0, bulkSize)
	for i := 0; i < bulkSize; i++ {
		bulk = append(bulk, slicemap.NewViewer(nil))
	}
	return &QueueBulk{mgr, bulk, 0}
}

// Size TODO
func (qb *QueueBulk) Size() int64 {
	return atomic.LoadInt64(&qb.size)
}

func (qb *QueueBulk) index(id uint64) int {
	return int(id>>32) % len(qb.bulk)
}

// GetOrAdd TODO
func (qb *QueueBulk) GetOrAdd(id uint64, f func(slicemap.Item) slicemap.Item) bool {
	r := qb.bulk[qb.index(id)].GetOrAdd(id, f)
	if r {
		atomic.AddInt64(&qb.size, 1)
	}
	return r
}

// GetAndDelete TODO
func (qb *QueueBulk) GetAndDelete(id uint64, f func(slicemap.Item) bool) bool {
	r := qb.bulk[qb.index(id)].GetAndDelete(id, f)
	if r {
		atomic.AddInt64(&qb.size, -1)
	}
	return r
}

// PopRequest TODO
func (qb *QueueBulk) PopRequest(qid sth.QueueID) (req *request.Request, err error) {
	iid := qid.ItemID()
	toBeDeleted := make([]UpstreamID, 0)
	qb.bulk[qb.index(iid)].View(iid,
		func(item slicemap.Item) {
			if item == nil {
				err = pod.NotExistError(qid.String())
				return
			}
			pack := item.(*QueueUpstreamsPack)
			l := pack.Size()
			iter := slicemap.NewCycleIter(pack.Map, qb.mgr.rand.Intn(l))
			iter.Iter(
				func(item slicemap.Item) bool {
					upstream := item.(*Upstream)
					var qsize int64
					req, qsize, err = upstream.PopRequest(qid)
					if err != nil || qsize <= 0 {
						toBeDeleted = append(toBeDeleted, upstream.ID)
						return true
					}
					return false
				},
			)
		},
	)
	if len(toBeDeleted) > 0 {
		go qb.mgr.DeleteOutdated(qid, toBeDeleted, time.Now())
	}
	return
}
