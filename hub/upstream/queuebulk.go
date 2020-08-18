package upstream

import (
	"sync/atomic"
	"time"

	"github.com/cfhamlet/os-rq-pod/pkg/request"
	"github.com/cfhamlet/os-rq-pod/pkg/slicemap"
	"github.com/cfhamlet/os-rq-pod/pkg/sth"
	"github.com/cfhamlet/os-rq-pod/pod/global"
)

// Pack TODO
type Pack struct {
	sth.QueueID
	*slicemap.Viewer
}

// NewPack TODO
func NewPack(qid sth.QueueID) *Pack {
	return &Pack{qid, slicemap.NewViewer(nil)}
}

// QueueBulk TODO
type QueueBulk struct {
	mgr   *Manager
	bulks []*slicemap.Viewer
	size  int64
}

// NewQueueBulk TODO
func NewQueueBulk(mgr *Manager, bulkSize int) *QueueBulk {
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

func (qb *QueueBulk) bulk(id uint64) *slicemap.Viewer {
	return qb.bulks[qb.index(id)]
}

func (qb *QueueBulk) index(id uint64) int {
	return int(id>>32) % len(qb.bulks)
}

// GetOrAdd TODO
func (qb *QueueBulk) GetOrAdd(id uint64, f func(slicemap.Item) slicemap.Item) bool {
	r := qb.bulk(id).GetOrAdd(id, f)
	if r {
		atomic.AddInt64(&qb.size, 1)
	}
	return r
}

// Exist TODO
func (qb *QueueBulk) Exist(qid sth.QueueID) bool {
	id := qid.ItemID()
	return qb.bulk(id).Get(id) != nil
}

// GetAndDelete TODO
func (qb *QueueBulk) GetAndDelete(id uint64, f func(slicemap.Item) bool) bool {
	r := qb.bulk(id).GetAndDelete(id, f)
	if r {
		atomic.AddInt64(&qb.size, -1)
	}
	return r
}

// UpdateUpstream TODO
func (qb *QueueBulk) UpdateUpstream(upstream *Upstream, qMeta *UpdateQueueMeta) (int, int) {
	uNew := 0
	gNew := 0
	iid := qMeta.ID.ItemID()
	qb.GetOrAdd(iid,
		func(item slicemap.Item) slicemap.Item {
			var pack *Pack
			if item == nil {
				pack = NewPack(qMeta.ID)
				gNew = 1
			} else {
				pack = item.(*Pack)
			}
			pack.GetOrAdd(upstream.ItemID(),
				func(uitem slicemap.Item) slicemap.Item {
					if uitem == nil {
						uNew = 1
					}
					upstream.UpdateQueue(qMeta)
					return upstream
				})
			return pack
		},
	)
	return uNew, gNew
}

// ClearUpstream TODO
func (qb *QueueBulk) ClearUpstream(id ID, queueIDs []sth.QueueID, ts *time.Time) sth.Result {
	uDel := 0
	gDel := 0
	for _, qid := range queueIDs {
		u, g := qb.clearQueue(qid, id, ts)
		uDel += u
		gDel += g
	}
	return sth.Result{"qids": queueIDs, "uid": id, "del": uDel, "del_global": gDel}
}

// ClearQueue TODO
func (qb *QueueBulk) ClearQueue(qid sth.QueueID, ids []ID, ts *time.Time) sth.Result {
	uDel := 0
	gDel := 0
	for _, id := range ids {
		u, g := qb.clearQueue(qid, id, ts)
		uDel += u
		gDel += g
	}
	return sth.Result{"qid": qid, "uids": ids, "del": uDel, "del_global": gDel}
}

func (qb *QueueBulk) clearQueue(qid sth.QueueID, id ID, ts *time.Time) (int, int) {
	uDel := 0
	gDel := 0
	iid := qid.ItemID()
	qb.GetAndDelete(iid,
		func(item slicemap.Item) bool {
			pack := item.(*Pack)
			if pack.Size() <= 0 {
				gDel++
				return true
			}
			uid := id.ItemID()
			uitem := pack.Get(uid)
			if uitem != nil {
				upstream := uitem.(*Upstream)
				if upstream.DeleteQueue(qid, ts) {
					pack.Delete(uid)
					uDel++
				}
			}
			if pack.Size() <= 0 {
				gDel++
				return true
			}
			return false
		},
	)
	return uDel, gDel
}

// DequeueRequest TODO
func (qb *QueueBulk) DequeueRequest(qid sth.QueueID) (req *request.Request, err error) {
	iid := qid.ItemID()
	toBeDeleted := make([]ID, 0)
	qb.bulk(iid).View(iid,
		func(item slicemap.Item) {
			if item == nil {
				err = global.NotExistError(qid.String())
				return
			}
			pack := item.(*Pack)
			l := pack.Size()
			iter := slicemap.NewCycleIter(pack.Map, qb.mgr.rand.Intn(l))
			iter.Iter(
				func(item slicemap.Item) bool {
					upstream := item.(*Upstream)
					var qsize int64
					req, qsize, err = upstream.DequeueRequest(qid)
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
		ts := time.Now()
		go qb.ClearQueue(qid, toBeDeleted, &ts)
	}
	return
}
