package upstream

import (
	"container/heap"
	"sync"
)

// QueueHeap TODO
type queueHeap []*Queue

// Len TODO
func (qh queueHeap) Len() int {
	return len(qh)
}

// Swap TODO
func (qh queueHeap) Swap(i, j int) {
	qh[i], qh[j] = qh[j], qh[i]
	qh[i].heapIdx = i
	qh[j].heapIdx = j
}

// Less TODO
func (qh queueHeap) Less(i, j int) bool {
	return qh[i].updateTime.Sub(qh[j].updateTime) < 0
}

// Push TODO
func (qh *queueHeap) Push(x interface{}) {
	n := len(*qh)
	q := x.(*Queue)
	q.heapIdx = n
	(*qh) = append((*qh), q)
}

// Pop TODO
func (qh *queueHeap) Pop() interface{} {
	old := *qh
	n := len(old)
	queue := old[n-1]
	queue.heapIdx = -1
	*qh = old[0 : n-1]
	return queue
}

// Pop TODO
func (qh *queueHeap) Top() *Queue {
	return (*qh)[len(*qh)-1]
}

// QueueHeap TODO
type QueueHeap struct {
	heap queueHeap
	*sync.RWMutex
}

// Size TODO
func (qh *QueueHeap) Size() int {
	qh.RLock()
	defer qh.RUnlock()
	return len(qh.heap)
}

// Push TODO
func (qh *QueueHeap) Push(queue *Queue) {
	qh.Lock()
	defer qh.Unlock()
	heap.Push(&qh.heap, queue)
}

// Pop TODO
func (qh *QueueHeap) Pop() *Queue {
	qh.Lock()
	defer qh.Unlock()
	q := heap.Pop(&qh.heap)
	return q.(*Queue)
}

// Top TODO
func (qh *QueueHeap) Top() *Queue {
	qh.RLock()
	defer qh.RUnlock()
	if len(qh.heap) <= 0 {
		return nil
	}
	return qh.heap.Top()
}

// Delete TODO
func (qh *QueueHeap) Delete(queue *Queue) *Queue {
	qh.Lock()
	defer qh.Unlock()
	return heap.Remove(&qh.heap, queue.heapIdx).(*Queue)
}

// Update TODO
func (qh *QueueHeap) Update(queue *Queue) {
	qh.Lock()
	defer qh.Unlock()
	heap.Fix(&qh.heap, queue.heapIdx)
}

// NewQueueHeap TODO
func NewQueueHeap() *QueueHeap {
	return &QueueHeap{make(queueHeap, 0), &sync.RWMutex{}}
}
