package hub

import "github.com/cfhamlet/os-rq-pod/pod"

// Queue TODO
type Queue struct {
	ID    pod.QueueID
	qsize int64
}

// NewQueue TODO
func NewQueue(qid pod.QueueID, qsize int64) *Queue {
	return &Queue{qid, qsize}
}

// ItemID TODO
func (queue *Queue) ItemID() uint64 {
	return queue.ID.ItemID()
}
