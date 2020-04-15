package upstream_test

import (
	"fmt"
	"testing"

	"github.com/cfhamlet/os-rq-hub/hub/upstream"
	"github.com/cfhamlet/os-rq-pod/pkg/sth"
	"github.com/stretchr/testify/assert"
)

func Test001(t *testing.T) {
	hp := upstream.NewQueueHeap()
	s := []*upstream.Queue{}
	for i := 0; i < 10; i++ {
		queue := upstream.NewQueue(nil,
			upstream.NewQueueMeta(sth.QueueID{Host: fmt.Sprintf("%d", i), Port: "", Scheme: ""}, 0))
		hp.Push(queue)
		s = append(s, queue)
	}
	assert.Equal(t, 10, hp.Size())
	queue := hp.Pop()
	assert.Equal(t, s[0].ID.Host, queue.ID.Host)
	assert.Equal(t, 9, hp.Size())
}

func Test002(t *testing.T) {
	hp := upstream.NewQueueHeap()
	s := []*upstream.Queue{}
	for i := 0; i < 10; i++ {
		queue := upstream.NewQueue(nil,
			upstream.NewQueueMeta(sth.QueueID{Host: fmt.Sprintf("%d", i), Port: "", Scheme: ""}, 0))
		hp.Push(queue)
		s = append(s, queue)
	}
	assert.Equal(t, hp.Delete(s[3]).ID.Host, s[3].ID.Host)
}

func Test003(t *testing.T) {
	hp := upstream.NewQueueHeap()
	s := []*upstream.Queue{}
	for i := 0; i < 10; i++ {
		queue := upstream.NewQueue(nil,
			upstream.NewQueueMeta(sth.QueueID{Host: fmt.Sprintf("%d", i), Port: "", Scheme: ""}, 0))
		hp.Push(queue)
		s = append(s, queue)
	}
	assert.Equal(t, s[0], hp.Top())
}
