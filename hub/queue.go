package hub

import "github.com/cfhamlet/os-rq-pod/pod"

// Queue TODO
type Queue struct {
	hub *Hub
	ID  pod.QueueID
}
