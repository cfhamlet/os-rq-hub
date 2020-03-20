package hub

// UpdateQueuesTask TODO
type UpdateQueuesTask struct {
	upstream *Upstream
}

// NewUpdateQueuesTask TODO
func NewUpdateQueuesTask(upstream *Upstream) *UpdateQueuesTask {
	return &UpdateQueuesTask{upstream}
}

func (task *UpdateQueuesTask) updateQueues() {

}

func (task *UpdateQueuesTask) run() {
}

// Start TODO
func (task *UpdateQueuesTask) Start() {
}

// Stop TODO
func (task *UpdateQueuesTask) Stop() {
}
