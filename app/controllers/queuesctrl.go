package controllers

import (
	"fmt"
	"strconv"

	"github.com/cfhamlet/os-rq-hub/hub/upstream"
	"github.com/cfhamlet/os-rq-pod/app/controllers"
	"github.com/cfhamlet/os-rq-pod/pkg/sth"
	"github.com/gin-gonic/gin"
)

// QueuesController TODO
type QueuesController struct {
	upstreamMgr *upstream.Manager
}

// NewQueuesController TODO
func NewQueuesController(upstreamMgr *upstream.Manager) *QueuesController {
	return &QueuesController{upstreamMgr}
}

// Queues TODO
func (ctrl *QueuesController) Queues(c *gin.Context) (result sth.Result, err error) {
	qk := c.DefaultQuery("k", "10")
	k, e := strconv.ParseInt(qk, 10, 64)
	if e != nil {
		err = controllers.InvalidQuery(fmt.Sprintf("k=%s %s", qk, err))
	} else if k <= 0 || k > 1000 {
		err = controllers.InvalidQuery(fmt.Sprintf("k=%s [1, 1000]", qk))
	}
	if err == nil {
		result, err = ctrl.upstreamMgr.Queues(int(k))
	}
	return
}
