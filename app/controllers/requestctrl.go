package controllers

import (
	"net/http"

	"github.com/cfhamlet/os-rq-hub/hub/upstream"
	"github.com/cfhamlet/os-rq-pod/app/controllers"
	"github.com/cfhamlet/os-rq-pod/pkg/request"
	"github.com/cfhamlet/os-rq-pod/pkg/sth"
	"github.com/gin-gonic/gin"
)

// RequestController TODO
type RequestController struct {
	upstreamMgr *upstream.Manager
}

// NewRequestController TODO
func NewRequestController(upstreamMgr *upstream.Manager) *RequestController {
	return &RequestController{upstreamMgr}
}

// PopRequest TODO
func (ctrl *RequestController) PopRequest(c *gin.Context) (result sth.Result, err error) {
	q := c.Query("q")

	qid, err := controllers.QueueIDFromQuery(q)
	if err == nil {
		var req *request.Request
		req, err = ctrl.upstreamMgr.PopRequest(qid)
		if err == nil {
			c.JSON(http.StatusOK, req)
		}
	}
	return
}
