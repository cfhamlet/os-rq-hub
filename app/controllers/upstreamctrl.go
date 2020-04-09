package controllers

import (
	"fmt"

	"github.com/cfhamlet/os-rq-hub/hub/upstream"
	"github.com/cfhamlet/os-rq-pod/app/controllers"
	"github.com/cfhamlet/os-rq-pod/pkg/sth"
	"github.com/cfhamlet/os-rq-pod/pkg/utils"
	"github.com/gin-gonic/gin"
)

// UpstreamController TODO
type UpstreamController struct {
	upstreamMgr *upstream.Manager
}

// NewUpstreamController TODO
func NewUpstreamController(upstreamMgr *upstream.Manager) *UpstreamController {
	return &UpstreamController{upstreamMgr}
}

// AddUpstream TODO
func (ctrl *UpstreamController) AddUpstream(c *gin.Context) (result sth.Result, err error) {
	var storeMeta *upstream.StoreMeta = upstream.NewStoreMeta(nil)

	if err = c.ShouldBindJSON(storeMeta); err != nil {
		err = controllers.InvalidBody(fmt.Sprintf("%s", err))
	} else {
		storeMeta.Status = upstream.UpstreamInit
		result, err = ctrl.upstreamMgr.AddUpstream(storeMeta)
	}

	c.Header("Access-Control-Allow-Origin", "*")
	return
}

func operateUpstreamByQuery(c *gin.Context, f CallByUpstreamID) (result sth.Result, err error) {
	id := c.Query("id")

	if id == "" {
		err = controllers.InvalidQuery("invalid id")
	} else {
		result, err = f(upstream.ID(id))
	}
	if result == nil {
		result = sth.Result{"id": id}
	}
	return
}

// PauseUpstream TODO
func (ctrl *UpstreamController) PauseUpstream(c *gin.Context) (result sth.Result, err error) {
	return operateUpstreamByQuery(c, ctrl.upstreamMgr.PauseUpstream)
}

// ResumeUpstream TODO
func (ctrl *UpstreamController) ResumeUpstream(c *gin.Context) (result sth.Result, err error) {
	return operateUpstreamByQuery(c, ctrl.upstreamMgr.ResumeUpstream)
}

// DeleteUpstream TODO
func (ctrl *UpstreamController) DeleteUpstream(c *gin.Context) (sth.Result, error) {
	return operateUpstreamByQuery(c, ctrl.upstreamMgr.DeleteUpstream)
}

// UpstreamInfo TODO
func (ctrl *UpstreamController) UpstreamInfo(c *gin.Context) (sth.Result, error) {
	return operateUpstreamByQuery(c, ctrl.upstreamMgr.UpstreamInfo)
}

// Upstreams TODO
func (ctrl *UpstreamController) Upstreams(c *gin.Context) (result sth.Result, err error) {
	qs := c.DefaultQuery("status", utils.Text(upstream.UpstreamWorking))
	status, ok := upstream.UpstreamStatusMap[qs]
	if !ok {
		err = controllers.InvalidQuery(fmt.Sprintf(`invalid status '%s'`, qs))
		return
	}
	return ctrl.upstreamMgr.Upstreams(status)
}

// UpstreamsInfo TODO
func (ctrl *UpstreamController) UpstreamsInfo(c *gin.Context) (sth.Result, error) {
	return ctrl.upstreamMgr.Info(), nil
}
