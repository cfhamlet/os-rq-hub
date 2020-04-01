package controller

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	core "github.com/cfhamlet/os-rq-hub/hub"
	"github.com/cfhamlet/os-rq-pod/app/controller"
	"github.com/cfhamlet/os-rq-pod/pkg/request"
	"github.com/cfhamlet/os-rq-pod/pkg/sth"
	"github.com/cfhamlet/os-rq-pod/pkg/utils"
	"github.com/gin-gonic/gin"
)

// Controller TODO
type Controller struct {
	*core.Core
}

// New TODO
func New(serv *core.Core) *Controller {
	return &Controller{serv}
}

// Resume TODO
func (ctrl *Controller) Resume(c *gin.Context) (sth.Result, error) {
	return ctrl.Switch(true)
}

// Pause TODO
func (ctrl *Controller) Pause(c *gin.Context) (sth.Result, error) {
	return ctrl.Switch(false)
}

// Info TODO
func (ctrl *Controller) Info(c *gin.Context) (sth.Result, error) {
	return ctrl.Core.Info()
}

// RedisInfo TODO
func (ctrl *Controller) RedisInfo(c *gin.Context) (sth.Result, error) {
	t := time.Now()
	s := c.DefaultQuery("section", "")
	var section []string
	if s != "" {
		section = strings.Split(s, ",")
	}
	info, err := ctrl.Core.RedisInfo(section...)
	return sth.Result{"info": info, "_cost_ms_": utils.SinceMS(t)}, err
}

// ProcessMemory TODO
func (ctrl *Controller) ProcessMemory(c *gin.Context) (sth.Result, error) {
	return sth.Result{"memory": ctrl.Core.ProcessMemory()}, nil
}

// AddUpstream TODO
func (ctrl *Controller) AddUpstream(c *gin.Context) (result sth.Result, err error) {
	var storeMeta *core.UpstreamStoreMeta = core.NewUpstreamStoreMeta(nil)

	if err = c.ShouldBindJSON(storeMeta); err != nil {
		err = controller.InvalidBody(fmt.Sprintf("%s", err))
	} else {
		storeMeta.Status = core.UpstreamWorking
		result, err = ctrl.UpstreamMgr.AddUpstream(storeMeta)
	}

	c.Header("Access-Control-Allow-Origin", "*")
	return
}

func operateUpstreamByQuery(c *gin.Context, f CallByUpstreamID) (result sth.Result, err error) {
	id := c.Query("id")

	if id == "" {
		err = controller.InvalidQuery("invalid id")
	} else {
		result, err = f(core.UpstreamID(id))
	}
	if result == nil {
		result = sth.Result{"id": id}
	}
	return
}

// PauseUpstream TODO
func (ctrl *Controller) PauseUpstream(c *gin.Context) (result sth.Result, err error) {
	return operateUpstreamByQuery(c, ctrl.UpstreamMgr.PauseUpstream)
}

// ResumeUpstream TODO
func (ctrl *Controller) ResumeUpstream(c *gin.Context) (result sth.Result, err error) {
	return operateUpstreamByQuery(c, ctrl.UpstreamMgr.ResumeUpstream)
}

// DeleteUpstream TODO
func (ctrl *Controller) DeleteUpstream(c *gin.Context) (sth.Result, error) {
	return operateUpstreamByQuery(c, ctrl.UpstreamMgr.DeleteUpstream)
}

// UpstreamInfo TODO
func (ctrl *Controller) UpstreamInfo(c *gin.Context) (sth.Result, error) {
	return operateUpstreamByQuery(c, ctrl.UpstreamMgr.UpstreamInfo)
}

// Upstreams TODO
func (ctrl *Controller) Upstreams(c *gin.Context) (result sth.Result, err error) {
	qs := c.DefaultQuery("status", utils.Text(core.UpstreamWorking))
	status, ok := core.UpstreamStatusMap[qs]
	if !ok {
		err = controller.InvalidQuery(fmt.Sprintf(`invalid status '%s'`, qs))
		return
	}
	return ctrl.UpstreamMgr.Upstreams(status)
}

// DownstreamInfo TODO
func (ctrl *Controller) DownstreamInfo(c *gin.Context) (result sth.Result, err error) {
	return nil, nil
}

// Downstreams TODO
func (ctrl *Controller) Downstreams(c *gin.Context) (sth.Result, error) {
	return nil, nil
}

// Queues TODO
func (ctrl *Controller) Queues(c *gin.Context) (result sth.Result, err error) {
	qk := c.DefaultQuery("k", "10")
	k, e := strconv.ParseInt(qk, 10, 64)
	if e != nil {
		err = controller.InvalidQuery(fmt.Sprintf("k=%s %s", qk, err))
	} else if k <= 0 || k > 1000 {
		err = controller.InvalidQuery(fmt.Sprintf("k=%s [1, 1000]", qk))
	}
	if err == nil {
		result = ctrl.UpstreamMgr.Queues(int(k))
	}
	return
}

// PopRequest TODO
func (ctrl *Controller) PopRequest(c *gin.Context) (result sth.Result, err error) {
	q := c.Query("q")

	qid, err := controller.QueueIDFromQuery(q)
	if err == nil {
		var req *request.Request
		req, err = ctrl.UpstreamMgr.PopRequest(qid)
		if err == nil {
			c.JSON(http.StatusOK, req)
		}
	}
	return
}
