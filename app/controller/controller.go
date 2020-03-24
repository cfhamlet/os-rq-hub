package controller

import (
	"fmt"
	"strconv"

	core "github.com/cfhamlet/os-rq-hub/hub"
	ctrl "github.com/cfhamlet/os-rq-pod/app/controller"
	"github.com/cfhamlet/os-rq-pod/pkg/utils"
	"github.com/gin-gonic/gin"
)

// CallByUpstreamID TODO
type CallByUpstreamID func(core.UpstreamID) (core.Result, error)

func infoResult(info interface{}, err error) (core.Result, error) {
	return core.Result{"info": info}, err
}

// RedisMemory TODO
func RedisMemory(c *gin.Context, hub *core.Hub) (core.Result, error) {
	return infoResult(hub.Client.Info("memory").Result())
}

// RedisInfo TODO
func RedisInfo(c *gin.Context, hub *core.Hub) (core.Result, error) {
	return infoResult(hub.Client.Info().Result())
}

// ProcessMemory TODO
func ProcessMemory(c *gin.Context, hub *core.Hub) (core.Result, error) {
	return infoResult(hub.Process.MemoryInfo())
}

// Info TODO
func Info(c *gin.Context, hub *core.Hub) (core.Result, error) {
	return hub.Info()
}

// AddUpstream TODO
func AddUpstream(c *gin.Context, hub *core.Hub) (result core.Result, err error) {
	var metaStore *core.UpstreamStoreMeta = core.NewUpstreamStoreMeta(nil)

	if err = c.ShouldBindJSON(metaStore); err != nil {
		err = ctrl.InvalidBody(fmt.Sprintf("%s", err))
	} else {
		result, err = hub.AddUpstream(metaStore)
	}

	c.Header("Access-Control-Allow-Origin", "*")
	return
}

func operateUpstreamByQuery(c *gin.Context, f CallByUpstreamID) (result core.Result, err error) {
	uid := c.Query("uid")

	if uid == "" {
		err = ctrl.InvalidQuery("invalid uid")
	} else {
		result, err = f(core.UpstreamID(uid))
	}
	if result == nil {
		result = core.Result{"uid": uid}
	}
	return
}

// PauseUpstream TODO
func PauseUpstream(c *gin.Context, hub *core.Hub) (result core.Result, err error) {
	return operateUpstreamByQuery(c, hub.PauseUpstream)
}

// ResumeUpstream TODO
func ResumeUpstream(c *gin.Context, hub *core.Hub) (result core.Result, err error) {
	return operateUpstreamByQuery(c, hub.ResumeUpstream)
}

// DeleteUpstream TODO
func DeleteUpstream(c *gin.Context, hub *core.Hub) (core.Result, error) {
	return operateUpstreamByQuery(c, hub.DeleteUpstream)
}

// UpstreamInfo TODO
func UpstreamInfo(c *gin.Context, hub *core.Hub) (core.Result, error) {
	return operateUpstreamByQuery(c, hub.UpstreamInfo)
}

// Upstreams TODO
func Upstreams(c *gin.Context, hub *core.Hub) (result core.Result, err error) {
	qs := c.DefaultQuery("status", utils.Text(core.UpstreamWorking))
	status, ok := core.UpstreamStatusMap[qs]
	if !ok {
		err = ctrl.InvalidQuery(fmt.Sprintf(`invalid status '%s'`, qs))
		return
	}
	return hub.Upstreams(status)
}

// DownstreamInfo TODO
func DownstreamInfo(c *gin.Context, hub *core.Hub) (result core.Result, err error) {
	return nil, nil
}

// Downstreams TODO
func Downstreams(c *gin.Context, hub *core.Hub) (core.Result, error) {
	return nil, nil
}

// Queues TODO
func Queues(c *gin.Context, hub *core.Hub) (result core.Result, err error) {
	qk := c.DefaultQuery("k", "10")
	k, e := strconv.ParseInt(qk, 10, 64)
	if e != nil {
		err = ctrl.InvalidQuery(fmt.Sprintf("k=%s %s", qk, err))
	} else if k <= 0 || k > 1000 {
		err = ctrl.InvalidQuery(fmt.Sprintf("k=%s [1, 1000]", qk))
	}
	if err == nil {
		result, err = hub.Queues(int(k))
	}
	return
}

// GetRequest TODO
func GetRequest(c *gin.Context, hub *core.Hub) (result core.Result, err error) {
	q := c.Query("q")

	qid, err := ctrl.QueueIDFromQuery(q)
	if err == nil {
		result, err = hub.GetRequest(qid)
	}
	return
}
