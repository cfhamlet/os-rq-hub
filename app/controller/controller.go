package controller

import (
	"fmt"
	"strconv"

	core "github.com/cfhamlet/os-rq-hub/hub"
	ctrl "github.com/cfhamlet/os-rq-pod/app/controller"
	"github.com/gin-gonic/gin"
)

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
func AddUpstream(c *gin.Context, hub *core.Hub) (core.Result, error) {
	return nil, nil
}

// DeleteUpstream TODO
func DeleteUpstream(c *gin.Context, hub *core.Hub) (core.Result, error) {
	return nil, nil
}

// UpdateUpstream TODO
func UpdateUpstream(c *gin.Context, hub *core.Hub) (core.Result, error) {
	return nil, nil
}

// UpstreamInfo TODO
func UpstreamInfo(c *gin.Context, hub *core.Hub) (core.Result, error) {
	return nil, nil
}

// Upstreams TODO
func Upstreams(c *gin.Context, hub *core.Hub) (core.Result, error) {
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
	if err != nil {
		return
	}

	result = hub.Queues(int(k))
	return
}

// GetRequest TODO
func GetRequest(c *gin.Context, hub *core.Hub) (core.Result, error) {
	return nil, nil
}
