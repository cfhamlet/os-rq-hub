package router

import (
	"github.com/cfhamlet/os-rq-hub/app/controller"
	core "github.com/cfhamlet/os-rq-hub/hub"
	"github.com/cfhamlet/os-rq-pod/pkg/ginserv"
)

// InitAPIRouter TODO
func InitAPIRouter(g ginserv.RouterGroup, hub *core.Hub) {

	routers := []struct {
		HTTPFunc ginserv.IRoutesHTTPFunc
		Path     string
		F        controller.CtrlFunc
	}{
		{g.GET, "/queues/", controller.Queues},

		{g.GET, "/sys/info/", controller.Info},
		{g.GET, "/sys/info/process/memory/", controller.ProcessMemory},
		{g.GET, "/sys/info/redis/memory/", controller.RedisMemory},
		{g.GET, "/sys/info/redis/", controller.RedisInfo},

		{g.POST, "/upstream/", controller.AddUpstream},
		{g.DELETE, "/upstream/", controller.DeleteUpstream},
		{g.GET, "/upstream/start/", controller.StartUpstream},
		{g.GET, "/upstream/stop/", controller.StopUpstream},
		{g.GET, "/upstream/info/", controller.UpstreamInfo},
		{g.GET, "/upstreams/", controller.Upstreams},

		{g.GET, "/downstream/info/", controller.DownstreamInfo},
		{g.GET, "/downstreams/", controller.Downstreams},

		{g.GET, "/request/pop/", controller.GetRequest},
	}

	wp := controller.NewHandlerWrapper(hub)

	for _, r := range routers {
		r.HTTPFunc(r.Path, wp.Wrap(r.F))
	}
}
