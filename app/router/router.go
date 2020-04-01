package router

import (
	"github.com/cfhamlet/os-rq-hub/app/controller"
	core "github.com/cfhamlet/os-rq-hub/hub"
	"github.com/cfhamlet/os-rq-pod/pkg/ginserv"
)

// InitAPIRouter TODO
func InitAPIRouter(g ginserv.RouterGroup, serv *core.Core) {
	ctrl := controller.New(serv)

	routers := []ginserv.RouterRecord{
		{M: g.POST, P: "/queues/", H: ctrl.Queues},

		{M: g.GET, P: "/system/info/", H: ctrl.Info},
		{M: g.GET, P: "/system/info/process/memory/", H: ctrl.ProcessMemory},
		{M: g.GET, P: "/system/info/redis/", H: ctrl.RedisInfo},

		{M: g.POST, P: "/upstream/", H: ctrl.AddUpstream},
		{M: g.DELETE, P: "/upstream/", H: ctrl.DeleteUpstream},
		{M: g.GET, P: "/upstream/resume/", H: ctrl.ResumeUpstream},
		{M: g.GET, P: "/upstream/pause/", H: ctrl.PauseUpstream},
		{M: g.GET, P: "/upstream/info/", H: ctrl.UpstreamInfo},
		{M: g.GET, P: "/upstreams/", H: ctrl.Upstreams},

		{M: g.GET, P: "/downstream/info/", H: ctrl.DownstreamInfo},
		{M: g.GET, P: "/downstreams/", H: ctrl.Downstreams},

		{M: g.POST, P: "/request/pop/", H: ctrl.PopRequest},
	}

	ginserv.Bind(routers, controller.ErrorCode)
}
