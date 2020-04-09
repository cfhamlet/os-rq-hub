package routers

import (
	"github.com/cfhamlet/os-rq-hub/app/controllers"
	"github.com/cfhamlet/os-rq-pod/pkg/ginserv"
	"github.com/cfhamlet/os-rq-pod/pkg/ginserv/route"
)

// RouteUpstreamCtrl TODO
func RouteUpstreamCtrl(root ginserv.RouterGroup, ctrl *controllers.UpstreamController) {
	g := root.Group("/upstream/")
	routes := []*route.Route{
		route.New(g.POST, "/", ctrl.AddUpstream),
		route.New(g.DELETE, "/", ctrl.DeleteUpstream),
		route.New(g.POST, "/resume/", ctrl.ResumeUpstream),
		route.New(g.POST, "/pause/", ctrl.PauseUpstream),
		route.New(g.GET, "/info/", ctrl.UpstreamInfo),
	}
	route.Bind(routes, controllers.ErrorCode)
}
