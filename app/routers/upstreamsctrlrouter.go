package routers

import (
	"github.com/cfhamlet/os-rq-hub/app/controllers"
	"github.com/cfhamlet/os-rq-pod/pkg/ginserv"
	"github.com/cfhamlet/os-rq-pod/pkg/ginserv/route"
)

// RouteUpstreamsCtrl TODO
func RouteUpstreamsCtrl(root ginserv.RouterGroup, ctrl *controllers.UpstreamController) {
	g := root.Group("/upstreams/")
	routes := []*route.Route{
		route.New(g.GET, "/", ctrl.Upstreams),
	}
	route.Bind(routes, controllers.ErrorCode)
}
