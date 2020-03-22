package controller

import (
	"net/http"
	"net/url"

	core "github.com/cfhamlet/os-rq-hub/hub"
	ctrl "github.com/cfhamlet/os-rq-pod/app/controller"
	"github.com/cfhamlet/os-rq-pod/pod"
	"github.com/gin-gonic/gin"
)

// CtrlFunc TODO
type CtrlFunc func(*gin.Context, *core.Hub) (core.Result, error)

// NewHandlerWrapper TODO
func NewHandlerWrapper(hub *core.Hub) *HandlerWrapper {
	return &HandlerWrapper{hub}
}

// HandlerWrapper TODO
type HandlerWrapper struct {
	hub *core.Hub
}

// Wrap TODO
func (wp *HandlerWrapper) Wrap(f CtrlFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		code := http.StatusOK
		result, err := f(c, wp.hub)

		if err != nil {
			switch err.(type) {
			case *url.Error, ctrl.InvalidQuery, ctrl.InvalidBody:
				code = http.StatusBadRequest
			case pod.QueueNotExistError, core.NotExistError:
				code = http.StatusNotFound
			case pod.UnavailableError, core.UnavailableError, core.ExistError:
				code = http.StatusNotAcceptable
			default:
				code = http.StatusInternalServerError
			}

			if result == nil {
				result = core.Result{}
			}

			result["err"] = err.Error()
		} else if result == nil {
			return
		}
		c.JSON(code, result)
	}
}
