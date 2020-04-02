package controller

import (
	"net/http"
	"net/url"

	"github.com/cfhamlet/os-rq-pod/app/controller"
	"github.com/cfhamlet/os-rq-pod/pod"
)

// ErrorCode TODO
func ErrorCode(err error) int {
	var code int
	switch err.(type) {
	case *url.Error, controller.InvalidQuery, controller.InvalidBody:
		code = http.StatusBadRequest
	case pod.NotExistError:
		code = http.StatusNotFound
	case pod.UnavailableError, pod.ExistError:
		code = http.StatusNotAcceptable
	default:
		code = http.StatusInternalServerError
	}
	return code

}
