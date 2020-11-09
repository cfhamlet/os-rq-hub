package controllers

import (
	"net/http"
	"net/url"

	"github.com/cfhamlet/os-rq-pod/app/controllers"
	"github.com/cfhamlet/os-rq-pod/pkg/serv"
	plobal "github.com/cfhamlet/os-rq-pod/pod/global"
)

// ErrorCode TODO
func ErrorCode(err error) int {
	var code int
	switch err.(type) {
	case *url.Error, controllers.InvalidQuery, controllers.InvalidBody:
		code = http.StatusBadRequest
	case plobal.NotExistError:
		code = http.StatusNotFound
	case plobal.UnavailableError, plobal.ExistError,
		*serv.StatusConflictError, *serv.StatusError:
		code = http.StatusNotAcceptable
	default:
		code = http.StatusInternalServerError
	}
	return code

}
