package controllers

import (
	"net/http"
	"net/url"

	"github.com/cfhamlet/os-go-rq/controllers"
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
	case plobal.UnavailableError, plobal.ExistError:
		code = http.StatusNotAcceptable
	default:
		code = http.StatusInternalServerError
	}
	return code

}
