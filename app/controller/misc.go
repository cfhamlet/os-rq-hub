package controller

import (
	core "github.com/cfhamlet/os-rq-hub/hub"
	"github.com/cfhamlet/os-rq-pod/pkg/sth"
)

// CallByUpstreamID TODO
type CallByUpstreamID func(core.UpstreamID) (sth.Result, error)
