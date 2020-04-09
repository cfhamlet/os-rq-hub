package controllers

import (
	"github.com/cfhamlet/os-rq-hub/hub/upstream"
	"github.com/cfhamlet/os-rq-pod/pkg/sth"
)

// CallByUpstreamID TODO
type CallByUpstreamID func(upstream.ID) (sth.Result, error)
