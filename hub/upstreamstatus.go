//go:generate stringer -type=UpstreamStatus -linecomment

package hub

// UpstreamStatus TODO
type UpstreamStatus int

// Status enum
const (
	_                   UpstreamStatus = iota
	UpstreamInit                       // "init"
	UpstreamWorking                    // = "working"
	UpstreamPaused                     // = "paused"
	UpstreamUnavailable                // = "unavailable"
	UpstreamStopping                   // = "stopping"
	UpstreamStopped                    // = "stopped"
	UpstreamRemoving                   // = "removing"
	UpstreamRemoved                    // = "removed"
)

// UpstreamStatusList TODO
var UpstreamStatusList = []UpstreamStatus{
	UpstreamInit,
	UpstreamWorking,
	UpstreamPaused,
	UpstreamUnavailable,
	UpstreamStopping,
	UpstreamStopped,
	UpstreamRemoving,
	UpstreamRemoved,
}
