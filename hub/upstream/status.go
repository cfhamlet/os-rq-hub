//go:generate stringer -type=Status -linecomment

package upstream

import (
	"bytes"
	"fmt"

	"github.com/cfhamlet/os-rq-pod/pkg/json"
	"github.com/cfhamlet/os-rq-pod/pkg/utils"
)

// Status TODO
type Status int

// Status enum
const (
	_                   Status = iota
	UpstreamInit               // init
	UpstreamWorking            // working
	UpstreamPaused             // paused
	UpstreamUnavailable        // unavailable
	UpstreamStopping           // stopping
	UpstreamStopped            // stopped
	UpstreamRemoving           // removing
	UpstreamRemoved            // removed
)

// UpstreamStatusMap TODO
var UpstreamStatusMap = map[string]Status{
	utils.Text(UpstreamInit):        UpstreamInit,
	utils.Text(UpstreamWorking):     UpstreamWorking,
	utils.Text(UpstreamPaused):      UpstreamPaused,
	utils.Text(UpstreamUnavailable): UpstreamUnavailable,
	utils.Text(UpstreamStopping):    UpstreamStopping,
	utils.Text(UpstreamStopped):     UpstreamStopped,
	utils.Text(UpstreamRemoving):    UpstreamRemoving,
	utils.Text(UpstreamRemoved):     UpstreamRemoved,
}

// UpstreamStatusList TODO
var UpstreamStatusList = []Status{
	UpstreamInit,
	UpstreamWorking,
	UpstreamPaused,
	UpstreamUnavailable,
	UpstreamStopping,
	UpstreamStopped,
	UpstreamRemoving,
	UpstreamRemoved,
}

// MarshalJSON TODO
func (s Status) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(`"`)
	buffer.WriteString(utils.Text(s))
	buffer.WriteString(`"`)
	return buffer.Bytes(), nil
}

// UnmarshalJSON TODO
func (s *Status) UnmarshalJSON(b []byte) error {
	var j string
	err := json.Unmarshal(b, &j)
	if err != nil {
		return err
	}

	t, ok := UpstreamStatusMap[j]
	if !ok {
		return fmt.Errorf(`invalid UpstreamStatus value '%s'`, j)
	}

	*s = t
	return nil
}

// WorkUpstreamStatus TODO
func WorkUpstreamStatus(status Status) bool {
	return status == UpstreamWorking ||
		status == UpstreamPaused ||
		status == UpstreamUnavailable
}

// StopUpstreamStatus TODO
func StopUpstreamStatus(status Status) bool {
	return status == UpstreamStopping ||
		status == UpstreamStopped ||
		status == UpstreamRemoving ||
		status == UpstreamRemoved
}
