//go:generate stringer -type=UpstreamStatus -linecomment

package hub

import (
	"bytes"
	"fmt"

	"github.com/cfhamlet/os-rq-pod/pkg/json"
	"github.com/cfhamlet/os-rq-pod/pkg/utils"
)

// UpstreamStatus TODO
type UpstreamStatus int

// Status enum
const (
	_                   UpstreamStatus = iota
	UpstreamInit                       // init
	UpstreamWorking                    // working
	UpstreamPaused                     // paused
	UpstreamUnavailable                // unavailable
	UpstreamStopping                   // stopping
	UpstreamStopped                    // stopped
	UpstreamRemoving                   // removing
	UpstreamRemoved                    // removed
)

// UpstreamStatusMap TODO
var UpstreamStatusMap = map[string]UpstreamStatus{
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

// MarshalJSON TODO
func (s UpstreamStatus) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(`"`)
	buffer.WriteString(utils.Text(s))
	buffer.WriteString(`"`)
	return buffer.Bytes(), nil
}

// UnmarshalJSON TODO
func (s *UpstreamStatus) UnmarshalJSON(b []byte) error {
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
