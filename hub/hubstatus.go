//go:generate stringer -type=Status

package hub

// Status type
type Status int

// Status enum
const (
	_ Status = iota
	Init
	Preparing
	Working
	Paused
	Stopping
	Stopped
)
