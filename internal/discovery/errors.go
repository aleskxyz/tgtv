package discovery

import "errors"

var ErrUnknownStream = errors.New("unknown stream")

// StreamEndedError is returned when refresh confirms a live stream has ended.
type StreamEndedError struct {
	ChatID int64
	Reason string
}

func (e *StreamEndedError) Error() string {
	return e.Reason
}
