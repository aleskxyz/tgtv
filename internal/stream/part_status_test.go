package stream

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gotd/td/tgerr"
)

func TestClassifyGetFileError_timeTooSmall(t *testing.T) {
	err := &tgerr.Error{Code: 400, Type: "TIME_TOO_SMALL", Message: "TIME_TOO_SMALL"}
	if got := ClassifyGetFileError(err); got != OutcomeResyncNeeded {
		t.Fatalf("TIME_TOO_SMALL: got %v, want ResyncNeeded", got)
	}
}

func TestClassifyGetFileError_timeTooBig(t *testing.T) {
	err := &tgerr.Error{Code: 400, Type: "TIME_TOO_BIG", Message: "TIME_TOO_BIG"}
	if got := ClassifyGetFileError(err); got != OutcomeNotReady {
		t.Fatalf("TIME_TOO_BIG: got %v, want NotReady", got)
	}
}

func TestClassifyGetFileError_wrapped(t *testing.T) {
	inner := ErrResyncNeeded
	if got := ClassifyGetFileError(inner); got != OutcomeResyncNeeded {
		t.Fatalf("wrapped ErrResyncNeeded: got %v", got)
	}
	if got := ClassifyGetFileError(errors.Join(ErrNotReady)); got != OutcomeNotReady {
		t.Fatalf("wrapped ErrNotReady: got %v", got)
	}
}

func TestClassifyGetFileError_streamDCWait(t *testing.T) {
	wait := streamDCWait{dcID: 4, until: time.Now().Add(time.Second)}
	err := fmt.Errorf("%w: %w", ErrNotReady, wait)
	if got := ClassifyGetFileError(err); got != OutcomeNotReady {
		t.Fatalf("streamDCWait: got %v, want NotReady", got)
	}
	if d, ok := streamDCWaitDelay(err); !ok || d <= 0 {
		t.Fatalf("streamDCWaitDelay: d=%v ok=%v", d, ok)
	}
}
