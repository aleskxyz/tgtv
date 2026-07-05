package stream

import (
	"errors"
	"time"

	"github.com/gotd/td/tgerr"
)

// PartOutcome mirrors BroadcastPart::Status in tgcalls.
type PartOutcome int

const (
	OutcomeNotReady PartOutcome = iota
	OutcomeResyncNeeded
)

var (
	ErrResyncNeeded = errors.New("stream resync needed")
)

// ClassifyGetFileError maps LivePlayer.java getFile error handling (lines 493-500).
// status 0 => NotReady (TIME_TOO_BIG, FLOOD_WAIT)
// status -1 => ResyncNeeded (everything else)
func ClassifyGetFileError(err error) PartOutcome {
	if errors.Is(err, ErrNotReady) {
		return OutcomeNotReady
	}
	if _, ok := asStreamDCWait(err); ok {
		return OutcomeNotReady
	}
	if errors.Is(err, ErrResyncNeeded) {
		return OutcomeResyncNeeded
	}
	if tgerr.Is(err, "TIME_TOO_BIG") {
		return OutcomeNotReady
	}
	if _, ok := tgerr.AsFloodWait(err); ok {
		return OutcomeNotReady
	}
	return OutcomeResyncNeeded
}

// FloodWaitDelay reports Telegram FLOOD_WAIT backoff from an RPC error.
func FloodWaitDelay(err error) (time.Duration, bool) {
	return tgerr.AsFloodWait(err)
}

func floodWaitDelay(err error) (time.Duration, bool) {
	return FloodWaitDelay(err)
}
