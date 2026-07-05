package ingest

import (
	"errors"
	"time"

	"github.com/aleskxyz/tgtv/internal/discovery"
)

var (
	ErrMaxConcurrentIngests   = errors.New("max concurrent ingests reached")
	ErrIngestBootstrapTimeout = errors.New("ingest bootstrap timed out")
	ErrIngestNotRunning       = errors.New("ingest not running")
	ErrRecoveryFailed         = errors.New("ingest recovery failed")
)

const recoveryRetryCooldown = 5 * time.Minute

func classifyIngestError(err error) error {
	if err == nil {
		return nil
	}
	var ended *discovery.StreamEndedError
	if errors.As(err, &ended) {
		return ended
	}
	if errors.Is(err, discovery.ErrUnknownStream) {
		return discovery.ErrUnknownStream
	}
	return err
}
