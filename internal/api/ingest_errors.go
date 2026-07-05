package api

import (
	"errors"
	"net/http"

	"go.uber.org/zap"

	"github.com/aleskxyz/tgtv/internal/discovery"
	"github.com/aleskxyz/tgtv/internal/ingest"
)

func (s *Server) respondIngestError(w http.ResponseWriter, err error) bool {
	if err == nil {
		return false
	}
	var ended *discovery.StreamEndedError
	switch {
	case errors.As(err, &ended):
		http.Error(w, "stream ended", http.StatusGone)
	case errors.Is(err, discovery.ErrUnknownStream):
		w.WriteHeader(http.StatusNotFound)
	case errors.Is(err, ingest.ErrMaxConcurrentIngests):
		http.Error(w, "ingest capacity reached", http.StatusServiceUnavailable)
	case errors.Is(err, ingest.ErrIngestBootstrapTimeout):
		http.Error(w, "ingest bootstrap timed out", http.StatusServiceUnavailable)
	case errors.Is(err, ingest.ErrIngestNotRunning):
		http.Error(w, "ingest not running", http.StatusServiceUnavailable)
	case errors.Is(err, ingest.ErrRecoveryFailed):
		w.WriteHeader(http.StatusNotFound)
	default:
		s.log.Warn("ingest start failed", zap.Error(err))
		http.Error(w, "ingest start failed", http.StatusServiceUnavailable)
	}
	return true
}
