package ingest

import (
	"testing"
	"time"

	"github.com/aleskxyz/tgtv/internal/config"
)

func TestIsOutputStalled_suppressedBeforeFirstMux(t *testing.T) {
	s := &Session{
		sup: &Supervisor{cfg: config.Settings{IngestStartupGraceSeconds: 0}},
	}
	s.startedAt = time.Now().Add(-time.Minute)
	s.unifiedIngest = false
	s.rtmpMuxStarted.Store(false)
	s.statsMu.Lock()
	s.segmentsIn = 5
	s.lastPartAt = time.Now()
	s.statsMu.Unlock()

	if s.isOutputStalled() {
		t.Fatal("output stall should be suppressed during separate A/V bootstrap")
	}
}
