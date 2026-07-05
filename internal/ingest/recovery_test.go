package ingest

import (
	"testing"
	"time"

	"github.com/aleskxyz/tgtv/internal/config"
	"github.com/aleskxyz/tgtv/internal/stream"
)

func TestBeginHardRejoinCooldown(t *testing.T) {
	s := &Session{}
	if !s.beginHardRejoin() {
		t.Fatal("expected first rejoin to begin")
	}
	s.endHardRejoin()

	if s.beginHardRejoin() {
		t.Fatal("expected cooldown to block immediate second rejoin")
	}
	s.cancelHardRejoin()

	time.Sleep(stream.MinRejoinCooldown)
	if !s.beginHardRejoin() {
		t.Fatal("expected rejoin after cooldown elapsed")
	}
	s.endHardRejoin()
}

func TestJoinFloodGraceBlocksRejoin(t *testing.T) {
	s := &Session{}
	s.noteJoinFloodWait(30 * time.Second)
	if s.beginHardRejoin() {
		t.Fatal("expected join flood grace to block rejoin")
	}
}

func TestShouldDeferRejoinDuringActiveRejoin(t *testing.T) {
	s := &Session{rejoinActive: true}
	if !s.shouldDeferRejoin() {
		t.Fatal("expected active rejoin to defer")
	}
}

func TestShouldHoldHLSDuringRejoin(t *testing.T) {
	s := &Session{}
	if s.shouldHoldHLS() {
		t.Fatal("expected no HLS hold before rejoin")
	}
	s.beginRecoveryRejoin()
	if !s.shouldHoldHLS() {
		t.Fatal("expected HLS hold during rejoin")
	}
	s.clearRecoveryOnSegment()
	if s.shouldHoldHLS() {
		t.Fatal("expected HLS hold cleared after segment")
	}
}

func TestIngestRecentlyHealthy(t *testing.T) {
	s := &Session{}
	if s.ingestRecentlyHealthy() {
		t.Fatal("expected no segments to be unhealthy")
	}
	s.noteSegmentOut()
	if !s.ingestRecentlyHealthy() {
		t.Fatal("expected fresh segment to be healthy")
	}
	s.statsMu.Lock()
	s.lastSegmentAt = time.Now().Add(-stream.IngestHealthyWindow - time.Second)
	s.statsMu.Unlock()
	if s.ingestRecentlyHealthy() {
		t.Fatal("expected stale segment to be unhealthy")
	}
}

func TestIngestHealthyIgnoresPartsWithoutSegments(t *testing.T) {
	s := &Session{}
	s.notePartReceived()
	if s.ingestRecentlyHealthy() {
		t.Fatal("parts without published segments must not count as healthy")
	}
}

func TestIsOutputStalledRequiresRecentInput(t *testing.T) {
	s := &Session{sup: &Supervisor{cfg: config.Settings{IngestRebufferSeconds: 3, IngestStartupGraceSeconds: 15}}}
	s.startedAt = time.Now().Add(-20 * time.Second)
	s.notePartReceived()
	s.statsMu.Lock()
	s.lastPartAt = time.Now().Add(-5 * time.Second)
	s.lastSegmentAt = time.Now().Add(-4 * time.Second)
	s.statsMu.Unlock()
	if s.isOutputStalled() {
		t.Fatal("expected no output stall when input is not recent")
	}
}

func TestIsOutputStalledDetectsLag(t *testing.T) {
	s := &Session{sup: &Supervisor{cfg: config.Settings{IngestRebufferSeconds: 3, IngestStartupGraceSeconds: 15}}}
	s.startedAt = time.Now().Add(-20 * time.Second)
	s.unifiedIngest = true
	s.statsMu.Lock()
	s.segmentsIn = 10
	s.segmentsOut = 5
	s.lastPartAt = time.Now().Add(-500 * time.Millisecond)
	s.lastSegmentAt = time.Now().Add(-4 * time.Second)
	s.statsMu.Unlock()
	if !s.isOutputStalled() {
		t.Fatal("expected output stall when input is recent and output is stale")
	}
}

func TestIsOutputStalledRespectsStartupGrace(t *testing.T) {
	s := &Session{sup: &Supervisor{cfg: config.Settings{IngestRebufferSeconds: 3, IngestStartupGraceSeconds: 15}}}
	s.startedAt = time.Now().Add(-5 * time.Second)
	s.statsMu.Lock()
	s.lastPartAt = time.Now().Add(-500 * time.Millisecond)
	s.lastSegmentAt = time.Now().Add(-4 * time.Second)
	s.statsMu.Unlock()
	if s.isOutputStalled() {
		t.Fatal("expected no output stall during startup grace")
	}
}
