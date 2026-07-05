package ingest

import (
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/aleskxyz/tgtv/internal/config"
	"github.com/aleskxyz/tgtv/internal/discovery"
)

func TestRecoveryBlockedPreventsEnsureIngest(t *testing.T) {
	reg := discovery.NewRegistry()
	s := NewSupervisor(nil, nil, reg, nil, defaultTestConfig(), nil, zap.NewNop())
	entry, _ := reg.Upsert(1, 10, "Live", 99)

	s.noteRecoveryFailure(entry.StreamID)
	if !s.IsRecoveryFailed(entry.StreamID) {
		t.Fatal("expected recovery failure to be visible")
	}
	if err := s.EnsureIngest(nil, entry.StreamID); err != ErrRecoveryFailed {
		t.Fatalf("EnsureIngest err=%v want ErrRecoveryFailed", err)
	}
}

func TestRecoveryBlockedExpires(t *testing.T) {
	reg := discovery.NewRegistry()
	s := NewSupervisor(nil, nil, reg, nil, defaultTestConfig(), nil, zap.NewNop())
	entry, _ := reg.Upsert(2, 20, "Live", 99)

	s.mu.Lock()
	s.recoveryBlockedUntil[entry.StreamID] = time.Now().Add(-time.Second)
	s.mu.Unlock()

	if s.recoveryBlocked(entry.StreamID) {
		t.Fatal("expected expired recovery block to clear")
	}
}

func defaultTestConfig() config.Settings {
	return config.Settings{MaxConcurrentIngests: 4}
}
