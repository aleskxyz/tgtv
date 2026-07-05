package discovery

import (
	"testing"

	"go.uber.org/zap"
)

func TestUpsertLiveLogsOnlyOnNewOrSuperseded(t *testing.T) {
	reg := NewRegistry()
	s := &Scanner{
		registry: reg,
		log:      zap.NewNop(),
	}
	chatID := int64(1140394567)

	s.upsertLive(chatID, 100, "Iran", 1, true)
	if _, ok := reg.SnapshotByChat(chatID); !ok {
		t.Fatal("expected live entry")
	}

	// Repeated upsert for same call should not trigger onLiveDiscovered again.
	discovered := 0
	s.onLiveDiscovered = func(int64) { discovered++ }
	s.upsertLive(chatID, 100, "Iran", 1, true)
	if discovered != 0 {
		t.Fatalf("onLiveDiscovered called %d times, want 0", discovered)
	}

	s.upsertLive(chatID, 200, "Iran", 2, true)
	if discovered != 0 {
		t.Fatalf("superseded should not re-fire onLiveDiscovered, got %d", discovered)
	}
	entry, _ := reg.SnapshotByChat(chatID)
	if entry.CallID != 200 {
		t.Fatalf("call id = %d want 200", entry.CallID)
	}
}
