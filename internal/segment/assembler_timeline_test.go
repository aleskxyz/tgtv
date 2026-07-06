package segment

import "testing"

func TestSyncOutputTimelineLocked_branchJump(t *testing.T) {
	a := NewAssembler("")
	a.lastPartTS = 2_275_000
	a.tsOffset = 45.0

	a.syncOutputTimelineLocked(2_302_000)

	want := 45.0 + 26.0 // (2302000 - 2275000 - 1000) / 1000
	if a.tsOffset != want {
		t.Fatalf("tsOffset = %v want %v", a.tsOffset, want)
	}
}

func TestSyncOutputTimelineLocked_contiguous(t *testing.T) {
	a := NewAssembler("")
	a.lastPartTS = 1000
	a.tsOffset = 5.0

	a.syncOutputTimelineLocked(2000)

	if a.tsOffset != 5.0 {
		t.Fatalf("tsOffset = %v want unchanged 5.0", a.tsOffset)
	}
}

func TestSyncOutputTimelineLocked_firstPart(t *testing.T) {
	a := NewAssembler("")

	a.syncOutputTimelineLocked(1_000_000)

	if a.tsOffset != 0 {
		t.Fatalf("tsOffset = %v want 0 for first part", a.tsOffset)
	}
}

func TestClearPendingPreservesLastPartTS(t *testing.T) {
	a := NewAssembler("")
	a.lastPartTS = 2_275_000
	a.tsOffset = 45.0
	a.pending[1000] = &pendingSegment{}
	a.ClearPending()
	if a.lastPartTS != 2_275_000 {
		t.Fatalf("lastPartTS = %d want preserved", a.lastPartTS)
	}
}

func TestResetClearsLastPartTS(t *testing.T) {
	a := NewAssembler("")
	a.lastPartTS = 1000
	a.Reset()
	if a.lastPartTS != 0 {
		t.Fatalf("lastPartTS = %d want 0", a.lastPartTS)
	}
}
