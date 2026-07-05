package segment

import "testing"

func TestClearPendingPreservesTSOffset(t *testing.T) {
	a := NewAssembler("")
	a.tsOffset = 8.5
	a.pending[1000] = &pendingSegment{}
	a.ClearPending()
	if a.tsOffset != 8.5 {
		t.Fatalf("tsOffset = %v, want 8.5", a.tsOffset)
	}
	if len(a.pending) != 0 {
		t.Fatalf("pending map not cleared")
	}
	a.Reset()
	if a.tsOffset != 0 {
		t.Fatalf("Reset tsOffset = %v, want 0", a.tsOffset)
	}
}
