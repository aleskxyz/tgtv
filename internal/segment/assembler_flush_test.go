package segment

import (
	"testing"
)

func TestAssemblerDropsPendingOnFlushError(t *testing.T) {
	a := NewAssembler("")
	a.separateAV = true
	a.pending[1000] = &pendingSegment{
		audio: &pendingSlice{container: "ogg", payload: []byte{1}},
		video: &pendingSlice{container: "not-a-real-container", payload: []byte{2, 3, 4}},
	}
	_, err := a.flushReadyLocked()
	if err == nil {
		t.Fatal("expected remux error")
	}
	if _, ok := a.pending[1000]; ok {
		t.Fatal("complete pending pair should be removed after flush error")
	}
}
