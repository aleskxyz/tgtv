package discovery_test

import (
	"testing"

	"github.com/aleskxyz/tgtv/internal/discovery"
)

func TestMakeStreamID(t *testing.T) {
	a := discovery.MakeStreamID(100)
	b := discovery.MakeStreamID(100)
	if a != b || len(a) != 12 {
		t.Fatalf("stream id=%q", a)
	}
	if discovery.MakeStreamID(101) == a {
		t.Fatal("expected different ids for different chats")
	}
}

func TestRegistryUpsertStablePerChat(t *testing.T) {
	r := discovery.NewRegistry()
	e1, _ := r.Upsert(1, 10, "News", 99)
	e2, superseded := r.Upsert(1, 11, "News Live", 100)
	if !superseded {
		t.Fatal("expected call id change to be reported as supersession")
	}
	if e1.StreamID != e2.StreamID {
		t.Fatalf("stream id changed: %q -> %q", e1.StreamID, e2.StreamID)
	}
	if e2.CallID != 11 {
		t.Fatalf("call id=%d", e2.CallID)
	}
	if _, ok := r.Get(e1.StreamID); !ok {
		t.Fatal("stream should remain active under same id")
	}
}

func TestRegistryUpsertReactivatesSameStreamID(t *testing.T) {
	r := discovery.NewRegistry()
	e1, _ := r.Upsert(42, 10, "Channel", 99)
	if _, ok := r.MarkEnded(42); !ok {
		t.Fatal("expected mark ended")
	}
	r.RemoveEndedChat(42)

	e2, _ := r.Upsert(42, 20, "Channel", 100)
	if e1.StreamID != e2.StreamID {
		t.Fatalf("stream id changed after rejoin: %q -> %q", e1.StreamID, e2.StreamID)
	}
}

func TestRegistryUpsertCallSupersededWhenIngesting(t *testing.T) {
	r := discovery.NewRegistry()
	entry, _ := r.Upsert(5, 100, "Live", 11)
	r.SetStatus(entry.StreamID, discovery.StatusIngesting)

	_, superseded := r.Upsert(5, 200, "Live", 22)
	if !superseded {
		t.Fatal("expected call supersession while ingesting")
	}
	got, ok := r.Get(entry.StreamID)
	if !ok || got.CallID != 200 {
		t.Fatalf("CallID = %d, ok=%v, want 200", got.CallID, ok)
	}

	r.SetStatus(entry.StreamID, discovery.StatusDiscovered)
	_, superseded = r.Upsert(5, 300, "Live", 33)
	if !superseded {
		t.Fatal("expected call supersession when call id changes regardless of status")
	}

	r.SetStatus(entry.StreamID, discovery.StatusStreaming)
	_, superseded = r.Upsert(5, 400, "Live", 44)
	if !superseded {
		t.Fatal("expected call supersession while streaming")
	}
}

func TestRegistryMarkEndedIdempotent(t *testing.T) {
	r := discovery.NewRegistry()
	entry, _ := r.Upsert(9, 1, "Live", 1)
	if _, ok := r.MarkEnded(9); !ok {
		t.Fatal("expected first mark ended")
	}
	if _, ok := r.MarkEnded(9); ok {
		t.Fatal("expected second mark ended to be no-op")
	}
	if _, ok := r.Get(entry.StreamID); ok {
		t.Fatal("ended entry should not be active")
	}
}

func TestRegistryUpdateTitle(t *testing.T) {
	r := discovery.NewRegistry()
	r.Upsert(7, 1, "Old Name", 0)

	if r.UpdateTitle(7, "Old Name") {
		t.Fatal("expected no change for same title")
	}
	if !r.UpdateTitle(7, "New Name") {
		t.Fatal("expected title change")
	}
	got, ok := r.SnapshotByChat(7)
	if !ok || got.Title != "New Name" {
		t.Fatalf("title=%q ok=%v", got.Title, ok)
	}
	if r.UpdateTitle(99, "X") {
		t.Fatal("unknown chat should not update")
	}
}
