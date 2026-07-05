package discovery

import (
	"testing"

	"github.com/gotd/td/tgerr"
)

func TestUseCachedLiveRequiresCallHash(t *testing.T) {
	_, err := useCachedLive(nil, "", LiveEntry{CallAccessHash: 0})
	if err == nil {
		t.Fatal("expected error without cached call hash")
	}

	entry, err := useCachedLive(nil, "", LiveEntry{CallAccessHash: 456, CallID: 123})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if entry.CallAccessHash != 456 {
		t.Fatalf("CallAccessHash = %d, want 456", entry.CallAccessHash)
	}
}

func TestUseCachedLivePrefersFreshRegistryCall(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(1, 100, "Live", 11)
	_, err := useCachedLive(reg, MakeStreamID(1), LiveEntry{CallAccessHash: 11, CallID: 100})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_, superseded := reg.Upsert(1, 200, "Live", 22)
	if !superseded {
		t.Fatal("expected supersession")
	}
	entry, err := useCachedLive(reg, MakeStreamID(1), LiveEntry{CallAccessHash: 11, CallID: 100})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if entry.CallID != 200 || entry.CallAccessHash != 22 {
		t.Fatalf("stale entry returned: call=%d hash=%d", entry.CallID, entry.CallAccessHash)
	}
}

func TestIsGroupCallInvalid(t *testing.T) {
	if isGroupCallInvalid(nil) {
		t.Fatal("nil error should not be invalid call")
	}
	err := &tgerr.Error{Code: 400, Message: "GROUPCALL_INVALID", Type: "GROUPCALL_INVALID"}
	if !isGroupCallInvalid(err) {
		t.Fatal("expected GROUPCALL_INVALID")
	}
}
