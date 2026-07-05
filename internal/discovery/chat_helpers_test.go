package discovery

import "testing"

func TestPeerDialogID(t *testing.T) {
	if got := PeerDialogID(1140394567); got != -1140394567 {
		t.Fatalf("PeerDialogID(positive) = %d, want -1140394567", got)
	}
	if got := PeerDialogID(-1140394567); got != -1140394567 {
		t.Fatalf("PeerDialogID(already negative) = %d, want -1140394567", got)
	}
}
