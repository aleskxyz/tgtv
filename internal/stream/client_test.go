package stream

import (
	"testing"

	"github.com/gotd/td/tg"

	"github.com/aleskxyz/tgtv/internal/discovery"
)

func TestPeerDialogIDMatchesParticipant(t *testing.T) {
	chatID := int64(1140394567)
	dialogID := discovery.PeerDialogID(chatID)

	peer := &tg.PeerChannel{ChannelID: chatID}
	if got := peerDialogID(peer); got != dialogID {
		t.Fatalf("peerDialogID(channel) = %d, want %d", got, dialogID)
	}

	c := &Client{dialogID: dialogID}
	if peerDialogID(peer) != c.dialogID {
		t.Fatal("positive chat ID must not match participant peer without PeerDialogID conversion")
	}
}

func TestOurParticipantSource(t *testing.T) {
	const selfID int64 = 42
	participants := []tg.GroupCallParticipant{
		{Peer: &tg.PeerUser{UserID: 99}, Source: 111},
		{Peer: &tg.PeerUser{UserID: selfID}, Source: 222},
	}
	src, ok := ourParticipantSource(participants, selfID)
	if !ok || src != 222 {
		t.Fatalf("ourParticipantSource() = (%d, %v), want (222, true)", src, ok)
	}
	if _, ok := ourParticipantSource(participants, 1); ok {
		t.Fatal("expected missing self participant")
	}
}

func TestJoinSetsSourceFromSSRC(t *testing.T) {
	c := &Client{}
	// Simulate successful join without API: source must be set from join SSRC.
	const ssrc uint32 = 987654321
	c.source = int(ssrc)
	if c.source != int(ssrc) {
		t.Fatalf("source = %d, want %d", c.source, ssrc)
	}
}
