package discovery

import (
	"testing"

	"github.com/gotd/td/tg"
)

func TestChannelParticipantJoined(t *testing.T) {
	left := &tg.ChannelParticipantLeft{}
	member := &tg.ChannelParticipant{}
	if !channelParticipantJoined(left, member) {
		t.Fatal("expected join after channel left")
	}
	if channelParticipantJoined(member, member) {
		t.Fatal("expected no join when already member")
	}
}
