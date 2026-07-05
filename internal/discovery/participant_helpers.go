package discovery

import "github.com/gotd/td/tg"

func channelParticipantJoined(prev, new tg.ChannelParticipantClass) bool {
	if _, left := new.(*tg.ChannelParticipantLeft); left {
		return false
	}
	if new == nil {
		return false
	}
	if prev == nil {
		return true
	}
	_, wasLeft := prev.(*tg.ChannelParticipantLeft)
	return wasLeft
}

func channelParticipantLeft(prev, new tg.ChannelParticipantClass) bool {
	if _, ok := new.(*tg.ChannelParticipantLeft); ok {
		return true
	}
	return new == nil && prev != nil
}
