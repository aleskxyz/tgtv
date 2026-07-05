package stream

import "github.com/gotd/td/tg"

// SelectPrimaryStreamChannel picks the highest-quality RTMP/unified stream channel.
// Telegram exposes simulcast layers as separate groupCallStreamChannel entries;
// LivePlayer.java requests QUALITY_FULL on the primary layer only.
func SelectPrimaryStreamChannel(channels []tg.GroupCallStreamChannel) (tg.GroupCallStreamChannel, bool) {
	var best tg.GroupCallStreamChannel
	found := false
	for _, ch := range channels {
		if ch.Channel <= 0 {
			continue
		}
		if !found || ch.Channel > best.Channel || (ch.Channel == best.Channel && ch.Scale > best.Scale) {
			best = ch
			found = true
		}
	}
	return best, found
}

// SelectPrimarySourceIndex picks the highest simulcast source index from a SIM group.
func SelectPrimarySourceIndex(sources []int) int {
	bestIdx := -1
	bestSrc := -1
	for i, src := range sources {
		if src > bestSrc {
			bestSrc = src
			bestIdx = i
		}
	}
	return bestIdx
}
