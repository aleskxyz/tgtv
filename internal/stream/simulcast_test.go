package stream

import (
	"testing"

	"github.com/gotd/td/tg"
)

func TestSelectPrimaryStreamChannel(t *testing.T) {
	channels := []tg.GroupCallStreamChannel{
		{Channel: 1, Scale: 0, LastTimestampMs: 1000},
		{Channel: 3, Scale: 1, LastTimestampMs: 2000},
		{Channel: 2, Scale: 0, LastTimestampMs: 1500},
	}
	got, ok := SelectPrimaryStreamChannel(channels)
	if !ok || got.Channel != 3 {
		t.Fatalf("SelectPrimaryStreamChannel() = (%d, %v), want channel 3", got.Channel, ok)
	}
	if got.LastTimestampMs != 2000 {
		t.Fatalf("LastTimestampMs = %d, want 2000", got.LastTimestampMs)
	}
}

func TestSelectPrimarySourceIndex(t *testing.T) {
	if got := SelectPrimarySourceIndex([]int{10, 30, 20}); got != 1 {
		t.Fatalf("SelectPrimarySourceIndex() = %d, want 1", got)
	}
	if got := SelectPrimarySourceIndex(nil); got != -1 {
		t.Fatalf("SelectPrimarySourceIndex(nil) = %d, want -1", got)
	}
}
