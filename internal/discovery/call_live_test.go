package discovery

import (
	"testing"
)

func TestVerifyCallStillLiveZeroHash(t *testing.T) {
	if got := VerifyCallStillLive(nil, nil, 1, 0); got != CallLiveUnknown {
		t.Fatalf("got %v want CallLiveUnknown", got)
	}
}
