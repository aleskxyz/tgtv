package publish

import (
	"strings"
	"testing"
)

func TestRTMPCommandOmitsDiscardCorrupt(t *testing.T) {
	cmd := rtmpCommand("rtmp://127.0.0.1/live/test")
	joined := strings.Join(cmd, " ")
	if strings.Contains(joined, "discardcorrupt") {
		t.Fatalf("rtmp input must not use discardcorrupt (drops video at segment joins): %q", joined)
	}
	idx := -1
	for i, arg := range cmd {
		if arg == "-fflags" {
			idx = i
			break
		}
	}
	if idx < 0 || idx+1 >= len(cmd) {
		t.Fatal("missing -fflags in rtmp command")
	}
	if cmd[idx+1] != rtmpInputFFlags {
		t.Fatalf("fflags = %q, want %q", cmd[idx+1], rtmpInputFFlags)
	}
}
