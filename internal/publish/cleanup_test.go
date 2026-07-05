package publish

import "testing"

func TestStreamIDFromRTMPCmd(t *testing.T) {
	cmd := "ffmpeg -hide_banner -f mpegts -i pipe:0 -c:v copy rtmp://127.0.0.1:1935/live/abc123def456"
	got := streamIDFromRTMPCmd(cmd, "rtmp://127.0.0.1:1935/live")
	if got != "abc123def456" {
		t.Fatalf("got %q want abc123def456", got)
	}
}

func TestIsStaleRTMPProcess(t *testing.T) {
	url := "rtmp://127.0.0.1:1935/live/teststream"
	cmd := "ffmpeg\x00-f\x00mpegts\x00" + url
	if !isStaleRTMPProcess(cmd, url) {
		t.Fatal("expected stale match")
	}
	if isStaleRTMPProcess("other-process", url) {
		t.Fatal("unexpected match")
	}
}
