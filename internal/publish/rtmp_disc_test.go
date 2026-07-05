package publish

import "testing"

func TestFirstProcessDoesNotNeedDiscontinuity(t *testing.T) {
	p := NewPublisher("test", "rtmp://127.0.0.1/nope", "http://127.0.0.1/nope", "secret", nil)
	if p.ConsumeDiscontinuity() {
		t.Fatal("fresh publisher must not flag discontinuity before first restart")
	}
}

func TestResetMarksDiscontinuity(t *testing.T) {
	p := NewPublisher("test", "rtmp://127.0.0.1/nope", "http://127.0.0.1/nope", "secret", nil)
	p.Reset()
	if !p.ConsumeDiscontinuity() {
		t.Fatal("reset publisher must flag discontinuity for next media playlist")
	}
	if p.ConsumeDiscontinuity() {
		t.Fatal("discontinuity flag must be consumed once")
	}
}
