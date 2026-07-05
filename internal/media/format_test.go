package media

import "testing"

func TestMayNeedVideoTranscode(t *testing.T) {
	if !MayNeedVideoTranscode("webm") {
		t.Fatal("webm should need video transcode fallback")
	}
	if !MayNeedVideoTranscode("mkv") {
		t.Fatal("mkv should need video transcode fallback")
	}
	if MayNeedVideoTranscode("mp4") {
		t.Fatal("mp4 should not preemptively transcode")
	}
}

func TestVideoCodecNeedsTranscode(t *testing.T) {
	for _, codec := range []string{"vp9", "av1", "av01"} {
		if !VideoCodecNeedsTranscode(codec) {
			t.Fatalf("codec %q should need transcode", codec)
		}
	}
	if VideoCodecNeedsTranscode("h264") {
		t.Fatal("h264 should copy")
	}
}
