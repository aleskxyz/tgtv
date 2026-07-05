package api

import "testing"

func TestValidHLSFilename(t *testing.T) {
	cases := map[string]bool{
		"index.m3u8":       true,
		"seg0.ts":          true,
		"main_stream.m3u8": true,
		"../etc/passwd":    false,
		"seg/0.ts":         false,
		"":                 false,
	}
	for name, want := range cases {
		if got := validHLSFilename(name); got != want {
			t.Fatalf("validHLSFilename(%q)=%v want %v", name, got, want)
		}
	}
}

func TestInjectDiscontinuityMediaPlaylist(t *testing.T) {
	in := "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXTINF:1.0,\nseg0.ts\n"
	out := InjectDiscontinuity(in)
	if !containsLine(out, "#EXT-X-DISCONTINUITY") {
		t.Fatalf("expected discontinuity tag in media playlist:\n%s", out)
	}
}

func TestInjectDiscontinuitySkipsMasterPlaylist(t *testing.T) {
	in := "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nhttp://example/hls/main_stream.m3u8\n"
	out := InjectDiscontinuity(in)
	if out != in {
		t.Fatalf("master playlist must be unchanged:\n%s", out)
	}
}

func containsLine(text, line string) bool {
	for _, l := range splitLines(text) {
		if l == line {
			return true
		}
	}
	return false
}

func splitLines(text string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(text); i++ {
		if text[i] == '\n' {
			line := text[start:i]
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
			lines = append(lines, line)
			start = i + 1
		}
	}
	if start < len(text) {
		lines = append(lines, text[start:])
	}
	return lines
}
