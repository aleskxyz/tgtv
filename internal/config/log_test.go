package config

import "testing"

func TestNewLoggerFormats(t *testing.T) {
	for _, format := range []string{"json", "jsonl", "console"} {
		log, err := NewLogger("debug", format)
		if err != nil {
			t.Fatalf("%s: %v", format, err)
		}
		if log == nil {
			t.Fatalf("%s: nil logger", format)
		}
		_ = log.Sync()
	}
}
