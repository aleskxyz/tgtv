package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadOrCreateMediamtxHLSCDNSecret_generatesAndPersists(t *testing.T) {
	dir := t.TempDir()
	cfg := Settings{ConfigDir: dir}

	secret1, err := LoadOrCreateMediamtxHLSCDNSecret(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if secret1 == "" || len(secret1) < 16 {
		t.Fatalf("unexpected secret: %q", secret1)
	}

	secret2, err := LoadOrCreateMediamtxHLSCDNSecret(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if secret1 != secret2 {
		t.Fatalf("expected persisted secret, got %q vs %q", secret1, secret2)
	}
	data, err := os.ReadFile(filepath.Join(dir, "mediamtx_hls_cdn_secret"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != secret1 {
		t.Fatalf("file contents = %q want %q", string(data), secret1)
	}
}
