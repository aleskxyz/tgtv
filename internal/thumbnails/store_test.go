package thumbnails

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"go.uber.org/zap"
)

func TestLogoPathRequiresFreshCache(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(nil, dir, zap.NewNop())
	path := filepath.Join(dir, "99.jpg")
	if err := os.WriteFile(path, []byte("jpeg"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := store.LogoPath(99); got == "" {
		t.Fatal("expected fresh logo path")
	}
	old := time.Now().Add(-25 * time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	if got := store.LogoPath(99); got != "" {
		t.Fatalf("expected stale logo rejected, got %q", got)
	}
}

func TestSyncPhotoFromChat(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(nil, dir, zap.NewNop())

	ch := &tg.Channel{ID: 123, Title: "News", Photo: &tg.ChatPhoto{PhotoID: 1}}
	if store.SyncPhotoFromChat(ch) {
		t.Fatal("first observation should not refresh")
	}

	ch.Photo = &tg.ChatPhoto{PhotoID: 2}
	if !store.SyncPhotoFromChat(ch) {
		t.Fatal("expected refresh when photo id changes")
	}
	if _, err := os.Stat(filepath.Join(dir, "123.jpg")); !os.IsNotExist(err) {
		t.Fatal("expected cache invalidated")
	}

	ch.Photo = &tg.ChatPhotoEmpty{}
	if !store.SyncPhotoFromChat(ch) {
		t.Fatal("expected refresh when photo removed")
	}

	store.writePhotoSidecar(123, 0)
	ch.Photo = &tg.ChatPhoto{PhotoID: 99}
	if !store.SyncPhotoFromChat(ch) {
		t.Fatal("expected refresh when first profile photo is added")
	}
}

func TestSyncPhotoFromChatUsesSidecar(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(nil, dir, zap.NewNop())
	store.writePhotoSidecar(123, 1)
	if err := os.WriteFile(filepath.Join(dir, "123.jpg"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}

	ch := &tg.Channel{ID: 123, Photo: &tg.ChatPhoto{PhotoID: 2}}
	if !store.SyncPhotoFromChat(ch) {
		t.Fatal("expected refresh when sidecar photo id differs")
	}
	if _, err := os.Stat(filepath.Join(dir, "123.jpg")); !os.IsNotExist(err) {
		t.Fatal("expected cache invalidated")
	}
}
