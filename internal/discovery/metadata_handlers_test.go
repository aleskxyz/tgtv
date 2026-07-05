package discovery

import (
	"testing"

	"github.com/gotd/td/tg"
	"go.uber.org/zap"

	"github.com/aleskxyz/tgtv/internal/config"
)

func TestMetadataAction(t *testing.T) {
	name, ok := metadataAction(&tg.MessageActionChatEditTitle{Title: "New"})
	if !ok || name != "chat_edit_title" {
		t.Fatalf("title action = %q ok=%v", name, ok)
	}
	name, ok = metadataAction(&tg.MessageActionChatEditPhoto{})
	if !ok || name != "chat_edit_photo" {
		t.Fatalf("photo action = %q ok=%v", name, ok)
	}
	_, ok = metadataAction(&tg.MessageActionChannelCreate{})
	if ok {
		t.Fatal("expected unrelated action ignored")
	}
}

func TestMetadataChangeFromMessage(t *testing.T) {
	msg := &tg.MessageService{
		PeerID: &tg.PeerChannel{ChannelID: 4420157879},
		Action: &tg.MessageActionChatEditTitle{Title: "New"},
	}
	chatID, action, ok := metadataChangeFromService(msg, peerChannelID)
	if !ok || chatID != 4420157879 || action != "chat_edit_title" {
		t.Fatalf("got chat=%d action=%q ok=%v", chatID, action, ok)
	}
}

func TestRefreshLiveMetadataIfActive(t *testing.T) {
	r := NewRegistry()
	r.Upsert(4420157879, 1, "Old", 99)

	var called bool
	s := NewScanner(nil, r, config.Settings{}, 1, zap.NewNop())
	s.SetOnLiveMetadataUpdated(func(chatID int64, chat tg.ChatClass) {
		called = true
	})

	ch := &tg.Channel{ID: 4420157879, Title: "New"}
	s.refreshLiveMetadataIfActive(4420157879, ch)
	if !called {
		t.Fatal("expected metadata callback")
	}
	entry, ok := r.SnapshotByChat(4420157879)
	if !ok || entry.Title != "New" {
		t.Fatalf("title = %q", entry.Title)
	}

	called = false
	s.refreshLiveMetadataIfActive(999, ch)
	if called {
		t.Fatal("inactive chat should not refresh")
	}
}
