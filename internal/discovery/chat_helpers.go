package discovery

import (
	"fmt"

	"github.com/gotd/td/tg"
)

// normalizeChatID converts Telegram mark peer IDs to the positive ID used in the registry.
func normalizeChatID(id int64) int64 {
	if id < -1_000_000_000_000 {
		return -(id + 1_000_000_000_000)
	}
	if id < 0 {
		return -id
	}
	return id
}

func isBasicGroup(chat tg.ChatClass) bool {
	_, ok := chat.(*tg.Chat)
	return ok
}

func hasActiveCallIndicator(chat tg.ChatClass) bool {
	ch, ok := chat.(*tg.Channel)
	if !ok {
		return false
	}
	return ch.CallActive && ch.CallNotEmpty
}

func chatMembershipLost(chat tg.ChatClass) bool {
	ch, ok := chat.(*tg.Channel)
	if !ok {
		return true
	}
	return ch.Left
}

func chatTitle(chat tg.ChatClass, chatID int64) string {
	if ch, ok := chat.(*tg.Channel); ok && ch.Title != "" {
		return ch.Title
	}
	return fmt.Sprintf("Chat %d", chatID)
}

// PeerDialogID converts a positive registry chat ID to Telegram dialog ID
// (DialogObject.getPeerDialogId / LivePlayer.dialogId convention).
func PeerDialogID(chatID int64) int64 {
	if chatID <= 0 {
		return chatID
	}
	return -chatID
}

// ChatIDOf returns the positive channel ID for a supported entity.
func ChatIDOf(chat tg.ChatClass) int64 {
	if ch, ok := chat.(*tg.Channel); ok {
		return ch.ID
	}
	return 0
}

func isLiveGroupCall(call tg.GroupCallClass) bool {
	switch c := call.(type) {
	case *tg.GroupCallDiscarded:
		return false
	case *tg.GroupCall:
		return c.ScheduleDate == 0
	default:
		return false
	}
}

func inputGroupCallClass(call tg.InputGroupCallClass) (tg.InputGroupCall, bool) {
	if gc, ok := call.(*tg.InputGroupCall); ok {
		return *gc, true
	}
	return tg.InputGroupCall{}, false
}

func inputGroupCall(call tg.GroupCallClass) (tg.InputGroupCall, bool) {
	if gc, ok := call.(*tg.GroupCall); ok {
		return tg.InputGroupCall{ID: gc.ID, AccessHash: gc.AccessHash}, true
	}
	return tg.InputGroupCall{}, false
}

type callKey struct {
	chatID int64
	callID int64
}
