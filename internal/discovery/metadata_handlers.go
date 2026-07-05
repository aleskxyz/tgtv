package discovery

import (
	"context"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"go.uber.org/zap"
)

// loadChatEntity mirrors Telegram Android's loadFullChat on updateChannel:
// always fetch a fresh channel entity from the API; only use the update batch for access hashes.
func (s *Scanner) loadChatEntity(ctx context.Context, chatID int64, e tg.Entities) (tg.ChatClass, error) {
	if ch, ok := e.Channels[chatID]; ok {
		s.rememberChannel(ch)
	}
	return s.fetchChatEntity(ctx, chatID)
}

func (s *Scanner) refreshLiveMetadataIfActive(chatID int64, chat tg.ChatClass) {
	if chat == nil || isBasicGroup(chat) {
		return
	}
	if _, ok := s.registry.SnapshotByChat(chatID); !ok {
		return
	}
	s.refreshLiveMetadata(chatID, chat, chatTitle(chat, chatID))
}

func (s *Scanner) onNewChannelMessage(ctx context.Context, e tg.Entities, update *tg.UpdateNewChannelMessage) error {
	chatID, action, ok := metadataChangeFromService(update.Message, peerChannelID)
	if !ok {
		return nil
	}
	return s.handleMetadataServiceMessage(ctx, e, chatID, action, "channel")
}

func (s *Scanner) handleMetadataServiceMessage(ctx context.Context, e tg.Entities, chatID int64, action, kind string) error {
	s.log.Debug(kind+" metadata service message",
		zap.Int64("chat", chatID),
		zap.String("action", action),
	)
	chat, err := s.chatFromEntities(ctx, chatID, e)
	if err != nil {
		if wait, ok := tgerr.AsFloodWait(err); ok {
			s.noteFloodWait(wait)
			return err
		}
		return nil
	}
	if chat == nil {
		chat, err = s.fetchChatEntity(ctx, chatID)
		if err != nil {
			if wait, ok := tgerr.AsFloodWait(err); ok {
				s.noteFloodWait(wait)
				return err
			}
			return nil
		}
	}
	s.refreshLiveMetadataIfActive(chatID, chat)
	return nil
}

func metadataChangeFromService(msg tg.MessageClass, peerID func(tg.PeerClass) (int64, bool)) (chatID int64, action string, ok bool) {
	svc, ok := msg.(*tg.MessageService)
	if !ok {
		return 0, "", false
	}
	actionName, changed := metadataAction(svc.Action)
	if !changed {
		return 0, "", false
	}
	chatID, ok = peerID(svc.PeerID)
	if !ok {
		return 0, "", false
	}
	return chatID, actionName, true
}

func metadataAction(action tg.MessageActionClass) (name string, changed bool) {
	switch action.(type) {
	case *tg.MessageActionChatEditPhoto:
		return "chat_edit_photo", true
	case *tg.MessageActionChatEditTitle:
		return "chat_edit_title", true
	default:
		return "", false
	}
}

func peerChannelID(peer tg.PeerClass) (int64, bool) {
	if p, ok := peer.(*tg.PeerChannel); ok {
		return p.ChannelID, true
	}
	return 0, false
}
