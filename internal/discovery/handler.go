package discovery

import (
	"context"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"go.uber.org/zap"
)

// Register wires Telegram update handlers for live discovery.
func (s *Scanner) Register(d *tg.UpdateDispatcher) {
	d.OnGroupCall(s.onGroupCall)
	d.OnChannel(s.onChannel)
	d.OnNewChannelMessage(s.onNewChannelMessage)
	d.OnChannelParticipant(s.onChannelParticipant)
}

func (s *Scanner) onChannelParticipant(ctx context.Context, e tg.Entities, update *tg.UpdateChannelParticipant) error {
	if update.UserID != s.selfUserID {
		return nil
	}
	chatID := update.ChannelID
	prev, _ := update.GetPrevParticipant()
	newP, _ := update.GetNewParticipant()

	switch {
	case channelParticipantJoined(prev, newP):
		return s.onSelfJoined(ctx, chatID, e, "channel participant joined")
	case channelParticipantLeft(prev, newP):
		s.onSelfLeft(chatID, "channel participant left")
	}
	return nil
}

func (s *Scanner) onSelfJoined(ctx context.Context, chatID int64, e tg.Entities, via string) error {
	s.log.Info("watchlist: joined", zap.Int64("chat", chatID), zap.String("via", via))
	chat, err := s.chatFromEntities(ctx, chatID, e)
	if err != nil {
		if wait, ok := tgerr.AsFloodWait(err); ok {
			s.noteFloodWait(wait)
			return err
		}
		s.log.Debug("joined chat entity lookup failed", zap.Int64("chat", chatID), zap.Error(err))
		s.mu.Lock()
		s.memberChats[chatID] = struct{}{}
		s.mu.Unlock()
		return nil
	}
	if chat == nil || isBasicGroup(chat) {
		s.mu.Lock()
		s.memberChats[chatID] = struct{}{}
		s.mu.Unlock()
		return nil
	}
	return s.applyEntityMembership(ctx, chatID, chat, "")
}

func (s *Scanner) onSelfLeft(chatID int64, via string) {
	s.log.Info("watchlist: left", zap.Int64("chat", chatID), zap.String("via", via))
	s.dropMembership(chatID)
	s.removeLiveForChat(chatID, via)
}

func (s *Scanner) chatFromEntities(ctx context.Context, chatID int64, e tg.Entities) (tg.ChatClass, error) {
	if ch, ok := e.Channels[chatID]; ok {
		s.rememberChannel(ch)
		return ch, nil
	}
	return s.fetchChatEntity(ctx, chatID)
}

func (s *Scanner) onGroupCall(ctx context.Context, _ tg.Entities, update *tg.UpdateGroupCall) error {
	switch call := update.Call.(type) {
	case *tg.GroupCallDiscarded:
		chatID := s.resolveChatID(update, call.ID)
		if chatID == 0 {
			s.removeLiveByCallID(call.ID, "group call discarded")
			return nil
		}
		s.removeLiveForChat(chatID, "group call discarded")
		return nil
	case *tg.GroupCall:
		if call.ScheduleDate != 0 {
			return nil
		}
		chatID := s.resolveChatID(update, call.ID)
		if chatID == 0 {
			s.scheduleProbeGroupCall(call)
			return nil
		}
		s.mu.Lock()
		s.seenCalls[callKey{chatID, call.ID}] = struct{}{}
		s.callToChat[call.ID] = chatID
		s.callIndicator[chatID] = true
		s.memberChats[chatID] = struct{}{}
		s.mu.Unlock()

		title := chatTitle(nil, chatID)
		if existing, ok := s.registry.SnapshotByChat(chatID); ok {
			title = mergeTitle(existing.Title, title)
		}
		if chat, err := s.fetchChatEntity(ctx, chatID); err == nil && chat != nil {
			title = mergeTitle(title, chatTitle(chat, chatID))
		} else if existing, ok := s.registry.SnapshotByChat(chatID); ok {
			title = existing.Title
		}

		s.upsertLive(chatID, call.ID, title, call.AccessHash, true)
	}
	return nil
}

func (s *Scanner) onChannel(ctx context.Context, e tg.Entities, update *tg.UpdateChannel) error {
	chatID := update.ChannelID
	s.log.Debug("UpdateChannel", zap.Int64("chat", chatID))

	chat, err := s.loadChatEntity(ctx, chatID, e)
	if err != nil {
		if isChannelPrivate(err) {
			s.dropMembership(chatID)
			s.removeLiveForChat(chatID, "channel became private after leave")
			return nil
		}
		if wait, ok := tgerr.AsFloodWait(err); ok {
			s.noteFloodWait(wait)
			return err
		}
		s.log.Debug("UpdateChannel entity lookup failed", zap.Int64("chat", chatID), zap.Error(err))
		return nil
	}
	if chat == nil || isBasicGroup(chat) {
		return nil
	}

	return s.applyEntityMembership(ctx, chatID, chat, "")
}

func (s *Scanner) resolveChatID(update *tg.UpdateGroupCall, callID int64) int64 {
	if peer, ok := update.GetPeer(); ok {
		if id, ok := peerChannelID(peer); ok {
			return normalizeChatID(id)
		}
	}
	s.mu.Lock()
	if chatID, ok := s.callToChat[callID]; ok {
		s.mu.Unlock()
		return chatID
	}
	for key := range s.seenCalls {
		if key.callID == callID {
			chatID := key.chatID
			s.mu.Unlock()
			return chatID
		}
	}
	s.mu.Unlock()

	for _, entry := range s.registry.ActiveLives() {
		if entry.CallID == callID {
			return entry.ChatID
		}
	}
	return 0
}

func isChannelPrivate(err error) bool {
	if err == nil {
		return false
	}
	return tgerr.Is(err, "CHANNEL_PRIVATE")
}
