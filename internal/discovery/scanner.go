package discovery

import (
	"context"
	"sync"
	"time"

	"github.com/gotd/td/telegram/query"
	"github.com/gotd/td/telegram/query/dialogs"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"go.uber.org/zap"

	"github.com/aleskxyz/tgtv/internal/config"
)

// LiveEnded is invoked when a live stream is removed from the registry.
type LiveEnded func(streamID string)

// LiveDiscovered is invoked when a new live entry is upserted.
type LiveDiscovered func(chatID int64)

// ChannelSeen is invoked when a channel entity is observed (for thumbnail access hashes).
type ChannelSeen func(ch *tg.Channel)

// LiveMetadataUpdated is invoked when title or photo may have changed for an active live entry.
type LiveMetadataUpdated func(chatID int64, chat tg.ChatClass)

// CallSuperseded is invoked when an ingesting stream gets a new group call ID for the same chat.
type CallSuperseded func(streamID string)

// Scanner discovers active group-call broadcasts and keeps the registry in sync via scans and updates.
type Scanner struct {
	api        *tg.Client
	registry   *Registry
	cfg        config.Settings
	log        *zap.Logger
	selfUserID int64

	onLiveEnded           LiveEnded
	onLiveDiscovered      LiveDiscovered
	onChannelSeen         ChannelSeen
	onLiveMetadataUpdated LiveMetadataUpdated
	onCallSuperseded      CallSuperseded

	mu                sync.Mutex
	memberChats       map[int64]struct{}
	callIndicator     map[int64]bool
	callToChat        map[int64]int64
	seenCalls         map[callKey]struct{}
	probingCalls      map[int64]struct{}
	floodBlockedUntil time.Time

	cancel      context.CancelFunc
	done        chan struct{}
	runCtx      context.Context
	scanMu      sync.Mutex
	scanCancel  context.CancelFunc
	scanPending bool
}

func NewScanner(api *tg.Client, registry *Registry, cfg config.Settings, selfUserID int64, log *zap.Logger) *Scanner {
	return &Scanner{
		api:           api,
		registry:      registry,
		cfg:           cfg,
		selfUserID:    selfUserID,
		log:           log.Named("scanner"),
		memberChats:   make(map[int64]struct{}),
		callIndicator: make(map[int64]bool),
		callToChat:    make(map[int64]int64),
		seenCalls:     make(map[callKey]struct{}),
		done:          make(chan struct{}),
	}
}

func (s *Scanner) SetOnLiveEnded(fn LiveEnded) {
	s.onLiveEnded = fn
}

func (s *Scanner) SetOnLiveDiscovered(fn LiveDiscovered) {
	s.onLiveDiscovered = fn
}

func (s *Scanner) SetOnChannelSeen(fn ChannelSeen) {
	s.onChannelSeen = fn
}

func (s *Scanner) SetOnLiveMetadataUpdated(fn LiveMetadataUpdated) {
	s.onLiveMetadataUpdated = fn
}

func (s *Scanner) SetOnCallSuperseded(fn CallSuperseded) {
	s.onCallSuperseded = fn
}

func (s *Scanner) Start(ctx context.Context) {
	ctx, s.cancel = context.WithCancel(ctx)
	s.runCtx = ctx
	go s.loop(ctx)
}

func (s *Scanner) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	s.cancelActiveScan()
	<-s.done
}

func (s *Scanner) loop(ctx context.Context) {
	defer close(s.done)

	s.log.Info("bootstrap discovery scan")
	s.scheduleFullScan()

	ticker := time.NewTicker(time.Duration(s.cfg.FullScanIntervalSeconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.log.Info("hourly safety full scan")
			s.scheduleFullScan()
		}
	}
}

func (s *Scanner) noteFloodWait(wait time.Duration) {
	backoff := wait + time.Second
	s.mu.Lock()
	newUntil := time.Now().Add(backoff)
	scheduleRescan := newUntil.After(s.floodBlockedUntil)
	s.floodBlockedUntil = newUntil
	s.mu.Unlock()
	if scheduleRescan && s.runCtx != nil {
		go func(d time.Duration) {
			timer := time.NewTimer(d)
			defer timer.Stop()
			select {
			case <-s.runCtx.Done():
			case <-timer.C:
				s.log.Debug("resuming scan after flood wait")
				s.scheduleFullScan()
			}
		}(backoff)
	}
}

func (s *Scanner) floodBlocked() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return time.Now().Before(s.floodBlockedUntil)
}

func (s *Scanner) scheduleFullScan() {
	if s.floodBlocked() {
		s.log.Debug("deferring scan: flood-wait backoff active")
		return
	}

	s.scanMu.Lock()
	defer s.scanMu.Unlock()
	if s.scanCancel != nil {
		s.scanPending = true
		s.log.Debug("full scan already running, queued follow-up")
		return
	}

	scanCtx, cancel := context.WithCancel(context.Background())
	s.scanCancel = cancel

	go func() {
		defer func() {
			s.scanMu.Lock()
			s.scanCancel = nil
			pending := s.scanPending
			s.scanPending = false
			s.scanMu.Unlock()
			if pending {
				s.scheduleFullScan()
			}
		}()

		err := s.runFullScan(scanCtx)
		if err == context.Canceled {
			s.log.Debug("full scan cancelled")
			return
		}
		if wait, ok := tgerr.AsFloodWait(err); ok {
			s.noteFloodWait(wait)
			s.log.Warn("full scan hit flood wait", zap.Duration("wait", wait))
			return
		}
		if err != nil {
			s.log.Warn("full scan failed", zap.Error(err))
		}
	}()
}

func (s *Scanner) cancelActiveScan() {
	s.scanMu.Lock()
	defer s.scanMu.Unlock()
	if s.scanCancel != nil {
		s.scanCancel()
	}
}

func (s *Scanner) runFullScan(ctx context.Context) error {
	seenChats := make(map[int64]struct{})
	activeChats := make(map[int64]struct{})
	probed := 0
	skipped := 0
	scanComplete := true

	iter := query.GetDialogs(s.api).BatchSize(100).Iter()
	for iter.Next(ctx) {
		elem := iter.Value()
		dialog, ok := elem.Dialog.(*tg.Dialog)
		if !ok {
			continue
		}

		chat := s.chatFromDialog(elem, dialog)
		if chat == nil {
			continue
		}
		chatID := ChatIDOf(chat)
		if chatID == 0 {
			continue
		}
		seenChats[chatID] = struct{}{}
		s.rememberChat(chat)

		if chatMembershipLost(chat) {
			s.removeLiveForChat(chatID, "left channel or lost access")
			skipped++
			continue
		}

		s.mu.Lock()
		s.memberChats[chatID] = struct{}{}
		newIndicator := hasActiveCallIndicator(chat)
		oldIndicator := s.callIndicator[chatID]
		s.callIndicator[chatID] = newIndicator
		s.mu.Unlock()

		switch {
		case newIndicator:
			probed++
			if err := s.probeEntityForLive(ctx, chatID, chat, activeChats, ""); err != nil {
				if wait, ok := tgerr.AsFloodWait(err); ok {
					s.noteFloodWait(wait)
					s.log.Warn("flood wait during full scan", zap.Duration("wait", wait))
					scanComplete = false
					break
				}
				return err
			}
		case oldIndicator:
			entry, ok := s.registry.SnapshotByChat(chatID)
			if ok {
				switch s.verifyCallLive(ctx, entry.CallID, entry.CallAccessHash) {
				case CallLiveYes, CallLiveUnknown:
					break
				case CallLiveNo:
					s.removeLiveForChat(chatID, "call indicators cleared during full scan")
				}
			}
		}

		time.Sleep(time.Duration(s.cfg.ScanDialogDelaySeconds * float64(time.Second)))
	}

	if err := iter.Err(); err != nil {
		if wait, ok := tgerr.AsFloodWait(err); ok {
			s.noteFloodWait(wait)
			s.log.Warn("flood wait listing dialogs", zap.Duration("wait", wait))
			return nil
		}
		return err
	}

	if !scanComplete {
		s.log.Debug("full scan incomplete, skipping membership cleanup",
			zap.Int("seen", len(seenChats)),
			zap.Int("probed", probed),
		)
		return nil
	}

	var staleMembers []int64
	s.mu.Lock()
	for chatID := range s.memberChats {
		if _, ok := seenChats[chatID]; !ok {
			s.dropMembershipLocked(chatID)
			if _, live := s.registry.SnapshotByChat(chatID); live {
				staleMembers = append(staleMembers, chatID)
			}
		}
	}
	s.mu.Unlock()
	for _, chatID := range staleMembers {
		s.removeLiveForChat(chatID, "no longer in dialogs")
	}

	for _, entry := range s.registry.ActiveLives() {
		if _, ok := activeChats[entry.ChatID]; !ok {
			s.mu.Lock()
			_, member := s.memberChats[entry.ChatID]
			s.mu.Unlock()
			if !member {
				continue
			}
			switch s.verifyCallLive(ctx, entry.CallID, entry.CallAccessHash) {
			case CallLiveYes:
				continue
			case CallLiveUnknown:
				s.log.Debug("skipping full-scan live removal: call verify inconclusive",
					zap.Int64("chat", entry.ChatID),
					zap.Int64("call", entry.CallID),
				)
				continue
			case CallLiveNo:
				s.removeLiveForChat(entry.ChatID, "not live during full scan")
			}
		}
	}

	s.log.Debug("full scan complete",
		zap.Int("watched", len(seenChats)),
		zap.Int("probed", probed),
		zap.Int("skipped", skipped),
		zap.Int("active", len(activeChats)),
	)
	return nil
}

func (s *Scanner) applyEntityMembership(ctx context.Context, chatID int64, chat tg.ChatClass, fallbackName string) error {
	if isBasicGroup(chat) {
		return nil
	}
	if chatMembershipLost(chat) {
		s.dropMembership(chatID)
		s.removeLiveForChat(chatID, "left channel or lost access")
		return nil
	}

	s.rememberChat(chat)
	s.mu.Lock()
	s.memberChats[chatID] = struct{}{}
	newIndicator := hasActiveCallIndicator(chat)
	oldIndicator := s.callIndicator[chatID]
	s.callIndicator[chatID] = newIndicator
	s.mu.Unlock()

	title := fallbackName
	if title == "" {
		title = chatTitle(chat, chatID)
	}

	switch {
	case newIndicator && !oldIndicator:
		active := make(map[int64]struct{})
		return s.probeEntityForLive(ctx, chatID, chat, active, title)
	case !newIndicator && oldIndicator:
		entry, ok := s.registry.SnapshotByChat(chatID)
		if ok {
			switch s.verifyCallLive(ctx, entry.CallID, entry.CallAccessHash) {
			case CallLiveYes, CallLiveUnknown:
				break
			case CallLiveNo:
				s.removeLiveForChat(chatID, "call indicators cleared")
			}
		}
	case newIndicator:
		if err := s.reconcileActiveCall(ctx, chatID, chat, title); err != nil {
			return err
		}
	default:
		s.refreshLiveMetadata(chatID, chat, title)
	}
	return nil
}

func (s *Scanner) reconcileActiveCall(ctx context.Context, chatID int64, chat tg.ChatClass, title string) error {
	entry, ok := s.registry.SnapshotByChat(chatID)
	if !ok {
		active := make(map[int64]struct{})
		return s.probeEntityForLive(ctx, chatID, chat, active, title)
	}

	s.refreshLiveMetadata(chatID, chat, title)

	inputCall, live, err := fetchActiveCall(ctx, s.api, chat)
	if err != nil {
		if wait, ok := tgerr.AsFloodWait(err); ok {
			s.noteFloodWait(wait)
		}
		// Title/photo were refreshed above; don't fail the update on transient call API errors.
		return nil
	}
	if !live {
		switch s.verifyCallLive(ctx, entry.CallID, entry.CallAccessHash) {
		case CallLiveYes, CallLiveUnknown:
			return nil
		case CallLiveNo:
			s.removeLiveForChat(chatID, "call ended")
			return nil
		}
	}
	if inputCall.ID != entry.CallID {
		s.mu.Lock()
		s.seenCalls[callKey{chatID, inputCall.ID}] = struct{}{}
		s.callToChat[inputCall.ID] = chatID
		s.mu.Unlock()
		if title == "" {
			title = chatTitle(chat, chatID)
		}
		s.upsertLive(chatID, inputCall.ID, title, inputCall.AccessHash, true)
		return nil
	}
	return nil
}

func (s *Scanner) refreshLiveMetadata(chatID int64, chat tg.ChatClass, title string) {
	if _, ok := s.registry.SnapshotByChat(chatID); !ok {
		return
	}
	if title == "" {
		title = chatTitle(chat, chatID)
	}
	if s.registry.UpdateTitle(chatID, title) {
		s.log.Info("live title updated",
			zap.Int64("chat", chatID),
			zap.String("title", title),
		)
	}
	if s.onLiveMetadataUpdated != nil {
		s.onLiveMetadataUpdated(chatID, chat)
	}
}

func (s *Scanner) probeEntityForLive(ctx context.Context, chatID int64, chat tg.ChatClass, activeChats map[int64]struct{}, fallbackName string) error {
	inputCall, ok, err := fetchActiveCall(ctx, s.api, chat)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	activeChats[chatID] = struct{}{}
	s.mu.Lock()
	s.seenCalls[callKey{chatID, inputCall.ID}] = struct{}{}
	s.callToChat[inputCall.ID] = chatID
	s.mu.Unlock()

	title := fallbackName
	if title == "" {
		title = chatTitle(chat, chatID)
	}
	s.upsertLive(chatID, inputCall.ID, title, inputCall.AccessHash, true)
	s.refreshLiveMetadata(chatID, chat, title)
	return nil
}

func (s *Scanner) verifyCallLive(ctx context.Context, callID, accessHash int64) CallLiveStatus {
	status := VerifyCallStillLive(ctx, s.api, callID, accessHash)
	if status == CallLiveUnknown {
		s.log.Debug("failed to verify group call",
			zap.Int64("call", callID),
		)
	}
	return status
}

func (s *Scanner) upsertLive(chatID, callID int64, title string, accessHash int64, verified bool) {
	if !verified && accessHash != 0 {
		verifyCtx := context.Background()
		if s.runCtx != nil {
			var cancel context.CancelFunc
			verifyCtx, cancel = context.WithTimeout(s.runCtx, 15*time.Second)
			defer cancel()
		}
		switch VerifyCallStillLive(verifyCtx, s.api, callID, accessHash) {
		case CallLiveNo:
			s.log.Debug("skipping upsert: call not live",
				zap.Int64("chat", chatID),
				zap.Int64("call", callID),
			)
			return
		case CallLiveUnknown, CallLiveYes:
		}
	}

	prev, hadPrev := s.registry.SnapshotByChat(chatID)
	entry, callSuperseded := s.registry.Upsert(chatID, callID, title, accessHash)
	isNew := !hadPrev || (hadPrev && prev.Status == StatusEnded)
	if isNew || callSuperseded {
		s.log.Info("discovered live",
			zap.String("stream", entry.StreamID),
			zap.String("title", entry.Title),
			zap.Int64("chat", chatID),
		)
	}
	if callSuperseded && s.onCallSuperseded != nil {
		s.onCallSuperseded(entry.StreamID)
	}
	if isNew && s.onLiveDiscovered != nil {
		s.onLiveDiscovered(chatID)
	}
}

func (s *Scanner) removeLiveByCallID(callID int64, reason string) {
	for _, entry := range s.registry.ActiveLives() {
		if entry.CallID == callID {
			s.removeLiveForChat(entry.ChatID, reason)
			return
		}
	}
}

func (s *Scanner) fetchChatEntity(ctx context.Context, chatID int64) (tg.ChatClass, error) {
	return fetchChatEntity(ctx, s.api, s.registry, chatID)
}

func (s *Scanner) removeLiveForChat(chatID int64, reason string) {
	s.mu.Lock()
	for key := range s.seenCalls {
		if key.chatID == chatID {
			delete(s.seenCalls, key)
		}
	}
	for callID, mapped := range s.callToChat {
		if mapped == chatID {
			delete(s.callToChat, callID)
		}
	}
	s.mu.Unlock()

	entry, ended := s.registry.MarkEnded(chatID)
	if !ended {
		return
	}
	s.log.Info("removing live",
		zap.Int64("chat", chatID),
		zap.String("stream", entry.StreamID),
		zap.String("reason", reason),
	)
	if s.onLiveEnded != nil {
		s.onLiveEnded(entry.StreamID)
	}
	s.registry.RemoveEndedChat(chatID)
}

func (s *Scanner) dropMembership(chatID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropMembershipLocked(chatID)
}

func (s *Scanner) dropMembershipLocked(chatID int64) {
	delete(s.memberChats, chatID)
	delete(s.callIndicator, chatID)
	for callID, mapped := range s.callToChat {
		if mapped == chatID {
			delete(s.callToChat, callID)
		}
	}
	for key := range s.seenCalls {
		if key.chatID == chatID {
			delete(s.seenCalls, key)
		}
	}
	s.registry.ForgetChannelAccess(chatID)
}

func (s *Scanner) rememberChat(chat tg.ChatClass) {
	if ch, ok := chat.(*tg.Channel); ok {
		s.rememberChannel(ch)
	}
}

func (s *Scanner) rememberChannel(ch *tg.Channel) {
	s.registry.RememberChannelAccess(ch.ID, ch.AccessHash)
	if s.onChannelSeen != nil {
		s.onChannelSeen(ch)
	}
}

func (s *Scanner) chatFromDialog(elem dialogs.Elem, dialog *tg.Dialog) tg.ChatClass {
	p, ok := dialog.Peer.(*tg.PeerChannel)
	if !ok {
		return nil
	}
	if ch, ok := elem.Entities.Channel(p.ChannelID); ok {
		return ch
	}
	return nil
}

func (s *Scanner) scheduleProbeGroupCall(call *tg.GroupCall) {
	if s.runCtx == nil || call == nil {
		return
	}
	s.mu.Lock()
	if s.probingCalls == nil {
		s.probingCalls = make(map[int64]struct{})
	}
	if _, ok := s.probingCalls[call.ID]; ok {
		s.mu.Unlock()
		return
	}
	s.probingCalls[call.ID] = struct{}{}
	s.mu.Unlock()

	go func() {
		defer func() {
			s.mu.Lock()
			delete(s.probingCalls, call.ID)
			s.mu.Unlock()
		}()
		ctx, cancel := context.WithTimeout(s.runCtx, 60*time.Second)
		defer cancel()
		s.probeGroupCall(ctx, call)
	}()
}

func (s *Scanner) probeGroupCall(ctx context.Context, call *tg.GroupCall) {
	s.mu.Lock()
	members := make([]int64, 0, len(s.memberChats))
	for chatID := range s.memberChats {
		members = append(members, chatID)
	}
	s.mu.Unlock()

	for _, chatID := range members {
		if err := ctx.Err(); err != nil {
			return
		}
		chat, err := s.fetchChatEntity(ctx, chatID)
		if err != nil || chat == nil {
			continue
		}
		inputCall, live, err := fetchActiveCall(ctx, s.api, chat)
		if err != nil || !live || inputCall.ID != call.ID {
			continue
		}

		s.mu.Lock()
		s.seenCalls[callKey{chatID, call.ID}] = struct{}{}
		s.callToChat[call.ID] = chatID
		s.callIndicator[chatID] = true
		s.memberChats[chatID] = struct{}{}
		s.mu.Unlock()

		title := chatTitle(chat, chatID)
		s.upsertLive(chatID, call.ID, title, call.AccessHash, true)
		s.log.Info("resolved group call via member probe",
			zap.Int64("chat", chatID),
			zap.Int64("call", call.ID),
		)
		return
	}
}
