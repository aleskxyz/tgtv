package ingest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/tg"
	"go.uber.org/zap"

	"github.com/aleskxyz/tgtv/internal/config"
	"github.com/aleskxyz/tgtv/internal/discovery"
	"github.com/aleskxyz/tgtv/internal/publish"
	"github.com/aleskxyz/tgtv/internal/segment"
	"github.com/aleskxyz/tgtv/internal/stream"
	"github.com/aleskxyz/tgtv/internal/thumbnails"
)

type Supervisor struct {
	mt         *stream.MTProto
	self       *tg.User
	registry   *discovery.Registry
	thumbnails *thumbnails.Store
	cfg        config.Settings
	log        *zap.Logger
	runCtx     context.Context

	mu                   sync.Mutex
	shuttingDown         bool
	sessions             map[string]*Session
	starting             map[string]struct{}
	restarting           map[string]struct{}
	restartPending       map[string]struct{}
	recoveryBlockedUntil map[string]time.Time
	startCancels         map[string]context.CancelFunc
	lastStartErr         map[string]error
	lastIngestStart      time.Time
}

func NewSupervisor(mt *stream.MTProto, self *tg.User, registry *discovery.Registry, thumbs *thumbnails.Store, cfg config.Settings, runCtx context.Context, log *zap.Logger) *Supervisor {
	return &Supervisor{
		mt:                   mt,
		self:                 self,
		registry:             registry,
		thumbnails:           thumbs,
		cfg:                  cfg,
		runCtx:               runCtx,
		log:                  log.Named("ingest"),
		sessions:             make(map[string]*Session),
		starting:             make(map[string]struct{}),
		restarting:           make(map[string]struct{}),
		restartPending:       make(map[string]struct{}),
		recoveryBlockedUntil: make(map[string]time.Time),
		startCancels:         make(map[string]context.CancelFunc),
		lastStartErr:         make(map[string]error),
	}
}

func (s *Supervisor) IsIngesting(streamID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.sessions[streamID]
	_, starting := s.starting[streamID]
	return ok || starting
}

func (s *Supervisor) IsReady(ctx context.Context, streamID string) bool {
	s.mu.Lock()
	sess := s.sessions[streamID]
	s.mu.Unlock()
	if sess == nil {
		return false
	}
	return sess.publisher.IsReady(ctx)
}

func (s *Supervisor) ConsumeHLSDiscontinuity(streamID string) bool {
	s.mu.Lock()
	sess := s.sessions[streamID]
	s.mu.Unlock()
	if sess == nil {
		return false
	}
	return sess.publisher.ConsumeDiscontinuity()
}

func (s *Supervisor) ShouldHoldHLS(streamID string) bool {
	s.mu.Lock()
	sess := s.sessions[streamID]
	s.mu.Unlock()
	if sess == nil {
		return false
	}
	return sess.shouldHoldHLS()
}

func (s *Supervisor) IsRecoveryFailed(streamID string) bool {
	if s.recoveryBlocked(streamID) {
		return true
	}
	s.mu.Lock()
	sess := s.sessions[streamID]
	s.mu.Unlock()
	if sess == nil {
		return false
	}
	return sess.isRecoveryFailed()
}

func (s *Supervisor) recoveryBlocked(streamID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	until, ok := s.recoveryBlockedUntil[streamID]
	if !ok {
		return false
	}
	if time.Now().After(until) {
		delete(s.recoveryBlockedUntil, streamID)
		return false
	}
	return true
}

func (s *Supervisor) noteRecoveryFailure(streamID string) {
	s.mu.Lock()
	s.recoveryBlockedUntil[streamID] = time.Now().Add(recoveryRetryCooldown)
	s.mu.Unlock()
}

func (s *Supervisor) activeIngestCountLocked() int {
	seen := make(map[string]struct{}, len(s.sessions)+len(s.starting))
	for id := range s.sessions {
		seen[id] = struct{}{}
	}
	for id := range s.starting {
		seen[id] = struct{}{}
	}
	return len(seen)
}

func (s *Supervisor) EnsureIngest(ctx context.Context, streamID string) error {
	_ = ctx // ingest lifetime is runCtx; ctx only cancels concurrent waiters

	if s.recoveryBlocked(streamID) {
		return ErrRecoveryFailed
	}

	s.mu.Lock()
	if s.shuttingDown {
		s.mu.Unlock()
		return fmt.Errorf("supervisor shutting down")
	}
	if sess, ok := s.sessions[streamID]; ok {
		s.mu.Unlock()
		return sess.waitBootstrap(ctx)
	}
	if _, ok := s.starting[streamID]; ok {
		s.mu.Unlock()
		return s.waitIngestOutcome(ctx, streamID)
	}
	if s.activeIngestCountLocked() >= s.cfg.MaxConcurrentIngests {
		s.mu.Unlock()
		return ErrMaxConcurrentIngests
	}
	s.starting[streamID] = struct{}{}
	startCtx, startCancel := context.WithCancel(s.runCtx)
	s.startCancels[streamID] = startCancel
	s.mu.Unlock()

	err := classifyIngestError(s.startIngest(startCtx, streamID))

	s.mu.Lock()
	delete(s.starting, streamID)
	bootstrapCancel := s.startCancels[streamID]
	delete(s.startCancels, streamID)
	if err != nil {
		s.lastStartErr[streamID] = err
	} else {
		delete(s.lastStartErr, streamID)
	}
	s.mu.Unlock()
	if bootstrapCancel != nil {
		bootstrapCancel()
	}
	return err
}

func (s *Supervisor) waitIngestOutcome(ctx context.Context, streamID string) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		s.mu.Lock()
		_, starting := s.starting[streamID]
		sess := s.sessions[streamID]
		err := s.lastStartErr[streamID]
		s.mu.Unlock()
		if !starting {
			if err != nil {
				return err
			}
			if sess != nil {
				return sess.waitBootstrap(ctx)
			}
			return ErrIngestNotRunning
		}
	}
}

func (s *Supervisor) waitStartingDone(streamID string) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		s.mu.Lock()
		_, ok := s.starting[streamID]
		s.mu.Unlock()
		if !ok {
			return
		}
		<-ticker.C
	}
}

func (s *Supervisor) startIngest(ctx context.Context, streamID string) error {
	if err := s.waitIngestStagger(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	refreshCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	entry, err := discovery.RefreshLiveEntry(refreshCtx, s.mt.Default, s.registry, streamID)
	if err != nil {
		var ended *discovery.StreamEndedError
		if errors.As(err, &ended) {
			s.registry.RemoveEndedChat(ended.ChatID)
		}
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if s.thumbnails != nil {
		prefetchCtx, prefetchCancel := context.WithTimeout(ctx, 10*time.Second)
		_, _ = s.thumbnails.GetJPEG(prefetchCtx, entry.ChatID)
		prefetchCancel()
	}
	logoPath := ""
	if s.thumbnails != nil {
		logoPath = s.thumbnails.LogoPath(entry.ChatID)
	}

	call := tg.InputGroupCall{ID: entry.CallID, AccessHash: entry.CallAccessHash}
	rtmpURL := fmt.Sprintf("%s/%s", s.cfg.RTMPBaseURL, streamID)
	if n := publish.KillStaleRTMPPublishers(rtmpURL, 0); n > 0 {
		s.log.Info("cleaned stale rtmp publishers before ingest",
			zap.String("stream", streamID),
			zap.Int("killed", n),
		)
	}
	ingestCtx, cancel := context.WithCancel(s.runCtx)
	sess := newSession(streamID, entry.ChatID, call, logoPath, s)
	sess.cancel = cancel
	s.mu.Lock()
	if _, exists := s.sessions[streamID]; exists {
		s.mu.Unlock()
		cancel()
		return nil
	}
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		cancel()
		return err
	}
	s.sessions[streamID] = sess
	delete(s.starting, streamID)
	s.mu.Unlock()

	go sess.run(ingestCtx)

	const bootstrapTimeout = 45 * time.Second
	select {
	case <-sess.bootstrapDone:
		if err := sess.bootstrapErr(); err != nil {
			if sess.cancel != nil {
				sess.cancel()
			}
			<-sess.done
			return err
		}
	case <-time.After(bootstrapTimeout):
		if sess.cancel != nil {
			sess.cancel()
		}
		<-sess.done
		return ErrIngestBootstrapTimeout
	case <-ctx.Done():
		if sess.cancel != nil {
			sess.cancel()
		}
		<-sess.done
		return ctx.Err()
	}

	s.registry.SetStatus(streamID, discovery.StatusIngesting)
	s.log.Info("ingest started", zap.String("stream", streamID), zap.Int64("chat", entry.ChatID))
	return nil
}

func (s *Supervisor) StopIngest(streamID string) {
	s.mu.Lock()
	if cancel, ok := s.startCancels[streamID]; ok {
		s.mu.Unlock()
		cancel()
		s.waitStartingDone(streamID)
		return
	}
	sess, ok := s.sessions[streamID]
	s.mu.Unlock()
	if !ok {
		return
	}
	if sess.cancel != nil {
		sess.cancel()
	}
	<-sess.done
}

func (s *Supervisor) removeSession(streamID string, recoveryFailed bool) {
	s.mu.Lock()
	delete(s.sessions, streamID)
	delete(s.lastStartErr, streamID)
	s.mu.Unlock()
	rtmpURL := fmt.Sprintf("%s/%s", s.cfg.RTMPBaseURL, streamID)
	if n := publish.KillStaleRTMPPublishers(rtmpURL, 0); n > 0 {
		s.log.Info("cleaned stale rtmp publishers after session end",
			zap.String("stream", streamID),
			zap.Int("killed", n),
		)
	}
	if recoveryFailed {
		return
	}
	if _, ok := s.registry.Get(streamID); ok {
		s.registry.SetStatus(streamID, discovery.StatusDiscovered)
	}
}

func (s *Supervisor) waitIngestStagger(ctx context.Context) error {
	stagger := time.Duration(s.cfg.IngestStartStaggerSeconds * float64(time.Second))
	if stagger <= 0 {
		return nil
	}
	s.mu.Lock()
	wait := stagger - time.Since(s.lastIngestStart)
	s.mu.Unlock()
	if wait > 0 {
		s.log.Debug("ingest start stagger", zap.Duration("wait", wait))
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	s.mu.Lock()
	s.lastIngestStart = time.Now()
	s.mu.Unlock()
	return nil
}

func (s *Supervisor) ActiveStreamIDs() map[string]struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]struct{}, len(s.sessions))
	for id := range s.sessions {
		out[id] = struct{}{}
	}
	return out
}

func (s *Supervisor) StartStaleProcessCleanup(ctx context.Context) {
	publish.StartStaleCleanup(ctx, s.cfg.RTMPBaseURL, s.ActiveStreamIDs, s.log)
}

func (s *Supervisor) PrepareShutdown() {
	s.mu.Lock()
	s.shuttingDown = true
	s.mu.Unlock()
}

func (s *Supervisor) StopAll() {
	s.PrepareShutdown()
	s.mu.Lock()
	for _, cancel := range s.startCancels {
		cancel()
	}
	starting := make([]string, 0, len(s.starting))
	for id := range s.starting {
		starting = append(starting, id)
	}
	ids := make([]string, 0, len(s.sessions))
	for id := range s.sessions {
		ids = append(ids, id)
	}
	s.mu.Unlock()
	for _, id := range starting {
		s.waitStartingDone(id)
	}
	for _, id := range ids {
		s.StopIngest(id)
	}
}

func (s *Supervisor) isShuttingDown() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.shuttingDown
}

// RestartIngest stops an active ingest and starts again with refreshed call metadata.
// Used when discovery reports a new group call ID for the same chat (LivePlayer recreation).
func (s *Supervisor) RestartIngest(streamID string) {
	go s.restartIngest(streamID)
}

func (s *Supervisor) restartIngest(streamID string) {
	for {
		if s.isShuttingDown() {
			return
		}
		if !s.beginRestart(streamID) {
			return
		}

		s.runRestartCycle(streamID)

		s.mu.Lock()
		pending := false
		if s.restartPending != nil {
			_, pending = s.restartPending[streamID]
			delete(s.restartPending, streamID)
		}
		s.mu.Unlock()

		s.endRestart(streamID)

		if !pending && !s.sessionCallStale(streamID) {
			return
		}
		if pending {
			s.log.Info("restarting ingest again after pending supersession",
				zap.String("stream", streamID),
			)
		} else {
			s.log.Info("restarting ingest again after stale call metadata",
				zap.String("stream", streamID),
			)
		}
	}
}

func (s *Supervisor) beginRestart(streamID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shuttingDown {
		return false
	}
	if s.restarting == nil {
		s.restarting = make(map[string]struct{})
	}
	if _, ok := s.restarting[streamID]; ok {
		if s.restartPending == nil {
			s.restartPending = make(map[string]struct{})
		}
		s.restartPending[streamID] = struct{}{}
		return false
	}
	s.restarting[streamID] = struct{}{}
	return true
}

func (s *Supervisor) endRestart(streamID string) {
	s.mu.Lock()
	delete(s.restarting, streamID)
	s.mu.Unlock()
}

func (s *Supervisor) runRestartCycle(streamID string) {
	if s.IsIngesting(streamID) {
		s.log.Info("restarting ingest for superseded call", zap.String("stream", streamID))
		s.StopIngest(streamID)
	} else {
		s.mu.Lock()
		cancel, starting := s.startCancels[streamID]
		s.mu.Unlock()
		if starting && cancel != nil {
			s.log.Info("cancelling bootstrap for superseded call", zap.String("stream", streamID))
			cancel()
			s.waitStartingDone(streamID)
		} else {
			return
		}
	}
	if err := s.EnsureIngest(s.runCtx, streamID); err != nil {
		s.log.Warn("restart ingest after call supersession failed",
			zap.String("stream", streamID),
			zap.Error(err),
		)
	}
}

func (s *Supervisor) sessionCallStale(streamID string) bool {
	entry, ok := s.registry.Get(streamID)
	if !ok {
		return false
	}
	s.mu.Lock()
	sess := s.sessions[streamID]
	s.mu.Unlock()
	if sess == nil {
		return false
	}
	return sess.call.ID != entry.CallID || (entry.CallAccessHash != 0 && sess.call.AccessHash != entry.CallAccessHash)
}

// RefreshLogoForChat updates the remux overlay logo for any active ingest of chatID.
func (s *Supervisor) RefreshLogoForChat(chatID int64) {
	logoPath := ""
	if s.thumbnails != nil {
		logoPath = s.thumbnails.LogoPath(chatID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sess := range s.sessions {
		if sess.chatID == chatID {
			sess.assembler.SetLogoPath(logoPath)
		}
	}
}

type Session struct {
	streamID           string
	chatID             int64
	call               tg.InputGroupCall
	sup                *Supervisor
	cancel             context.CancelFunc
	done               chan struct{}
	bootstrapDone      chan struct{}
	bootstrapOnce      sync.Once
	bootstrapFailure   error
	statsMu            sync.Mutex
	client             *stream.Client
	scheduler          *stream.Scheduler
	assembler          *segment.Assembler
	publisher          *publish.Publisher
	lastPartAt         time.Time
	lastSegmentAt      time.Time
	segmentsIn         int
	segmentsOut        int
	consumeGen         atomic.Int32
	exitRecoveryFailed atomic.Bool

	recoveryMu            sync.Mutex
	rejoinActive          bool
	joinGraceUntil        time.Time
	floodGraceUntil       time.Time
	lastRejoinAt          time.Time
	getFileJoinMissingAt  time.Time
	lastResyncAt          time.Time
	lastOutputRecoveryAt  time.Time
	outputRecoveryPending bool
	startedAt             time.Time
	unifiedIngest         bool
	rtmpMuxStarted        atomic.Bool
	rejoinCh              chan hardRejoinReq
	rejoinDone            chan error
	recoveryState         recoveryState
	recoveryStartedAt     time.Time
}

type hardRejoinReq struct {
	reason      string
	resetOutput bool
}

func newSession(streamID string, chatID int64, call tg.InputGroupCall, logoPath string, sup *Supervisor) *Session {
	return &Session{
		streamID:      streamID,
		chatID:        chatID,
		call:          call,
		sup:           sup,
		done:          make(chan struct{}),
		bootstrapDone: make(chan struct{}),
		rejoinCh:      make(chan hardRejoinReq, 1),
		rejoinDone:    make(chan error, 1),
		assembler:     segment.NewAssembler(logoPath),
		publisher: publish.NewPublisher(
			streamID,
			sup.cfg.RTMPBaseURL,
			sup.cfg.MediamtxHLSURL,
			sup.cfg.MediamtxHLSCDNSecret,
			sup.log,
		),
	}
}

func (s *Session) bootstrapErr() error {
	return s.bootstrapFailure
}

func (s *Session) waitBootstrap(ctx context.Context) error {
	select {
	case <-s.bootstrapDone:
		return s.bootstrapFailure
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Session) signalBootstrap(err error) {
	s.bootstrapOnce.Do(func() {
		s.bootstrapFailure = err
		close(s.bootstrapDone)
	})
}

func (s *Session) run(ctx context.Context) {
	defer close(s.done)
	defer func() {
		if s.client != nil {
			_ = s.client.Leave(context.Background())
		}
		s.publisher.Stop()
		s.sup.removeSession(s.streamID, s.exitRecoveryFailed.Load())
	}()

	s.client = stream.NewClient(s.sup.mt, s.sup.self, s.call, false)
	s.client.SetDialogID(discovery.PeerDialogID(s.chatID))
	info, err := s.client.FetchCallInfo(ctx)
	if err != nil {
		if stream.IsCallEnded(err) {
			s.sup.registry.MarkEnded(s.chatID)
			s.sup.registry.RemoveEndedChat(s.chatID)
		}
		s.sup.log.Error("fetch call info failed", zap.String("stream", s.streamID), zap.Error(err))
		s.signalBootstrap(err)
		return
	}

	bootstrapSource := "stream_channels"
	if info.Unified {
		bootstrapSource = "last_timestamp_unified"
	}
	s.sup.log.Info("stream mode",
		zap.String("stream", s.streamID),
		zap.Bool("rtmp_stream", info.Unified),
		zap.Bool("unified", info.Unified),
		zap.String("bootstrap", bootstrapSource),
	)
	if info.StreamDCID > 0 {
		s.client.SetStreamDC(info.StreamDCID)
		s.sup.log.Debug("stream DC routing", zap.String("stream", s.streamID), zap.Int("dc", info.StreamDCID))
	}
	s.scheduler = stream.NewScheduler(s.client, s.sup.log)
	s.scheduler.SetEndpointMapper(segment.EndpointMappingFromAudio)
	s.scheduler.SetHooks(stream.SchedulerHooks{
		OnGetFileJoinMissing: s.noteGetFileJoinMissing,
		OnGetFileFloodWait:   s.noteFloodGrace,
		OnResync:             s.onTelegramResync,
	})

	if err := s.joinWithFloodRetry(ctx); err != nil {
		s.sup.log.Error("join failed", zap.String("stream", s.streamID), zap.Error(err))
		s.signalBootstrap(err)
		return
	}
	if err := s.initStreamChannels(ctx, info.Unified); err != nil {
		s.sup.log.Error("init stream channels failed", zap.String("stream", s.streamID), zap.Error(err))
		s.signalBootstrap(err)
		return
	}
	s.unifiedIngest = info.Unified
	s.signalBootstrap(nil)
	s.startedAt = time.Now()

	schedCtx, schedCancel := context.WithCancel(ctx)
	defer schedCancel()
	go s.scheduler.Run(schedCtx)

	partsDone := make(chan struct{})
	go func() {
		defer close(partsDone)
		for part := range s.scheduler.Parts() {
			partsInBefore, _, _ := s.partStats()
			if err := s.publishPart(part); err != nil {
				s.sup.log.Warn("publish failed", zap.String("stream", s.streamID), zap.Error(err))
			} else if s.sup.cfg.Debug() {
				partsIn, _, lastPart := s.partStats()
				if partsIn > partsInBefore {
					s.sup.log.Debug("ingest heartbeat",
						zap.String("stream", s.streamID),
						zap.Int("parts_in", partsIn),
						zap.Int("segments_out", s.segmentsOutLocked()),
						zap.Duration("since_last_part", time.Since(lastPart)),
					)
				}
			}
		}
	}()

	checkHealth := time.NewTicker(stream.CheckGroupCallInterval)
	stallCheck := time.NewTicker(5 * time.Second)
	defer checkHealth.Stop()
	defer stallCheck.Stop()

	for {
		select {
		case <-ctx.Done():
			schedCancel()
			<-partsDone
			return

		case <-s.scheduler.CallEnded():
			s.sup.registry.MarkEnded(s.chatID)
			s.sup.registry.RemoveEndedChat(s.chatID)
			schedCancel()
			<-partsDone
			return

		case <-s.scheduler.RejoinNeeded():
			if stop := s.evalAndApplyRecovery(ctx, triggerGetFileJoinMissing, true); stop {
				schedCancel()
				<-partsDone
				return
			}

		case req := <-s.rejoinCh:
			go func(r hardRejoinReq) {
				err := s.hardRejoin(ctx, r.reason, r.resetOutput)
				select {
				case s.rejoinDone <- err:
				default:
				}
			}(req)

		case err := <-s.rejoinDone:
			if stop := s.handleHardRejoinResult(ctx, err); stop {
				schedCancel()
				<-partsDone
				return
			}

		case <-checkHealth.C:
			if stop := s.evalAndApplyRecovery(ctx, triggerCheckJoin, true); stop {
				schedCancel()
				<-partsDone
				return
			}

		case <-stallCheck.C:
			if s.isInputStalled() {
				_, _, lastPart := s.partStats()
				s.sup.log.Warn("prolonged Telegram input stall; evaluating recovery",
					zap.String("stream", s.streamID),
					zap.Duration("since_last_part", time.Since(lastPart)),
				)
				if stop := s.evalAndApplyRecovery(ctx, triggerInputStall, true); stop {
					schedCancel()
					<-partsDone
					return
				}
				continue
			}
			if s.isOutputStalled() {
				partsIn, partsOut, lastPart := s.partStats()
				lastSeg := s.lastSegmentTime()
				outAge := time.Since(lastSeg)
				if lastSeg.IsZero() {
					outAge = time.Since(lastPart)
				}
				s.sup.log.Warn("output stall; evaluating recovery",
					zap.String("stream", s.streamID),
					zap.Int("parts_in", partsIn),
					zap.Int("segments_out", partsOut),
					zap.Duration("since_last_output", outAge),
				)
				if stop := s.evalAndApplyRecovery(ctx, triggerOutputStall, false); stop {
					schedCancel()
					<-partsDone
					return
				}
			}
		}
	}
}

func (s *Session) notePartReceived() {
	s.statsMu.Lock()
	s.segmentsIn++
	s.lastPartAt = time.Now()
	s.statsMu.Unlock()
}

func (s *Session) noteSegmentOut() {
	s.statsMu.Lock()
	s.segmentsOut++
	s.lastSegmentAt = time.Now()
	s.statsMu.Unlock()
	s.clearRecoveryOnSegment()
}

func (s *Session) partStats() (partsIn, partsOut int, lastPart time.Time) {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	return s.segmentsIn, s.segmentsOut, s.lastPartAt
}

func (s *Session) lastSegmentTime() time.Time {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	return s.lastSegmentAt
}

func (s *Session) segmentsOutLocked() int {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	return s.segmentsOut
}

func (s *Session) resetPartStats() {
	s.statsMu.Lock()
	s.lastPartAt = time.Time{}
	s.lastSegmentAt = time.Time{}
	s.statsMu.Unlock()
}

func (s *Session) publishPart(part stream.Part) error {
	if int(part.ResyncGen) < int(s.consumeGen.Load()) {
		s.sup.log.Debug("dropping stale part after resync",
			zap.String("stream", s.streamID),
			zap.Int64("time_ms", part.TimestampMS),
			zap.Int("part_gen", part.ResyncGen),
			zap.Int32("consume_gen", s.consumeGen.Load()),
		)
		return nil
	}

	s.notePartReceived()

	s.sup.log.Debug("part received",
		zap.String("stream", s.streamID),
		zap.Int64("time_ms", part.TimestampMS),
		zap.Int("channel", part.ChannelID),
		zap.Int("bytes", len(part.Data)),
	)

	chunks, err := s.assembler.Accept(part)
	if int(part.ResyncGen) < int(s.consumeGen.Load()) {
		s.sup.log.Debug("dropping stale part after resync (post-assemble)",
			zap.String("stream", s.streamID),
			zap.Int64("time_ms", part.TimestampMS),
			zap.Int("part_gen", part.ResyncGen),
			zap.Int32("consume_gen", s.consumeGen.Load()),
		)
		return nil
	}
	if len(chunks) > 0 {
		if writeErr := s.writeChunks(chunks); writeErr != nil {
			return writeErr
		}
	}
	if err != nil {
		s.sup.log.Warn("dropping part after parse/remux error",
			zap.String("stream", s.streamID),
			zap.Int64("time_ms", part.TimestampMS),
			zap.Error(err),
		)
		return nil
	}
	if s.segmentsOutLocked() > 0 {
		s.sup.registry.SetStatus(s.streamID, discovery.StatusStreaming)
	}
	return nil
}

func (s *Session) writeChunks(chunks [][]byte) error {
	if !s.unifiedIngest && !s.rtmpMuxStarted.Load() {
		s.rtmpMuxStarted.Store(true)
		s.sup.log.Info("first muxed A/V segment to RTMP",
			zap.String("stream", s.streamID),
		)
	}
	for _, chunk := range chunks {
		if err := s.publisher.Write(chunk); err != nil {
			s.applyRecoverOutput("rtmp write failed", true)
			if err2 := s.publisher.Write(chunk); err2 != nil {
				return err2
			}
		}
		s.noteSegmentOut()
	}
	return nil
}

func (s *Session) recoverOutput(reason string) {
	gen := s.scheduler.RecoverOutput()
	s.consumeGen.Store(int32(gen))
	s.assembler.Reset()
	if !s.unifiedIngest {
		s.rtmpMuxStarted.Store(false)
	}
	s.publisher.Reset()
	s.sup.log.Info("output recovery",
		zap.String("stream", s.streamID),
		zap.String("reason", reason),
		zap.Int("consume_gen", gen),
	)
}

// onTelegramResync runs after timeline jumps. Keeps playing buffered segments
// and RTMP continuous — only clears in-flight A/V pairing (native client).
func (s *Session) onTelegramResync(gen int) {
	s.noteResync()
	s.assembler.ClearPending()
	s.sup.log.Debug("telegram resync",
		zap.String("stream", s.streamID),
		zap.Int("scheduler_gen", gen),
	)
}

func (s *Session) ingestRecentlyHealthy() bool {
	lastSegment := s.lastSegmentTime()
	if lastSegment.IsZero() {
		return false
	}
	return time.Since(lastSegment) < stream.IngestHealthyWindow
}

func (s *Session) joinWithFloodRetry(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := s.client.Join(ctx)
		if err == nil {
			return nil
		}
		if wait, ok := stream.FloodWaitDelay(err); ok {
			s.noteJoinFloodWait(wait)
			s.sup.log.Warn("join flood wait during bootstrap",
				zap.String("stream", s.streamID),
				zap.Duration("wait", wait),
			)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
				continue
			}
		}
		return err
	}
}

func (s *Session) initStreamChannels(ctx context.Context, unified bool) error {
	const attempts = 3
	var lastErr error
	for i := 0; i < attempts; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.scheduler.InitFromCall(ctx); err != nil {
			lastErr = err
			if unified {
				s.sup.log.Warn("unified init stream channels failed; retrying",
					zap.String("stream", s.streamID),
					zap.Int("attempt", i+1),
					zap.Error(err),
				)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(500 * time.Millisecond):
				}
				continue
			}
			s.sup.log.Warn("separate A/V init partial; audio-only until endpoint mapping",
				zap.String("stream", s.streamID),
				zap.Error(err),
			)
			return nil
		}
		return nil
	}
	if unified {
		return lastErr
	}
	s.sup.log.Warn("separate A/V init partial; audio-only until endpoint mapping",
		zap.String("stream", s.streamID),
		zap.Error(lastErr),
	)
	return nil
}

func (s *Session) handleHardRejoinResult(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errStopIngest) {
		s.sup.registry.MarkEnded(s.chatID)
		s.sup.registry.RemoveEndedChat(s.chatID)
		return true
	}
	s.sup.log.Warn("hard rejoin failed",
		zap.String("stream", s.streamID),
		zap.Error(err),
	)
	return false
}

func (s *Session) hardRejoin(ctx context.Context, reason string, resetOutput bool) error {
	if !s.beginHardRejoin() {
		return nil
	}
	defer s.endHardRejoin()

	if s.discoveryEnded() {
		return errStopIngest
	}
	if s.client != nil {
		if err := s.client.CheckCallLive(ctx); err != nil {
			if stream.IsCallEnded(err) {
				return errStopIngest
			}
		}
	}

	if s.shouldDeferStreamDCRejoin() {
		s.cancelHardRejoin()
		return nil
	}

	if s.client != nil {
		_ = s.client.RefreshJoinSource(ctx)
		if err := s.client.Leave(ctx); err != nil {
			if wait, ok := stream.FloodWaitDelay(err); ok {
				s.noteJoinFloodWait(wait)
				s.cancelHardRejoin()
				s.sup.log.Warn("leave flood wait; deferring rejoin",
					zap.String("stream", s.streamID),
					zap.String("reason", reason),
					zap.Duration("wait", wait),
				)
				return nil
			}
			s.sup.log.Warn("leave failed; deferring hard rejoin",
				zap.String("stream", s.streamID),
				zap.String("reason", reason),
				zap.Error(err),
			)
			s.cancelHardRejoin()
			s.clearRecoveryHoldOnDefer()
			return nil
		}
	}
	s.beginRecoveryRejoin()
	select {
	case <-ctx.Done():
		s.cancelHardRejoin()
		s.clearRecoveryHoldOnDefer()
		return ctx.Err()
	case <-time.After(stream.HardRejoinSettle):
	}
	s.scheduler.ResetAfterRejoin()
	if resetOutput {
		s.recoverOutput("hard rejoin: " + reason)
	}
	if err := s.client.Join(ctx); err != nil {
		if wait, ok := stream.FloodWaitDelay(err); ok {
			s.noteJoinFloodWait(wait)
			s.clearRecoveryHoldOnDefer()
			s.sup.log.Warn("join flood wait; deferring rejoin",
				zap.String("stream", s.streamID),
				zap.String("reason", reason),
				zap.Duration("wait", wait),
			)
			return nil
		}
		s.sup.log.Error("join failed after hard rejoin; stopping ingest",
			zap.String("stream", s.streamID),
			zap.String("reason", reason),
			zap.Error(err),
		)
		return errStopIngest
	}
	if err := s.initStreamChannels(ctx, s.client.Unified()); err != nil {
		s.clearRecoveryHoldOnDefer()
		return err
	}
	s.resetPartStats()
	s.clearGetFileJoinMissing()
	return nil
}

func (s *Session) shouldDeferRejoin() bool {
	s.recoveryMu.Lock()
	now := time.Now()
	if s.rejoinActive ||
		now.Before(s.joinGraceUntil) ||
		now.Before(s.floodGraceUntil) ||
		(!s.lastRejoinAt.IsZero() && now.Sub(s.lastRejoinAt) < stream.MinRejoinCooldown) {
		s.recoveryMu.Unlock()
		return true
	}
	s.recoveryMu.Unlock()
	return s.shouldDeferStreamDCRejoin()
}

func (s *Session) shouldDeferStreamDCRejoin() bool {
	if s.client == nil || s.scheduler == nil {
		return false
	}
	var wait time.Duration
	if d, ok := s.client.StreamDCWaitRemaining(); ok && d > wait {
		wait = d
	}
	if d := s.scheduler.BootstrapBackoffRemaining(); d > 5*time.Second && d > wait {
		wait = d
	}
	if wait < 5*time.Second {
		return false
	}
	s.sup.log.Debug("deferring rejoin during stream DC backoff",
		zap.String("stream", s.streamID),
		zap.Duration("retry_after", wait),
	)
	return true
}
