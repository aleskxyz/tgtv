package ingest

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"

	"github.com/aleskxyz/tgtv/internal/discovery"
	"github.com/aleskxyz/tgtv/internal/stream"
)

type recoveryTrigger int

const (
	triggerGetFileJoinMissing recoveryTrigger = iota
	triggerCheckJoin
	triggerInputStall
	triggerOutputStall
)

type recoveryState int

const (
	recoveryNone recoveryState = iota
	recoveryRejoining
	recoveryFailed
)

// RecoveryAction is the outcome of evidence-based recovery evaluation.
type RecoveryAction int

const (
	RecoveryActionNone RecoveryAction = iota
	RecoveryActionDefer
	RecoveryActionRecoverOutput
	RecoveryActionHardRejoin
	RecoveryActionStopEnded
	RecoveryActionFailRecovery
)

var errStopIngest = errors.New("stop ingest")

type RecoverySnapshot struct {
	CallLive          bool
	CallEnded         bool
	CallLiveUnknown   bool
	CheckJoinOK       bool
	CheckJoinMissing  bool
	GetFileJoinRecent bool
	InputStalled      bool
	OutputStalled     bool
	InputRecent       bool
	IngestHealthy     bool
	DiscoveryEnded    bool
	InResyncGrace     bool
	StreamDCWaiting   bool
}

func (s *Session) discoveryEnded() bool {
	entry, ok := s.sup.registry.Get(s.streamID)
	return !ok || entry.Status == discovery.StatusEnded
}

func (s *Session) noteGetFileJoinMissing() {
	s.recoveryMu.Lock()
	s.getFileJoinMissingAt = time.Now()
	s.recoveryMu.Unlock()
}

func (s *Session) getFileJoinMissingRecent() bool {
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()
	if s.getFileJoinMissingAt.IsZero() {
		return false
	}
	return time.Since(s.getFileJoinMissingAt) < stream.GetFileJoinMissingLatch
}

func (s *Session) clearGetFileJoinMissing() {
	s.recoveryMu.Lock()
	s.getFileJoinMissingAt = time.Time{}
	s.recoveryMu.Unlock()
}

func (s *Session) noteResync() {
	s.recoveryMu.Lock()
	s.lastResyncAt = time.Now()
	s.recoveryMu.Unlock()
}

func (s *Session) inResyncGrace() bool {
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()
	if s.lastResyncAt.IsZero() {
		return false
	}
	return time.Since(s.lastResyncAt) < stream.ResyncGrace
}

func (s *Session) streamDCWaiting() bool {
	if s.client == nil || s.scheduler == nil {
		return false
	}
	if d, ok := s.client.StreamDCWaitRemaining(); ok && d > 0 {
		return true
	}
	return s.scheduler.BootstrapBackoffRemaining() > 0
}

func (s *Session) isInputStalled() bool {
	_, _, lastPart := s.partStats()
	if lastPart.IsZero() {
		return false
	}
	threshold := time.Duration(s.sup.cfg.IngestInputRejoinSeconds * float64(time.Second))
	if threshold <= 0 {
		return false
	}
	return time.Since(lastPart) >= threshold
}

func (s *Session) rebufferThreshold() time.Duration {
	sec := s.sup.cfg.IngestRebufferSeconds
	if sec <= 0 {
		sec = 3
	}
	return time.Duration(sec * float64(time.Second))
}

func (s *Session) startupGrace() time.Duration {
	sec := s.sup.cfg.IngestStartupGraceSeconds
	if sec <= 0 {
		sec = 15
	}
	return time.Duration(sec * float64(time.Second))
}

func (s *Session) outputRecoverCooldown() time.Duration {
	sec := s.sup.cfg.IngestOutputRecoverCooldownSeconds
	if sec <= 0 {
		sec = 1
	}
	return time.Duration(sec * float64(time.Second))
}

func (s *Session) hasRecentTelegramInput() bool {
	_, _, lastPart := s.partStats()
	if lastPart.IsZero() {
		return false
	}
	return time.Since(lastPart) < s.rebufferThreshold()
}

func (s *Session) isOutputStalled() bool {
	if s.startedAt.IsZero() || time.Since(s.startedAt) < s.startupGrace() {
		return false
	}
	if !s.unifiedIngest && !s.rtmpMuxStarted.Load() {
		return false
	}
	if !s.hasRecentTelegramInput() {
		return false
	}
	threshold := s.rebufferThreshold()
	lastSeg := s.lastSegmentTime()
	if lastSeg.IsZero() {
		partsIn, _, lastPart := s.partStats()
		return partsIn > 0 && !lastPart.IsZero() && time.Since(lastPart) >= threshold
	}
	return time.Since(lastSeg) >= threshold
}

func (s *Session) canRecoverOutput() bool {
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()
	if s.lastOutputRecoveryAt.IsZero() {
		return true
	}
	return time.Since(s.lastOutputRecoveryAt) >= s.outputRecoverCooldown()
}

func (s *Session) gatherSnapshot(ctx context.Context, probeJoin bool) (RecoverySnapshot, error) {
	snap := RecoverySnapshot{
		InputStalled:      s.isInputStalled(),
		OutputStalled:     s.isOutputStalled(),
		InputRecent:       s.hasRecentTelegramInput(),
		IngestHealthy:     s.ingestRecentlyHealthy(),
		DiscoveryEnded:    s.discoveryEnded(),
		InResyncGrace:     s.inResyncGrace(),
		StreamDCWaiting:   s.streamDCWaiting(),
		GetFileJoinRecent: s.getFileJoinMissingRecent(),
	}
	if s.client == nil {
		return snap, nil
	}
	var callLiveErr error
	if err := s.client.CheckCallLive(ctx); err != nil {
		snap.CallEnded = stream.IsCallEnded(err)
		if snap.CallEnded {
			snap.CallLive = false
			return snap, nil
		}
		snap.CallLiveUnknown = true
		callLiveErr = err
	} else {
		snap.CallLive = true
	}
	if !probeJoin {
		return snap, callLiveErr
	}
	if callLiveErr != nil {
		return snap, callLiveErr
	}
	err := s.client.CheckJoin(ctx)
	switch {
	case err == nil:
		snap.CheckJoinOK = true
	case stream.IsCallEnded(err):
		snap.CheckJoinOK = false
		snap.CallEnded = true
		snap.CallLive = false
	case errors.Is(err, stream.ErrRejoinRequired):
		snap.CheckJoinOK = false
		snap.CheckJoinMissing = true
	default:
		snap.CheckJoinOK = false
	}
	return snap, err
}

func (s *Session) evalAndApplyRecovery(ctx context.Context, trigger recoveryTrigger, probeJoin bool) (stop bool) {
	if action := s.monitorRecoveryTimeout(); action == RecoveryActionFailRecovery {
		stop, _ = s.applyRecoveryAction(ctx, action, "recovery_hold_timeout", false)
		return stop
	}
	snap, err := s.gatherSnapshot(ctx, probeJoin)
	if wait, ok := stream.FloodWaitDelay(err); ok {
		s.noteJoinFloodWait(wait)
		if stop := s.applyTerminalFromSnapshot(ctx, snap); stop {
			return true
		}
		if probeJoin {
			if stop := s.applyTerminalFromCallLiveOnly(ctx); stop {
				return true
			}
		}
		s.sup.log.Warn("checkGroupCall flood wait; deferring rejoin",
			zap.String("stream", s.streamID),
			zap.Duration("wait", wait),
		)
		return false
	}
	action, reason, resetOutput := evaluateRecovery(snap, trigger)
	stop, applyErr := s.applyRecoveryAction(ctx, action, reason, resetOutput)
	if applyErr != nil {
		s.sup.log.Warn("recovery action failed",
			zap.String("stream", s.streamID),
			zap.String("reason", reason),
			zap.Error(applyErr),
		)
	}
	return stop
}

func (s *Session) applyTerminalFromSnapshot(ctx context.Context, snap RecoverySnapshot) bool {
	if snap.DiscoveryEnded || snap.CallEnded {
		stop, _ := s.applyRecoveryAction(ctx, RecoveryActionStopEnded, "call_or_discovery_ended", false)
		return stop
	}
	return false
}

func (s *Session) applyTerminalFromCallLiveOnly(ctx context.Context) bool {
	liveSnap, liveErr := s.gatherSnapshot(ctx, false)
	if _, ok := stream.FloodWaitDelay(liveErr); ok {
		return false
	}
	if liveSnap.CallLiveUnknown {
		return false
	}
	if !liveSnap.CallLive && !liveSnap.CallEnded {
		stop, _ := s.applyRecoveryAction(ctx, RecoveryActionStopEnded, "call_not_live", false)
		return stop
	}
	return s.applyTerminalFromSnapshot(ctx, liveSnap)
}

func evaluateRecovery(snap RecoverySnapshot, trigger recoveryTrigger) (RecoveryAction, string, bool) {
	if snap.DiscoveryEnded || snap.CallEnded {
		return RecoveryActionStopEnded, "call_or_discovery_ended", false
	}
	if snap.CallLiveUnknown {
		return RecoveryActionDefer, "call_live_unknown", false
	}
	if !snap.CallLive {
		return RecoveryActionStopEnded, "call_not_live", false
	}

	if snap.InResyncGrace && trigger != triggerGetFileJoinMissing {
		return RecoveryActionDefer, "resync_grace", false
	}
	if snap.StreamDCWaiting && snap.IngestHealthy {
		return RecoveryActionDefer, "stream_dc_wait_healthy", false
	}

	switch trigger {
	case triggerGetFileJoinMissing:
		if snap.IngestHealthy && snap.CheckJoinOK {
			return RecoveryActionDefer, "getFile_join_missing_healthy_check_ok", false
		}
		if snap.IngestHealthy && !snap.CheckJoinMissing {
			return RecoveryActionDefer, "getFile_join_missing_healthy_alone", false
		}
		return RecoveryActionHardRejoin, "getFile_join_missing", !snap.IngestHealthy

	case triggerCheckJoin:
		if snap.CheckJoinOK {
			return RecoveryActionNone, "", false
		}
		if snap.IngestHealthy {
			return RecoveryActionDefer, "checkJoin_missing_healthy", false
		}
		if snap.InputStalled || snap.GetFileJoinRecent {
			return RecoveryActionHardRejoin, "checkJoin_corroborated", false
		}
		return RecoveryActionDefer, "checkJoin_missing_uncorroborated", false

	case triggerInputStall:
		if snap.CheckJoinMissing || snap.GetFileJoinRecent {
			return RecoveryActionHardRejoin, "input_stall_corroborated", false
		}
		if snap.InputStalled && !snap.IngestHealthy {
			return RecoveryActionHardRejoin, "input_stall_no_output", false
		}
		return RecoveryActionDefer, "input_stall_uncorroborated", false

	case triggerOutputStall:
		if !snap.OutputStalled {
			return RecoveryActionNone, "", false
		}
		if snap.InResyncGrace {
			return RecoveryActionDefer, "resync_grace", false
		}
		if !snap.InputRecent {
			return RecoveryActionDefer, "output_stall_no_recent_input", false
		}
		return RecoveryActionRecoverOutput, "output_stall", false
	}

	return RecoveryActionNone, "", false
}

func (s *Session) recoveryHoldDuration() time.Duration {
	sec := s.sup.cfg.IngestRecoveryHoldSeconds
	if sec <= 0 {
		sec = float64(stream.RecoveryHoldSec / time.Second)
	}
	return time.Duration(sec * float64(time.Second))
}

func (s *Session) beginRecoveryRejoin() {
	s.recoveryMu.Lock()
	s.recoveryState = recoveryRejoining
	s.recoveryStartedAt = time.Now()
	s.recoveryMu.Unlock()
}

func (s *Session) clearRecoveryOnSegment() {
	s.recoveryMu.Lock()
	s.getFileJoinMissingAt = time.Time{}
	if s.recoveryState == recoveryRejoining {
		s.recoveryState = recoveryNone
		s.recoveryStartedAt = time.Time{}
	}
	s.recoveryMu.Unlock()
}

func (s *Session) clearRecoveryHoldOnDefer() {
	s.recoveryMu.Lock()
	if s.recoveryState == recoveryRejoining {
		s.recoveryState = recoveryNone
		s.recoveryStartedAt = time.Time{}
	}
	s.recoveryMu.Unlock()
}

func (s *Session) markRecoveryFailed() {
	s.recoveryMu.Lock()
	s.recoveryState = recoveryFailed
	s.recoveryMu.Unlock()
}

func (s *Session) shouldHoldHLS() bool {
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()
	return s.recoveryState == recoveryRejoining
}

func (s *Session) isRecoveryFailed() bool {
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()
	return s.recoveryState == recoveryFailed
}

func (s *Session) monitorRecoveryTimeout() RecoveryAction {
	s.recoveryMu.Lock()
	state := s.recoveryState
	started := s.recoveryStartedAt
	s.recoveryMu.Unlock()
	if state != recoveryRejoining || started.IsZero() {
		return RecoveryActionNone
	}
	if time.Since(started) < s.recoveryHoldDuration() {
		return RecoveryActionNone
	}
	if s.ingestRecentlyHealthy() {
		s.clearRecoveryOnSegment()
		return RecoveryActionNone
	}
	return RecoveryActionFailRecovery
}

func (s *Session) applyRecoverOutput(reason string, force bool) bool {
	if !force && !s.canRecoverOutput() {
		s.sup.log.Debug("output recovery deferred",
			zap.String("stream", s.streamID),
			zap.String("reason", "cooldown"),
		)
		return false
	}

	s.recoveryMu.Lock()
	if !force && s.outputRecoveryPending {
		s.recoveryMu.Unlock()
		s.sup.log.Debug("output recovery deferred",
			zap.String("stream", s.streamID),
			zap.String("reason", "in_flight"),
		)
		return false
	}
	s.outputRecoveryPending = true
	s.recoveryMu.Unlock()

	partsIn, partsOut, lastPart := s.partStats()
	lastSeg := s.lastSegmentTime()
	outAge := time.Since(lastSeg)
	if lastSeg.IsZero() {
		outAge = time.Since(lastPart)
	}
	s.sup.log.Warn("recovering output stall",
		zap.String("stream", s.streamID),
		zap.String("reason", reason),
		zap.Int("parts_in", partsIn),
		zap.Int("segments_out", partsOut),
		zap.Duration("since_last_part", time.Since(lastPart)),
		zap.Duration("since_last_output", outAge),
		zap.Bool("force", force),
	)

	s.recoverOutput(reason)

	s.recoveryMu.Lock()
	s.outputRecoveryPending = false
	s.lastOutputRecoveryAt = time.Now()
	s.recoveryMu.Unlock()
	return true
}

func (s *Session) applyRecoveryAction(
	ctx context.Context,
	action RecoveryAction,
	reason string,
	resetOutput bool,
) (stop bool, err error) {
	switch action {
	case RecoveryActionNone, RecoveryActionDefer:
		if reason != "" && action == RecoveryActionDefer {
			s.sup.log.Debug("recovery deferred",
				zap.String("stream", s.streamID),
				zap.String("reason", reason),
			)
		}
		return false, nil

	case RecoveryActionStopEnded:
		s.sup.log.Info("stopping ingest: stream ended",
			zap.String("stream", s.streamID),
			zap.String("reason", reason),
		)
		s.sup.registry.MarkEnded(s.chatID)
		s.sup.registry.RemoveEndedChat(s.chatID)
		return true, nil

	case RecoveryActionFailRecovery:
		s.exitRecoveryFailed.Store(true)
		s.sup.noteRecoveryFailure(s.streamID)
		s.markRecoveryFailed()
		s.sup.log.Warn("recovery failed; stopping ingest",
			zap.String("stream", s.streamID),
			zap.String("reason", reason),
			zap.Duration("hold", s.recoveryHoldDuration()),
		)
		return true, nil

	case RecoveryActionRecoverOutput:
		if !s.applyRecoverOutput(reason, false) {
			return false, nil
		}
		return false, nil

	case RecoveryActionHardRejoin:
		if s.shouldDeferRejoin() {
			s.sup.log.Debug("recovery deferred by rejoin policy",
				zap.String("stream", s.streamID),
				zap.String("reason", reason),
			)
			return false, nil
		}
		s.sup.log.Warn("hard rejoin",
			zap.String("stream", s.streamID),
			zap.String("reason", reason),
			zap.Bool("reset_output", resetOutput),
		)
		req := hardRejoinReq{reason: reason, resetOutput: resetOutput}
		select {
		case s.rejoinCh <- req:
		default:
			// Rejoin already queued; coalesce to latest request.
			select {
			case <-s.rejoinCh:
			default:
			}
			s.rejoinCh <- req
		}
		return false, nil
	}
	return false, nil
}
