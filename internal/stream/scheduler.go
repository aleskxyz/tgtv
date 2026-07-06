package stream

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"
)

// PartKind mirrors PendingAudioSegmentData / PendingVideoSegmentData / PendingUnifiedSegmentData.
type PartKind int

const (
	PartKindAudio PartKind = iota
	PartKindVideo
	PartKindUnified
)

// VideoChannel mirrors StreamingMediaContext::VideoChannel (endpoint + quality).
type VideoChannel struct {
	Endpoint string
	Quality  int
}

// pendingPart mirrors PendingMediaSegmentPart (StreamingMediaContext.cpp:47-54).
type pendingPart struct {
	kind           PartKind
	channelID      int
	quality        int
	minRequestAt   time.Time
	result         []byte
	hasResult      bool
	taskID         int64
	inFlight       bool
	notReadyStreak int
}

// pendingSegment mirrors PendingMediaSegment.
type pendingSegment struct {
	timestamp int64
	parts     []*pendingPart
}

// Scheduler implements StreamingMediaContext request/check loop for ingest.
type Scheduler struct {
	client *Client
	log    *zap.Logger

	mu                   sync.Mutex
	nextSegmentTimestamp int64 // -1 = unknown
	pending              []*pendingSegment

	activeVideoChannels []VideoChannel
	endpointMapping     map[string]int32

	bootstrapInFlight    bool
	bootstrapRetryAt     time.Time
	bootstrapDelayTaskID int
	nextBootstrapTaskID  int

	delayedCheckID int
	nextDelayedID  int

	downloads *downloadRegistry

	endpointMapper func([]byte) map[string]int32

	gen       int
	closed    bool
	runCtx    context.Context
	runCancel context.CancelFunc

	// available mirrors _availableSegments: downloaded, waiting for render().
	available []Part
	// waitBufferedMS mirrors _waitForBufferredMillisecondsBeforeRendering (0 = not waiting).
	waitBufferedMS int64
	// playbackRefTime mirrors _playbackReferenceTimestamp for 1 Hz segment consumption.
	playbackRefTime time.Time
	// postResync skips rebuffer/pacing delays after TIME_TOO_SMALL while RTMP stays up.
	postResync bool

	lastLiveEdgeProbeAt   time.Time
	liveEdgeProbeInFlight bool
	lastLiveEdgeCatchUpAt time.Time

	out          chan Part
	rejoinNeeded chan struct{}
	callEnded    chan struct{}

	onGetFileJoinMissing func()
	onGetFileFloodWait   func(time.Duration)
	onResync             func(gen int)
}

func NewScheduler(client *Client, log *zap.Logger) *Scheduler {
	return &Scheduler{
		client:               client,
		log:                  log.Named("scheduler"),
		nextSegmentTimestamp: -1,
		endpointMapping:      make(map[string]int32),
		downloads:            newDownloadRegistry(),
		out:                  make(chan Part, 8),
		rejoinNeeded:         make(chan struct{}, 1),
		callEnded:            make(chan struct{}, 1),
	}
}

func (s *Scheduler) Generation() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gen
}

func (s *Scheduler) Parts() <-chan Part { return s.out }

func (s *Scheduler) SetEndpointMapper(fn func([]byte) map[string]int32) {
	s.endpointMapper = fn
}

func (s *Scheduler) RejoinNeeded() <-chan struct{} { return s.rejoinNeeded }

func (s *Scheduler) CallEnded() <-chan struct{} { return s.callEnded }

// SchedulerHooks optional session callbacks for recovery evidence.
type SchedulerHooks struct {
	OnGetFileJoinMissing func()
	OnGetFileFloodWait   func(time.Duration)
	OnResync             func(gen int)
}

func (s *Scheduler) SetHooks(h SchedulerHooks) {
	s.onGetFileJoinMissing = h.OnGetFileJoinMissing
	s.onGetFileFloodWait = h.OnGetFileFloodWait
	s.onResync = h.OnResync
}

// Run drives StreamingMediaContext render() at ~120 Hz (MinSchedulerDelay).
func (s *Scheduler) Run(ctx context.Context) {
	runCtx, runCancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.runCtx = runCtx
	s.runCancel = runCancel
	s.mu.Unlock()

	ticker := time.NewTicker(MinSchedulerDelay)
	defer ticker.Stop()
	defer func() {
		runCancel()
		s.mu.Lock()
		s.closed = true
		s.gen++
		// Invalidate delayed bootstrap/check callbacks (LivePlayer destroyed checks).
		s.bootstrapDelayTaskID = s.nextBootstrapTaskID
		s.delayedCheckID = s.nextDelayedID
		s.downloads.cancelAll()
		s.mu.Unlock()
		close(s.out)
	}()

	for {
		select {
		case <-runCtx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			if !s.closed {
				s.render(runCtx)
			}
			s.mu.Unlock()
		}
	}
}

func (s *Scheduler) bgCtx() context.Context {
	if s.runCtx != nil {
		return s.runCtx
	}
	return context.Background()
}

// availableBufferDuration — StreamingMediaContext.cpp getAvailableBufferDuration().
func (s *Scheduler) availableBufferDuration() int64 {
	if len(s.available) == 0 {
		return 0
	}
	seen := make(map[int64]struct{}, len(s.available))
	for _, part := range s.available {
		seen[part.TimestampMS] = struct{}{}
	}
	return int64(len(seen)) * SegmentDurationMS
}

// render — StreamingMediaContext.cpp render() (lines 273-554).
// Consumption attempt, then always requestSegmentsIfNeeded + checkPendingSegments.
// Caller must hold s.mu.
func (s *Scheduler) render(ctx context.Context) {
	var toSend []Part

	if len(s.available) == 0 {
		s.playbackRefTime = time.Time{}
		if !s.postResync {
			s.waitBufferedMS = RebufferMS
		}
	} else if s.waitBufferedMS > 0 {
		if s.availableBufferDuration() >= s.waitBufferedMS {
			s.waitBufferedMS = 0
		}
	} else {
		if s.playbackRefTime.IsZero() {
			if s.postResync {
				s.playbackRefTime = time.Now().Add(-time.Duration(SegmentDurationMS) * time.Millisecond)
				s.postResync = false
			} else {
				s.playbackRefTime = time.Now()
			}
		}
		elapsed := time.Since(s.playbackRefTime)
		if elapsed >= time.Duration(SegmentDurationMS)*time.Millisecond {
			ts := s.available[0].TimestampMS
			for len(s.available) > 0 && s.available[0].TimestampMS == ts {
				toSend = append(toSend, s.available[0])
				s.available = s.available[1:]
			}
			// Native client advances by segment duration, not wall now()
			// (StreamingMediaContext.cpp:521 — _playbackReferenceTimestamp += segment->duration).
			s.playbackRefTime = s.playbackRefTime.Add(time.Duration(SegmentDurationMS) * time.Millisecond)
		}
	}

	s.requestSegmentsIfNeeded(ctx)
	s.checkPendingSegments(ctx)
	s.maybeProbeLiveEdge(ctx)

	if len(toSend) > 0 {
		s.mu.Unlock()
		for _, part := range toSend {
			s.safeSend(part)
		}
		s.mu.Lock()
	}
}

// requestSegmentsIfNeeded — StreamingMediaContext.cpp:628-718
func (s *Scheduler) requestSegmentsIfNeeded(ctx context.Context) {
	if s.closed {
		return
	}
	for {
		if s.nextSegmentTimestamp == -1 {
			if !s.bootstrapInFlight && time.Now().After(s.bootstrapRetryAt) {
				s.bootstrapInFlight = true
				go s.requestCurrentTime(ctx)
			}
			break
		}

		buffered := s.availableBufferDuration() + int64(len(s.pending))*SegmentDurationMS
		if buffered > SegmentBufferMS {
			break
		}

		seg := &pendingSegment{timestamp: s.nextSegmentTimestamp}
		seg.parts = s.partsForSegment()
		s.pending = append(s.pending, seg)
		s.nextSegmentTimestamp += SegmentDurationMS

		if s.nextSegmentTimestamp == -1 {
			break
		}
	}
}

func (s *Scheduler) partsForSegment() []*pendingPart {
	if s.client.Unified() {
		return []*pendingPart{{kind: PartKindUnified, channelID: UnifiedVideoChannel, quality: QualityFull}}
	}

	parts := []*pendingPart{{kind: PartKindAudio, channelID: 0}}
	// StreamingMediaContext.cpp:699-711 — video parts only when endpoint mapping exists.
	for _, vc := range s.activeVideoChannels {
		idx, ok := s.endpointMapping[vc.Endpoint]
		if !ok {
			continue
		}
		parts = append(parts, &pendingPart{
			kind:      PartKindVideo,
			channelID: int(idx) + 1,
			quality:   vc.Quality,
		})
	}
	return parts
}

func (s *Scheduler) applyEndpointMapping(mapping map[string]int32) {
	if len(mapping) == 0 {
		return
	}
	s.endpointMapping = mapping
	if s.client.Unified() {
		return
	}
	if len(s.activeVideoChannels) == 0 {
		for endpoint := range mapping {
			s.activeVideoChannels = []VideoChannel{{Endpoint: endpoint, Quality: QualityFull}}
			break
		}
	}
}

// hasVideoEndpointMappingLocked reports whether any active video channel has a
// mapped stream index (StreamingMediaContext.cpp:700-703). Caller must hold s.mu.
func (s *Scheduler) hasVideoEndpointMappingLocked() bool {
	for _, vc := range s.activeVideoChannels {
		if _, ok := s.endpointMapping[vc.Endpoint]; ok {
			return true
		}
	}
	return false
}

// requestCurrentTime — LivePlayer.java requestCurrentTime callback (lines 517-569).
func (s *Scheduler) requestCurrentTime(ctx context.Context) {
	raw, err := s.client.RequestCurrentTime(ctx)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.bootstrapInFlight = false

	if s.closed {
		return
	}

	if err != nil {
		if delay, ok := streamDCWaitDelay(err); ok {
			s.bootstrapRetryAt = time.Now().Add(delay)
			s.log.Warn("bootstrap waiting for stream DC",
				zap.Int("dc", s.client.StreamDC()),
				zap.Duration("retry_after", delay),
			)
			return
		}
		s.scheduleBootstrapRetryLocked()
		return
	}

	adjusted := AdjustBootstrapTimestamp(raw)
	if adjusted <= 0 {
		s.scheduleBootstrapRetryLocked()
		return
	}

	s.nextSegmentTimestamp = adjusted
	s.lastLiveEdgeProbeAt = time.Now()
	s.log.Info("bootstrap timestamp", zap.Int64("ms", adjusted))
	s.requestSegmentsIfNeeded(ctx)
	s.checkPendingSegments(ctx)
}

// maybeProbeLiveEdge polls last_timestamp_ms during steady unified ingest and
// jumps forward when output has drifted too far behind Telegram's live edge.
// Caller must hold s.mu.
func (s *Scheduler) maybeProbeLiveEdge(ctx context.Context) {
	if s.closed || !s.client.Unified() || s.nextSegmentTimestamp == -1 {
		return
	}
	if s.bootstrapInFlight || s.liveEdgeProbeInFlight || s.waitBufferedMS > 0 {
		return
	}
	if !s.lastLiveEdgeProbeAt.IsZero() && time.Now().Before(s.lastLiveEdgeProbeAt.Add(LiveEdgeProbeInterval)) {
		return
	}
	s.lastLiveEdgeProbeAt = time.Now()
	s.liveEdgeProbeInFlight = true
	go s.probeLiveEdge(ctx)
}

func (s *Scheduler) probeLiveEdge(ctx context.Context) {
	raw, err := s.client.RequestCurrentTime(ctx)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.liveEdgeProbeInFlight = false

	if s.closed || err != nil {
		if err != nil {
			s.log.Debug("live edge probe failed", zap.Error(err))
		}
		return
	}
	s.handleLiveEdgeProbeResult(raw)
}

// handleLiveEdgeProbeResult compares Telegram live edge to our output head and
// seeks forward when lag exceeds LiveEdgeCatchUpThresholdMS. Caller must hold s.mu.
func (s *Scheduler) handleLiveEdgeProbeResult(raw int64) {
	if s.closed || s.waitBufferedMS > 0 {
		return
	}
	head := s.outputHeadTimestampMS()
	if head < 0 {
		return
	}
	lag := raw - head
	excessLag := lag - RebufferMS
	if excessLag < LiveEdgeCatchUpThresholdMS {
		s.log.Debug("live edge ok",
			zap.Int64("lag_ms", lag),
			zap.Int64("excess_lag_ms", excessLag),
			zap.Int64("head_ms", head),
			zap.Int64("live_ms", raw),
		)
		return
	}
	if !s.lastLiveEdgeCatchUpAt.IsZero() && time.Now().Before(s.lastLiveEdgeCatchUpAt.Add(LiveEdgeCatchUpCooldown)) {
		s.log.Debug("live edge lag above threshold; cooldown",
			zap.Int64("lag_ms", lag),
			zap.Int64("excess_lag_ms", excessLag),
			zap.Int64("head_ms", head),
			zap.Duration("cooldown", time.Until(s.lastLiveEdgeCatchUpAt.Add(LiveEdgeCatchUpCooldown))),
		)
		return
	}
	adjusted := AdjustBootstrapTimestamp(raw)
	s.seekLiveEdgeLocked(adjusted, lag, "periodic")
}

// outputHeadTimestampMS is the media timestamp we are about to emit next.
// Caller must hold s.mu.
func (s *Scheduler) outputHeadTimestampMS() int64 {
	if len(s.available) > 0 {
		return s.available[0].TimestampMS
	}
	if s.nextSegmentTimestamp == -1 {
		return -1
	}
	pendingMS := int64(len(s.pending)) * SegmentDurationMS
	head := s.nextSegmentTimestamp - pendingMS - s.availableBufferDuration()
	if head < 0 {
		return 0
	}
	return head
}

// seekLiveEdgeLocked discards stale timeline state and resumes fetching near live.
// Caller must hold s.mu.
func (s *Scheduler) seekLiveEdgeLocked(adjusted int64, lag int64, reason string) {
	if s.closed || adjusted <= 0 {
		return
	}
	head := s.outputHeadTimestampMS()
	if head >= 0 && adjusted <= head {
		return
	}
	s.trimAvailableBeforeLocked(adjusted)
	nextFetch := s.catchUpNextFetchMS(adjusted)
	s.nextSegmentTimestamp = nextFetch
	s.discardAllPendingLocked()
	s.lastLiveEdgeCatchUpAt = time.Now()
	gen := s.gen
	s.log.Info("live edge catch-up",
		zap.String("reason", reason),
		zap.Int64("lag_ms", lag),
		zap.Int64("next_ms", nextFetch),
		zap.Int64("adjusted_ms", adjusted),
		zap.Int("gen", gen),
	)
	if s.onResync != nil {
		s.onResync(gen)
	}
	ctx := s.bgCtx()
	s.requestSegmentsIfNeeded(ctx)
	s.checkPendingSegments(ctx)
}

// trimAvailableBeforeLocked drops queued segments older than the catch-up target.
// Caller must hold s.mu.
func (s *Scheduler) trimAvailableBeforeLocked(ts int64) {
	if len(s.available) == 0 {
		return
	}
	out := s.available[:0]
	for _, part := range s.available {
		if part.TimestampMS >= ts {
			out = append(out, part)
		}
	}
	s.available = out
}

// catchUpNextFetchMS picks the next fetch position without re-requesting segments
// already present in available. Caller must hold s.mu.
func (s *Scheduler) catchUpNextFetchMS(adjusted int64) int64 {
	next := adjusted
	if s.availableHasTimestampLocked(adjusted) {
		next = adjusted + SegmentDurationMS
	}
	if max := s.maxAvailableTimestampLocked(); max >= adjusted {
		if candidate := max + SegmentDurationMS; candidate > next {
			next = candidate
		}
	}
	return next
}

func (s *Scheduler) availableHasTimestampLocked(ts int64) bool {
	for _, part := range s.available {
		if part.TimestampMS == ts {
			return true
		}
	}
	return false
}

func (s *Scheduler) maxAvailableTimestampLocked() int64 {
	var max int64 = -1
	for _, part := range s.available {
		if part.TimestampMS > max {
			max = part.TimestampMS
		}
	}
	return max
}

func (s *Scheduler) scheduleBootstrapRetryLocked() {
	s.bootstrapRetryAt = time.Now().Add(TimestampBootstrapRetry)
	s.runDelayed(&s.bootstrapDelayTaskID, &s.nextBootstrapTaskID, TimestampBootstrapRetry, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.closed {
			return
		}
		ctx := s.bgCtx()
		s.requestSegmentsIfNeeded(ctx)
		s.checkPendingSegments(ctx)
	})
}

func (s *Scheduler) runDelayed(activeID *int, nextID *int, delay time.Duration, fn func()) {
	id := *nextID
	*nextID++
	*activeID = id
	go func(taskID int, d time.Duration) {
		time.Sleep(d)
		s.mu.Lock()
		stale := *activeID != taskID || s.closed
		if !stale {
			*activeID = 0
		}
		s.mu.Unlock()
		if stale {
			return
		}
		fn()
	}(id, delay)
}

// checkPendingSegments — StreamingMediaContext.cpp:800-958
func (s *Scheduler) checkPendingSegments(ctx context.Context) {
	if s.closed {
		return
	}
	now := time.Now()
	var minDelayed time.Duration
	hasDelayed := false
	shouldRequestMore := false

	for i := 0; i < len(s.pending); i++ {
		seg := s.pending[i]
		allPartsDone := true

		for _, part := range seg.parts {
			if !part.hasResult {
				allPartsDone = false
			}
			if part.hasResult || part.inFlight {
				continue
			}
			if !part.minRequestAt.IsZero() && now.Before(part.minRequestAt) {
				delay := part.minRequestAt.Sub(now)
				if !hasDelayed || delay < minDelayed {
					minDelayed = delay
					hasDelayed = true
				}
				continue
			}
			// Do not issue upload.getFile while stream DC export is in flood-wait.
			if wait, ok := s.client.StreamDCWaitRemaining(); ok && wait > 0 {
				part.minRequestAt = now.Add(wait)
				if !hasDelayed || wait < minDelayed {
					minDelayed = wait
					hasDelayed = true
				}
				continue
			}
			s.beginPartTask(ctx, seg.timestamp, part)
		}

		if allPartsDone && i == 0 {
			seg := s.pending[0]
			s.pending = s.pending[1:]
			i--
			shouldRequestMore = true
			s.enqueueSegment(seg)
		}
	}

	if hasDelayed {
		delay := minDelayed
		if delay < MinSchedulerDelay {
			delay = MinSchedulerDelay
		}
		s.scheduleDelayedCheck(delay)
	}
	if shouldRequestMore {
		s.requestSegmentsIfNeeded(ctx)
	}
}

func (s *Scheduler) scheduleDelayedCheck(delay time.Duration) {
	s.runDelayed(&s.delayedCheckID, &s.nextDelayedID, delay, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.closed {
			return
		}
		s.checkPendingSegments(s.bgCtx())
	})
}

func (s *Scheduler) beginPartTask(ctx context.Context, segmentTimestamp int64, part *pendingPart) {
	if s.closed {
		return
	}
	gen := s.gen
	part.inFlight = true
	dctx, taskID := s.downloads.start(ctx)
	part.taskID = taskID

	s.log.Debug("getFile requested",
		zap.Int64("time_ms", segmentTimestamp),
		zap.Int("channel", part.channelID),
		zap.Int("quality", part.quality),
		zap.Int("stream_dc", s.client.StreamDC()),
		zap.String("kind", partKindName(part.kind)),
	)

	go func() {
		data, responseMS, err := s.client.FetchPart(dctx, segmentTimestamp, SegmentDurationMS, part)
		s.handlePartResult(gen, segmentTimestamp, part, taskID, data, responseMS, err)
	}()
}

func (s *Scheduler) handlePartResult(
	gen int,
	segmentTimestamp int64,
	part *pendingPart,
	taskID int64,
	data []byte,
	responseMS int64,
	err error,
) {
	s.mu.Lock()
	s.downloads.finish(taskID)
	part.inFlight = false

	if s.closed || gen != s.gen {
		s.mu.Unlock()
		return
	}

	if err != nil {
		if errors.Is(err, context.Canceled) {
			s.mu.Unlock()
			return
		}
		s.log.Debug("getFile failed",
			zap.Int64("time_ms", segmentTimestamp),
			zap.Int("channel", part.channelID),
			zap.Error(err),
			zap.String("outcome", outcomeName(ClassifyGetFileError(err))),
		)
		if errors.Is(err, ErrRejoinRequired) {
			if s.onGetFileJoinMissing != nil {
				s.onGetFileJoinMissing()
			}
			s.signalRejoin()
			s.mu.Unlock()
			return
		}
		if errors.Is(err, ErrCallEnded) {
			s.signalCallEnded()
			s.mu.Unlock()
			return
		}
		switch ClassifyGetFileError(err) {
		case OutcomeNotReady:
			s.handleNotReady(segmentTimestamp, part, responseMS, err)
		default:
			s.handleResync(responseMS)
		}
		s.mu.Unlock()
		return
	}

	part.result = data
	part.hasResult = true
	part.notReadyStreak = 0
	s.log.Debug("getFile ok",
		zap.Int64("time_ms", segmentTimestamp),
		zap.Int("channel", part.channelID),
		zap.Int("bytes", len(data)),
	)
	if s.nextSegmentTimestamp == -1 {
		s.nextSegmentTimestamp = segmentTimestamp + SegmentDurationMS
	}

	if part.kind == PartKindAudio && len(data) > 0 && s.endpointMapper != nil {
		if mapping := s.endpointMapper(data); len(mapping) > 0 {
			hadVideo := s.hasVideoEndpointMappingLocked()
			s.applyEndpointMapping(mapping)
			if !hadVideo && s.hasVideoEndpointMappingLocked() {
				ctx := s.bgCtx()
				s.requestSegmentsIfNeeded(ctx)
			}
		}
	}
	s.mu.Unlock()

	s.mu.Lock()
	if !s.closed && gen == s.gen {
		s.checkPendingSegments(s.bgCtx())
	}
	s.mu.Unlock()
}

// notReadyRetryDelay returns how long to wait before retrying upload.getFile after
// TIME_TOO_BIG / not-ready. The official client always uses NotReadyRetry (100ms).
// We escalate after sustained failure to reduce RPC volume during multi-stream ingest.
func notReadyRetryDelay(consecutiveNotReady int) time.Duration {
	switch {
	case consecutiveNotReady <= NotReadyRetryMediumAfter:
		return NotReadyRetry
	case consecutiveNotReady <= NotReadyRetryMaxAfter:
		return NotReadyRetryMedium
	default:
		return NotReadyRetryMax
	}
}

// handleNotReady — StreamingMediaContext.cpp:858-870
func (s *Scheduler) handleNotReady(segmentTimestamp int64, part *pendingPart, responseMS int64, err error) {
	if s.closed {
		return
	}
	if part.kind == PartKindVideo && part.notReadyStreak >= NotReadyRetryMaxAfter {
		part.hasResult = true
		part.result = nil
		part.notReadyStreak = 0
		s.log.Warn("abandoning video part after sustained not-ready",
			zap.Int64("ts", segmentTimestamp),
			zap.Int("channel", part.channelID),
		)
		s.checkPendingSegments(s.bgCtx())
		return
	}
	if segmentTimestamp == 0 && !s.client.Unified() {
		s.nextSegmentTimestamp = ResyncBoundary(responseMS)
		s.discardAllPendingLocked()
		ctx := s.bgCtx()
		s.requestSegmentsIfNeeded(ctx)
		s.checkPendingSegments(ctx)
		return
	}
	delay := notReadyRetryDelay(part.notReadyStreak)
	part.notReadyStreak++
	if d, ok := streamDCWaitDelay(err); ok {
		delay = d
		s.log.Warn("stream DC wait",
			zap.Int("dc", s.client.StreamDC()),
			zap.Duration("retry_after", delay),
		)
		if delay >= LiveEdgeCatchUpAfterDCWait && s.client.Unified() {
			s.handleLiveEdgeCatchUp(delay)
			return
		}
	} else if wait, ok := floodWaitDelay(err); ok {
		if s.onGetFileFloodWait != nil {
			s.onGetFileFloodWait(wait)
		}
		// StreamingMediaContext.cpp:868 — NotReady always retries in 100ms, even when
		// Telegram returns FLOOD_WAIT_N on upload.getFile (LivePlayer.java:494).
		delay = NotReadyRetry
		s.log.Debug("getFile flood wait; native retry",
			zap.Int("dc", s.client.StreamDC()),
			zap.Duration("telegram_wait", wait),
			zap.Duration("retry_after", delay),
		)
	}
	part.minRequestAt = time.Now().Add(delay)
	s.checkPendingSegments(s.bgCtx())
}

// handleResync — StreamingMediaContext.cpp:873-885
func (s *Scheduler) handleResync(responseMS int64) {
	if s.closed {
		return
	}
	s.nextSegmentTimestamp = ResyncNextTimestamp(s.client.Unified(), responseMS)
	// StreamingMediaContext.cpp:873-885 — discard pending only; keep _availableSegments.
	s.discardAllPendingLocked()
	gen := s.gen
	s.log.Info("resync", zap.Int64("next_ms", s.nextSegmentTimestamp), zap.Int("gen", gen))
	if s.onResync != nil {
		s.onResync(gen)
	}
	ctx := s.bgCtx()
	s.requestSegmentsIfNeeded(ctx)
	s.checkPendingSegments(ctx)
}

// handleLiveEdgeCatchUp discards stale timeline state during a long stream-DC wait
// and re-bootstraps from last_timestamp_ms when the DC unblocks (multi-stream ingest).
func (s *Scheduler) handleLiveEdgeCatchUp(retryAfter time.Duration) {
	if s.closed {
		return
	}
	s.nextSegmentTimestamp = -1
	s.discardAllPendingLocked()
	s.bootstrapInFlight = false
	s.bootstrapRetryAt = time.Now().Add(retryAfter)
	gen := s.gen
	s.log.Info("live edge catch-up",
		zap.Int("dc", s.client.StreamDC()),
		zap.Duration("retry_after", retryAfter),
		zap.Int("gen", gen),
	)
	if s.onResync != nil {
		s.onResync(gen)
	}
	s.runDelayed(&s.bootstrapDelayTaskID, &s.nextBootstrapTaskID, retryAfter, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.closed {
			return
		}
		ctx := s.bgCtx()
		s.requestSegmentsIfNeeded(ctx)
		s.checkPendingSegments(ctx)
	})
}

// enqueueSegment pushes completed parts into _availableSegments (not out).
func (s *Scheduler) enqueueSegment(seg *pendingSegment) {
	if s.closed {
		return
	}
	gen := s.gen
	expectVideo := false
	for _, part := range seg.parts {
		if part.kind == PartKindVideo && part.hasResult && len(part.result) > 0 {
			expectVideo = true
			break
		}
	}
	for _, part := range seg.parts {
		if !part.hasResult || len(part.result) == 0 {
			if part.hasResult && s.log != nil {
				s.log.Debug("empty part", zap.Int64("ts", seg.timestamp), zap.Int("channel", part.channelID))
			}
			continue
		}
		s.available = append(s.available, Part{
			TimestampMS:        seg.timestamp,
			ChannelID:          part.channelID,
			Quality:            part.quality,
			Kind:               part.kind,
			Data:               part.result,
			ResyncGen:          gen,
			ExpectVideoPartner: expectVideo && part.kind == PartKindAudio,
		})
	}
}

func (s *Scheduler) safeSend(part Part) {
	defer func() {
		if recover() != nil {
			s.log.Debug("part emit skipped, scheduler stopped", zap.Int64("time_ms", part.TimestampMS))
		}
	}()
	s.out <- part
}

func (s *Scheduler) signalRejoin() {
	select {
	case s.rejoinNeeded <- struct{}{}:
	default:
	}
}

func (s *Scheduler) signalCallEnded() {
	select {
	case s.callEnded <- struct{}{}:
	default:
	}
}

func (s *Scheduler) clearPendingLocked() {
	for _, seg := range s.pending {
		for _, part := range seg.parts {
			part.inFlight = false
			part.taskID = 0
		}
	}
	s.pending = nil
}

func (s *Scheduler) discardAllPendingLocked() {
	s.gen++
	s.downloads.cancelAll()
	s.clearPendingLocked()
}

// flushAvailableForResyncLocked drops queued segments from before a Telegram
// timeline jump. Caller must hold s.mu. RTMP is not reset (official client).
func (s *Scheduler) flushAvailableForResyncLocked() {
	s.available = nil
	s.waitBufferedMS = 0
	s.playbackRefTime = time.Time{}
	s.postResync = true
}

func (s *Scheduler) ResetAfterRejoin() {
	s.mu.Lock()
	defer s.mu.Unlock()
	preserveBootstrap := s.nextSegmentTimestamp == -1 &&
		!s.bootstrapRetryAt.IsZero() &&
		time.Now().Before(s.bootstrapRetryAt)
	savedRetry := s.bootstrapRetryAt
	s.discardAllPendingLocked()
	s.available = nil
	s.waitBufferedMS = 0
	s.playbackRefTime = time.Time{}
	s.nextSegmentTimestamp = -1
	s.bootstrapInFlight = false
	s.bootstrapRetryAt = time.Time{}
	s.lastLiveEdgeProbeAt = time.Time{}
	s.lastLiveEdgeCatchUpAt = time.Time{}
	s.liveEdgeProbeInFlight = false
	s.endpointMapping = make(map[string]int32)
	if preserveBootstrap {
		s.bootstrapRetryAt = savedRetry
	}
}

// RecoverOutput clears buffered segments after output-side recovery (remux/RTMP failure).
func (s *Scheduler) RecoverOutput() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gen++
	s.available = nil
	s.waitBufferedMS = 0
	s.playbackRefTime = time.Time{}
	s.clearPendingLocked()
	s.downloads.cancelAll()
	return s.gen
}

// BootstrapBackoffRemaining is how long until bootstrap retries after stream DC flood wait.
func (s *Scheduler) BootstrapBackoffRemaining() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nextSegmentTimestamp != -1 || s.bootstrapRetryAt.IsZero() {
		return 0
	}
	d := time.Until(s.bootstrapRetryAt)
	if d < 0 {
		return 0
	}
	return d
}

// InitFromCall configures active video channels like LivePlayer.addIncomingVideoOutput.
func (s *Scheduler) InitFromCall(ctx context.Context) error {
	if s.client.Unified() {
		channels, err := s.client.StreamChannels(ctx)
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.activeVideoChannels = []VideoChannel{{Endpoint: "unified", Quality: QualityFull}}
		if primary, ok := SelectPrimaryStreamChannel(channels); ok {
			s.endpointMapping["unified"] = int32(primary.Channel - 1)
		}
		s.mu.Unlock()
		return nil
	}

	endpoint, sources, err := s.client.BroadcasterVideo(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.activeVideoChannels = nil
		return err
	}
	s.activeVideoChannels = []VideoChannel{{Endpoint: endpoint, Quality: QualityFull}}
	if idx := SelectPrimarySourceIndex(sources); idx >= 0 {
		s.endpointMapping[endpoint] = int32(idx)
	}
	return nil
}

func partKindName(k PartKind) string {
	switch k {
	case PartKindAudio:
		return "audio"
	case PartKindVideo:
		return "video"
	case PartKindUnified:
		return "unified"
	default:
		return "unknown"
	}
}

func outcomeName(o PartOutcome) string {
	switch o {
	case OutcomeNotReady:
		return "not_ready"
	default:
		return "resync"
	}
}
