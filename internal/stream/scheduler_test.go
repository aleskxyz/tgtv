package stream

import (
	"fmt"
	"testing"
	"time"

	"github.com/gotd/td/tgerr"
	"go.uber.org/zap"
)

func TestAvailableBufferDurationCountsTimestamps(t *testing.T) {
	s := &Scheduler{}
	s.available = []Part{
		{TimestampMS: 1000},
		{TimestampMS: 1000, ChannelID: 1},
		{TimestampMS: 2000},
	}
	if got := s.availableBufferDuration(); got != 2000 {
		t.Fatalf("got %d want 2000", got)
	}
}

func TestPartsForSegment_audioOnlyUntilEndpointMapping(t *testing.T) {
	s := &Scheduler{
		client: &Client{},
	}
	s.activeVideoChannels = []VideoChannel{{Endpoint: "ep", Quality: QualityFull}}

	parts := s.partsForSegment()
	if len(parts) != 1 || parts[0].kind != PartKindAudio {
		t.Fatalf("expected audio-only without mapping, got %v", parts)
	}

	s.endpointMapping = map[string]int32{"ep": 0}
	parts = s.partsForSegment()
	if len(parts) != 2 {
		t.Fatalf("expected audio+video with mapping, got %d parts", len(parts))
	}
	if parts[1].kind != PartKindVideo || parts[1].channelID != 1 {
		t.Fatalf("expected video ch1, got %+v", parts[1])
	}
}

func TestApplyEndpointMappingActivatesVideoChannel(t *testing.T) {
	s := &Scheduler{client: &Client{}}
	s.applyEndpointMapping(map[string]int32{"ep-main": 2})
	if len(s.activeVideoChannels) != 1 || s.activeVideoChannels[0].Endpoint != "ep-main" {
		t.Fatalf("activeVideoChannels = %+v", s.activeVideoChannels)
	}
	parts := s.partsForSegment()
	if len(parts) != 2 || parts[1].channelID != 3 {
		t.Fatalf("parts = %+v", parts)
	}
}

func TestEnqueueSegmentExpectVideoPartner(t *testing.T) {
	s := &Scheduler{}
	seg := &pendingSegment{
		timestamp: 1000,
		parts: []*pendingPart{
			{kind: PartKindAudio, channelID: 0, hasResult: true, result: []byte{1}},
			{kind: PartKindVideo, channelID: 1, hasResult: true, result: []byte{2}},
		},
	}
	s.enqueueSegment(seg)
	if len(s.available) != 2 {
		t.Fatalf("available len = %d", len(s.available))
	}
	if !s.available[0].ExpectVideoPartner {
		t.Fatal("audio part should expect video partner")
	}
	if s.available[1].ExpectVideoPartner {
		t.Fatal("video part should not set ExpectVideoPartner")
	}
}

func TestEnqueueSegmentEmptyVideoNoExpectPartner(t *testing.T) {
	s := &Scheduler{}
	seg := &pendingSegment{
		timestamp: 1000,
		parts: []*pendingPart{
			{kind: PartKindAudio, channelID: 0, hasResult: true, result: []byte{1}},
			{kind: PartKindVideo, channelID: 1, hasResult: true, result: nil},
		},
	}
	s.enqueueSegment(seg)
	if len(s.available) != 1 {
		t.Fatalf("available len = %d want 1 (audio only)", len(s.available))
	}
	if s.available[0].ExpectVideoPartner {
		t.Fatal("audio should not expect video partner when video payload empty")
	}
}

func TestRecoverOutputClearsPending(t *testing.T) {
	s := &Scheduler{
		downloads: newDownloadRegistry(),
	}
	s.pending = []*pendingSegment{{timestamp: 1000, parts: []*pendingPart{{kind: PartKindAudio}}}}
	s.available = []Part{{TimestampMS: 1000}}
	gen := s.RecoverOutput()
	if len(s.pending) != 0 {
		t.Fatalf("pending len = %d want 0", len(s.pending))
	}
	if len(s.available) != 0 {
		t.Fatal("available should be cleared")
	}
	if gen != 1 {
		t.Fatalf("gen = %d want 1", gen)
	}
}

func TestNotReadyRetryDelay_escalates(t *testing.T) {
	if got := notReadyRetryDelay(1); got != NotReadyRetry {
		t.Fatalf("streak 1: got %v want %v", got, NotReadyRetry)
	}
	if got := notReadyRetryDelay(NotReadyRetryMediumAfter); got != NotReadyRetry {
		t.Fatalf("streak at medium threshold: got %v want %v", got, NotReadyRetry)
	}
	if got := notReadyRetryDelay(NotReadyRetryMediumAfter + 1); got != NotReadyRetryMedium {
		t.Fatalf("streak after medium threshold: got %v want %v", got, NotReadyRetryMedium)
	}
	if got := notReadyRetryDelay(NotReadyRetryMaxAfter + 1); got != NotReadyRetryMax {
		t.Fatalf("streak after max threshold: got %v want %v", got, NotReadyRetryMax)
	}
}

func TestResyncNextTimestamp_unifiedBootstrap(t *testing.T) {
	got := ResyncNextTimestamp(true, 25_577_000)
	if got != -1 {
		t.Fatalf("got %d want -1", got)
	}
}

func TestResyncNextTimestamp_nonUnifiedBoundary(t *testing.T) {
	responseMS := int64(25_577_123)
	got := ResyncNextTimestamp(false, responseMS)
	want := ResyncBoundary(responseMS)
	if got != want {
		t.Fatalf("got %d want %d", got, want)
	}
}

func TestSchedulerLiveEdgeCatchUpOnLongDCWait(t *testing.T) {
	client := &Client{unified: true}
	sched := NewScheduler(client, zap.NewNop())
	part := &pendingPart{kind: PartKindUnified, channelID: UnifiedVideoChannel, quality: QualityFull}

	sched.mu.Lock()
	sched.pending = []*pendingSegment{{timestamp: 8_914_000, parts: []*pendingPart{part}}}
	sched.nextSegmentTimestamp = 8_915_000
	sched.available = []Part{{TimestampMS: 8_913_000}}
	sched.handleLiveEdgeCatchUp(10 * time.Second)
	if sched.nextSegmentTimestamp != -1 {
		t.Fatalf("nextSegmentTimestamp = %d, want -1", sched.nextSegmentTimestamp)
	}
	if len(sched.pending) != 0 {
		t.Fatalf("pending len = %d, want 0", len(sched.pending))
	}
	if len(sched.available) != 0 {
		t.Fatalf("available len = %d, want 0", len(sched.available))
	}
	if !sched.postResync {
		t.Fatal("postResync must be set")
	}
	sched.mu.Unlock()
}

func TestOutputHeadTimestampMS(t *testing.T) {
	s := &Scheduler{nextSegmentTimestamp: 5000}
	s.available = []Part{{TimestampMS: 3000}, {TimestampMS: 4000}}
	if got := s.outputHeadTimestampMS(); got != 3000 {
		t.Fatalf("with available: got %d want 3000", got)
	}

	s.available = nil
	s.pending = []*pendingSegment{{timestamp: 4000}, {timestamp: 5000}}
	if got := s.outputHeadTimestampMS(); got != 3000 {
		t.Fatalf("without available: got %d want 3000", got)
	}
}

func TestHandleLiveEdgeProbeResult_catchUpWhenLagHigh(t *testing.T) {
	client := &Client{unified: true}
	sched := NewScheduler(client, zap.NewNop())
	var hookGen int
	sched.SetHooks(SchedulerHooks{OnResync: func(gen int) { hookGen = gen }})

	sched.mu.Lock()
	sched.nextSegmentTimestamp = 3_930_000
	sched.available = []Part{{TimestampMS: 3_928_000}}
	beforeGen := sched.gen
	sched.handleLiveEdgeProbeResult(3_936_000) // lag 8000ms
	wantNext := AdjustBootstrapTimestamp(3_936_000)
	if len(sched.pending) == 0 || sched.pending[0].timestamp != wantNext {
		t.Fatalf("first pending ts = %v want %d", sched.pending, wantNext)
	}
	if sched.nextSegmentTimestamp <= wantNext {
		t.Fatalf("next_ms = %d want > %d after prefetch", sched.nextSegmentTimestamp, wantNext)
	}
	if len(sched.available) != 0 {
		t.Fatalf("available len = %d want 0", len(sched.available))
	}
	if !sched.postResync {
		t.Fatal("postResync must be set")
	}
	if hookGen != beforeGen+1 {
		t.Fatalf("hook gen = %d want %d", hookGen, beforeGen+1)
	}
	sched.mu.Unlock()
}

func TestHandleLiveEdgeProbeResult_skipsWhenLagLow(t *testing.T) {
	client := &Client{unified: true}
	sched := NewScheduler(client, zap.NewNop())

	sched.mu.Lock()
	sched.nextSegmentTimestamp = 3_930_000
	sched.available = []Part{{TimestampMS: 3_928_000}}
	sched.handleLiveEdgeProbeResult(3_931_000) // lag 3000ms
	if sched.nextSegmentTimestamp != 3_930_000 {
		t.Fatalf("next_ms = %d want unchanged 3930000", sched.nextSegmentTimestamp)
	}
	if len(sched.available) != 1 {
		t.Fatalf("available len = %d want 1", len(sched.available))
	}
	sched.mu.Unlock()
}

func TestHandleLiveEdgeProbeResult_respectsCooldown(t *testing.T) {
	client := &Client{unified: true}
	sched := NewScheduler(client, zap.NewNop())

	sched.mu.Lock()
	sched.nextSegmentTimestamp = 3_930_000
	sched.available = []Part{{TimestampMS: 3_928_000}}
	sched.lastLiveEdgeCatchUpAt = time.Now()
	sched.handleLiveEdgeProbeResult(3_940_000) // lag 12000ms but cooldown active
	if sched.nextSegmentTimestamp != 3_930_000 {
		t.Fatalf("next_ms = %d want unchanged during cooldown", sched.nextSegmentTimestamp)
	}
	sched.mu.Unlock()
}

func TestSchedulerShortDCWaitKeepsTimeline(t *testing.T) {
	client := &Client{unified: true}
	sched := NewScheduler(client, zap.NewNop())
	part := &pendingPart{kind: PartKindUnified, channelID: UnifiedVideoChannel, quality: QualityFull}

	sched.mu.Lock()
	sched.nextSegmentTimestamp = 8_915_000
	err := streamDCWait{dcID: 4, until: time.Now().Add(2 * time.Second)}
	sched.handleNotReady(8_914_000, part, 0, err)
	if sched.nextSegmentTimestamp != 8_915_000 {
		t.Fatalf("nextSegmentTimestamp = %d, want 8915000", sched.nextSegmentTimestamp)
	}
	sched.mu.Unlock()
}

func TestSchedulerGetFileFloodWaitRetriesAt100ms(t *testing.T) {
	client := &Client{unified: true}
	sched := NewScheduler(client, zap.NewNop())
	part := &pendingPart{kind: PartKindUnified, channelID: UnifiedVideoChannel, quality: QualityFull}

	floodErr := tgerr.New(420, tgerr.ErrFloodWait)
	floodErr.Argument = 29
	err := fmt.Errorf("%w: %w", ErrNotReady, floodErr)

	before := time.Now()
	sched.mu.Lock()
	sched.handleNotReady(9_295_000, part, 0, err)
	retryAt := part.minRequestAt
	sched.mu.Unlock()

	delay := retryAt.Sub(before)
	if delay < NotReadyRetry/2 || delay > NotReadyRetry*2 {
		t.Fatalf("retry delay %v, want ~%v (not full flood wait)", delay, NotReadyRetry)
	}
}
