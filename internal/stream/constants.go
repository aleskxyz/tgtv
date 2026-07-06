package stream

import "time"

// Timings from Telegram official client (StreamingMediaContext.cpp, LivePlayer.java).
const (
	SegmentDurationMS = 1000
	SegmentBufferMS   = 2000
	RebufferMS        = SegmentBufferMS + SegmentDurationMS
	NotReadyRetry     = 100 * time.Millisecond
	// Escalating not-ready backoff (see notReadyRetryDelay) applies to TIME_TOO_BIG only.
	// FLOOD_WAIT from upload.getFile always uses NotReadyRetry (native client behavior).
	NotReadyRetryMedium      = 500 * time.Millisecond
	NotReadyRetryMax         = 1 * time.Second
	NotReadyRetryMediumAfter = 5  // consecutive TIME_TOO_BIG before stepping up
	NotReadyRetryMaxAfter    = 15 // consecutive TIME_TOO_BIG before max backoff
	VideoNotReadyGiveUpAfter = NotReadyRetryMaxAfter
	TimestampBootstrapRetry  = 1000 * time.Millisecond
	MinSchedulerDelay        = 10 * time.Millisecond
	GetFileLimitBytes        = 128 * 1024
	CheckGroupCallInterval   = 4 * time.Second
	HardRejoinSettle         = 500 * time.Millisecond
	// IngestHealthyWindow — if parts arrived within this window, checkGroupCall
	// join-missing is treated as a false positive (getFile is the source of truth).
	IngestHealthyWindow = 3 * time.Second
	// ResyncGrace suppresses hard rejoin briefly after scheduler resync (native RESYNC_GRACE).
	ResyncGrace = 45 * time.Second
	// GetFileJoinMissingLatch — how long a getFile JOIN_MISSING signal counts as evidence.
	GetFileJoinMissingLatch = 30 * time.Second
	// RecoveryHoldSec default — stop ingest if rejoin produces no segments within this window.
	RecoveryHoldSec = 90 * time.Second
	// MinRejoinCooldown limits hard rejoin attempts per stream when multiple ingests
	// share one Telegram account (native client assumes a single active player).
	MinRejoinCooldown = CheckGroupCallInterval
	// LiveEdgeCatchUpAfterDCWait — after this stream-DC flood wait, discard stale
	// segments and re-bootstrap from last_timestamp_ms instead of falling behind.
	LiveEdgeCatchUpAfterDCWait = 3 * time.Second
	// LiveEdgeProbeInterval — how often unified ingest polls last_timestamp_ms to
	// detect drift from Telegram's live edge (scheduler-only; not in native client).
	LiveEdgeProbeInterval = 600 * time.Second
	// LiveEdgeCatchUpThresholdMS — catch up when (live − head − RebufferMS) exceeds
	// this (excess drift beyond normal playback buffer).
	LiveEdgeCatchUpThresholdMS int64 = 2000
	// LiveEdgeCatchUpCooldown — minimum time between proactive catch-up jumps.
	LiveEdgeCatchUpCooldown = 30 * time.Second

	QualityFull         = 2
	UnifiedVideoChannel = 1
)
