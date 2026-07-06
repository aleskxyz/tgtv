package segment

import (
	"slices"
	"sync"

	"github.com/aleskxyz/tgtv/internal/remux"
	"github.com/aleskxyz/tgtv/internal/stream"
)

const maxPendingSegments = 8

type pendingSlice struct {
	container string
	payload   []byte
}

type pendingSegment struct {
	audio *pendingSlice
	video *pendingSlice
}

// Assembler pairs separate audio/video parts and remuxes to MPEG-TS.
type Assembler struct {
	mu                  sync.Mutex
	tsOffset            float64
	lastPartTS          int64 // last Telegram segment timestamp written
	pending             map[int64]*pendingSegment
	separateAV          bool
	primaryVideoChannel int
	logoPath            string
}

func NewAssembler(logoPath string) *Assembler {
	return &Assembler{
		pending:  make(map[int64]*pendingSegment),
		logoPath: logoPath,
	}
}

func (a *Assembler) SetLogoPath(path string) {
	a.mu.Lock()
	a.logoPath = path
	a.mu.Unlock()
}

func (a *Assembler) Reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.tsOffset = 0
	a.lastPartTS = 0
	a.pending = make(map[int64]*pendingSegment)
	a.separateAV = false
	a.primaryVideoChannel = 0
}

// ClearPending drops in-flight A/V pairing state after a Telegram resync without
// resetting the continuous MPEG-TS timeline (RTMP is not restarted).
func (a *Assembler) ClearPending() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.pending = make(map[int64]*pendingSegment)
}

func (a *Assembler) Accept(part stream.Part) ([][]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.acceptLocked(part)
}

func (a *Assembler) acceptLocked(part stream.Part) ([][]byte, error) {
	container, payload, err := FirstPayload(part.Data)
	if err != nil {
		ready, flushErr := a.flushReadyLocked()
		if flushErr != nil {
			return ready, flushErr
		}
		return ready, err
	}

	if part.ChannelID != 0 && part.Quality != stream.QualityFull {
		return a.flushReadyLocked()
	}

	if part.Kind == stream.PartKindUnified {
		a.syncOutputTimelineLocked(part.TimestampMS)
		result, err := remux.PayloadToMPEGTS(container, payload, a.tsOffset, a.logoPath)
		if err != nil {
			ready, _ := a.flushReadyLocked()
			return ready, err
		}
		a.tsOffset += result.Duration
		a.lastPartTS = part.TimestampMS
		out := [][]byte{result.MPEGTS}
		ready, err := a.flushReadyLocked()
		out = append(out, ready...)
		return out, err
	}

	if part.ChannelID == 0 {
		a.separateAV = true
		if !part.ExpectVideoPartner {
			return a.flushReadyLocked()
		}
		p := a.pendingForLocked(part.TimestampMS)
		p.audio = &pendingSlice{container: container, payload: payload}
		a.evictPendingIfNeededLocked()
		return a.flushReadyLocked()
	}

	// Non-zero channel is always separate A/V; never emit video-only unified remux.
	a.separateAV = true
	if a.primaryVideoChannel == 0 {
		a.primaryVideoChannel = part.ChannelID
	} else if part.ChannelID != a.primaryVideoChannel {
		return a.flushReadyLocked()
	}

	p := a.pendingForLocked(part.TimestampMS)
	p.video = &pendingSlice{container: container, payload: payload}
	a.evictPendingIfNeededLocked()
	return a.flushReadyLocked()
}

func (a *Assembler) pendingForLocked(ts int64) *pendingSegment {
	p, ok := a.pending[ts]
	if !ok {
		p = &pendingSegment{}
		a.pending[ts] = p
	}
	return p
}

// evictPendingIfNeededLocked drops oldest incomplete pairs when over cap.
func (a *Assembler) evictPendingIfNeededLocked() {
	for len(a.pending) > maxPendingSegments {
		var oldest int64
		found := false
		for ts, p := range a.pending {
			if p.audio != nil && p.video != nil {
				continue
			}
			if !found || ts < oldest {
				oldest = ts
				found = true
			}
		}
		if !found {
			return
		}
		delete(a.pending, oldest)
	}
}

func (a *Assembler) flushReadyLocked() ([][]byte, error) {
	var out [][]byte
	var ready []int64
	for ts, p := range a.pending {
		if p.audio != nil && p.video != nil {
			ready = append(ready, ts)
		}
	}
	slices.Sort(ready)
	for _, ts := range ready {
		p := a.pending[ts]
		chunks, err := a.flushSegmentLocked(ts, p)
		if err != nil {
			delete(a.pending, ts)
			return out, err
		}
		out = append(out, chunks...)
		delete(a.pending, ts)
	}
	return out, nil
}

func (a *Assembler) flushSegmentLocked(ts int64, p *pendingSegment) ([][]byte, error) {
	if p.audio == nil || p.video == nil {
		return nil, nil
	}
	a.syncOutputTimelineLocked(ts)
	result, err := remux.MuxAV(
		p.video.container, p.video.payload,
		p.audio.container, p.audio.payload,
		a.tsOffset, a.logoPath,
	)
	if err != nil {
		return nil, err
	}
	a.tsOffset += result.Duration
	a.lastPartTS = ts
	return [][]byte{result.MPEGTS}, nil
}

// syncOutputTimelineLocked advances MPEG-TS offset when Telegram media timestamps
// jump (unified resync keeps old segments then appends a new branch). The native
// client resets PTS per 1 s fragment; we must not map a +30 s content cliff into
// +1 s of output timeline.
func (a *Assembler) syncOutputTimelineLocked(nextTS int64) {
	if a.lastPartTS <= 0 {
		return
	}
	delta := nextTS - a.lastPartTS
	if delta > stream.SegmentDurationMS {
		a.tsOffset += float64(delta-stream.SegmentDurationMS) / 1000.0
	}
}
