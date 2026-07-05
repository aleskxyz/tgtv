package ingest

import (
	"time"

	"github.com/aleskxyz/tgtv/internal/stream"
)

// beginHardRejoin returns false when another rejoin is active or multi-ingest
// policy forbids starting one yet (flood grace, cooldown).
func (s *Session) beginHardRejoin() bool {
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()
	now := time.Now()
	if s.rejoinActive {
		return false
	}
	if now.Before(s.joinGraceUntil) || now.Before(s.floodGraceUntil) {
		return false
	}
	if !s.lastRejoinAt.IsZero() && now.Sub(s.lastRejoinAt) < stream.MinRejoinCooldown {
		return false
	}
	s.rejoinActive = true
	return true
}

func (s *Session) endHardRejoin() {
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()
	s.rejoinActive = false
	s.lastRejoinAt = time.Now()
}

func (s *Session) cancelHardRejoin() {
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()
	s.rejoinActive = false
}

func (s *Session) noteJoinFloodWait(wait time.Duration) {
	if wait <= 0 {
		return
	}
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()
	until := time.Now().Add(wait)
	if until.After(s.joinGraceUntil) {
		s.joinGraceUntil = until
	}
}

func (s *Session) noteFloodGrace(wait time.Duration) {
	if wait <= 0 {
		return
	}
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()
	until := time.Now().Add(wait)
	if until.After(s.floodGraceUntil) {
		s.floodGraceUntil = until
	}
}
