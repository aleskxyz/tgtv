package discovery

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
)

type LiveStatus string

const (
	StatusDiscovered LiveStatus = "discovered"
	StatusIngesting  LiveStatus = "ingesting"
	StatusStreaming  LiveStatus = "streaming"
	StatusEnded      LiveStatus = "ended"
)

type LiveEntry struct {
	StreamID       string
	ChatID         int64
	CallID         int64
	CallAccessHash int64
	Title          string
	Status         LiveStatus
}

// MakeStreamID returns a stable stream identifier for a chat.
// The same chat always maps to the same stream link, regardless of call id or live sessions.
func MakeStreamID(chatID int64) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d", chatID)))
	return hex.EncodeToString(sum[:])[:12]
}

type Registry struct {
	mu            sync.RWMutex
	byID          map[string]*LiveEntry
	byChat        map[int64]string
	channelAccess map[int64]int64
}

func NewRegistry() *Registry {
	return &Registry{
		byID:   make(map[string]*LiveEntry),
		byChat: make(map[int64]string),
	}
}

func (r *Registry) Upsert(chatID, callID int64, title string, accessHash int64) (LiveEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	streamID := MakeStreamID(chatID)

	if existing, ok := r.byID[streamID]; ok {
		callSuperseded := existing.CallID != 0 && callID != 0 && existing.CallID != callID
		existing.CallID = callID
		existing.Title = mergeTitle(existing.Title, title)
		if accessHash != 0 {
			existing.CallAccessHash = accessHash
		}
		if existing.Status == StatusEnded {
			existing.Status = StatusDiscovered
		}
		r.byChat[chatID] = streamID
		return *existing, callSuperseded
	}

	entry := &LiveEntry{
		StreamID:       streamID,
		ChatID:         chatID,
		CallID:         callID,
		CallAccessHash: accessHash,
		Title:          title,
		Status:         StatusDiscovered,
	}
	r.byID[streamID] = entry
	r.byChat[chatID] = streamID
	return *entry, false
}

func (r *Registry) MarkEnded(chatID int64) (LiveEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	streamID, ok := r.byChat[chatID]
	if !ok {
		return LiveEntry{}, false
	}
	entry, ok := r.byID[streamID]
	if !ok || entry.Status == StatusEnded {
		return LiveEntry{}, false
	}
	entry.Status = StatusEnded
	return *entry, true
}

// RemoveEndedChat deletes a single ended entry for chatID.
func (r *Registry) RemoveEndedChat(chatID int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	streamID, ok := r.byChat[chatID]
	if !ok {
		return
	}
	entry, ok := r.byID[streamID]
	if !ok || entry.Status != StatusEnded {
		return
	}
	delete(r.byID, streamID)
	delete(r.byChat, chatID)
}

func (r *Registry) Get(streamID string) (LiveEntry, bool) {
	return r.Snapshot(streamID)
}

// Snapshot returns a copy of an active live entry safe for use without holding the registry lock.
func (r *Registry) Snapshot(streamID string) (LiveEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.byID[streamID]
	if !ok || entry.Status == StatusEnded {
		return LiveEntry{}, false
	}
	return *entry, true
}

// SnapshotByChat returns a copy of an active live entry for chatID.
func (r *Registry) SnapshotByChat(chatID int64) (LiveEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	streamID, ok := r.byChat[chatID]
	if !ok {
		return LiveEntry{}, false
	}
	entry, ok := r.byID[streamID]
	if !ok || entry.Status == StatusEnded {
		return LiveEntry{}, false
	}
	return *entry, true
}

// UpdateCallInfo updates call metadata for an active entry under the registry lock.
func (r *Registry) UpdateCallInfo(streamID string, callID, accessHash int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.byID[streamID]
	if !ok || entry.Status == StatusEnded {
		return false
	}
	entry.CallID = callID
	if accessHash != 0 {
		entry.CallAccessHash = accessHash
	}
	return true
}

// Reactivate marks a ended entry discovered again when the call is still live.
func (r *Registry) Reactivate(streamID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry, ok := r.byID[streamID]; ok && entry.Status == StatusEnded {
		entry.Status = StatusDiscovered
	}
}

// UpdateTitle updates the display title for an active live entry. Returns true when changed.
func (r *Registry) UpdateTitle(chatID int64, title string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	streamID, ok := r.byChat[chatID]
	if !ok {
		return false
	}
	entry, ok := r.byID[streamID]
	if !ok || entry.Status == StatusEnded {
		return false
	}
	merged := mergeTitle(entry.Title, title)
	if merged == entry.Title {
		return false
	}
	entry.Title = merged
	return true
}

func (r *Registry) SetStatus(streamID string, status LiveStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry, ok := r.byID[streamID]; ok {
		entry.Status = status
	}
}

func (r *Registry) ActiveLives() []LiveEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]LiveEntry, 0)
	for _, e := range r.byID {
		if e.Status != StatusEnded {
			out = append(out, *e)
		}
	}
	return out
}

// RememberChannelAccess stores a channel access hash for later entity lookups.
func (r *Registry) RememberChannelAccess(chatID, accessHash int64) {
	if accessHash == 0 {
		return
	}
	r.mu.Lock()
	if r.channelAccess == nil {
		r.channelAccess = make(map[int64]int64)
	}
	r.channelAccess[chatID] = accessHash
	r.mu.Unlock()
}

// ForgetChannelAccess removes a cached channel access hash.
func (r *Registry) ForgetChannelAccess(chatID int64) {
	r.mu.Lock()
	delete(r.channelAccess, chatID)
	r.mu.Unlock()
}

// ChannelAccessHash returns a cached channel access hash when known.
func (r *Registry) ChannelAccessHash(chatID int64) (int64, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.channelAccess == nil {
		return 0, false
	}
	hash, ok := r.channelAccess[chatID]
	return hash, ok && hash != 0
}

func (r *Registry) snapshotLocked(streamID string) (LiveEntry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.byID[streamID]
	if !ok || entry.Status == StatusEnded {
		return LiveEntry{}, fmt.Errorf("stream no longer active")
	}
	return *entry, nil
}

func mergeTitle(existing, incoming string) string {
	incoming = strings.TrimSpace(incoming)
	if incoming == "" {
		return existing
	}
	if existing == "" {
		return incoming
	}
	if strings.EqualFold(existing, incoming) {
		return existing
	}
	return incoming
}
