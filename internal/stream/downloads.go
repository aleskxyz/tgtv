package stream

import (
	"context"
	"sync"
	"sync/atomic"
)

// downloadRegistry tracks in-flight getFile requests for cancellation
// (LivePlayer.java currentStreamRequestTimestamp + onCancelRequestBroadcastPart).
type downloadRegistry struct {
	mu     sync.Mutex
	nextID atomic.Int64
	cancel map[int64]context.CancelFunc
}

func newDownloadRegistry() *downloadRegistry {
	return &downloadRegistry{cancel: make(map[int64]context.CancelFunc)}
}

func (r *downloadRegistry) start(parent context.Context) (context.Context, int64) {
	ctx, cancel := context.WithCancel(parent)
	id := r.nextID.Add(1)
	r.mu.Lock()
	r.cancel[id] = cancel
	r.mu.Unlock()
	return ctx, id
}

func (r *downloadRegistry) finish(id int64) {
	r.mu.Lock()
	if cancel, ok := r.cancel[id]; ok {
		cancel()
		delete(r.cancel, id)
	}
	r.mu.Unlock()
}

func (r *downloadRegistry) cancelAll() {
	r.mu.Lock()
	for id, cancel := range r.cancel {
		cancel()
		delete(r.cancel, id)
	}
	r.mu.Unlock()
}
