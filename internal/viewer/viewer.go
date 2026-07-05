package viewer

import (
	"context"
	"sync"
	"time"
)

type Manager struct {
	idleGrace time.Duration
	onIdle    func(streamID string)

	mu       sync.Mutex
	last     map[string]time.Time
	stopping sync.Map
	cancel   context.CancelFunc
	done     chan struct{}
}

func NewManager(idleGraceSeconds int, onIdle func(string)) *Manager {
	return &Manager{
		idleGrace: time.Duration(idleGraceSeconds) * time.Second,
		onIdle:    onIdle,
		last:      make(map[string]time.Time),
		done:      make(chan struct{}),
	}
}

func (m *Manager) Start(ctx context.Context) {
	ctx, m.cancel = context.WithCancel(ctx)
	go m.loop(ctx)
}

func (m *Manager) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
	<-m.done
}

func (m *Manager) RecordActivity(streamID string) {
	m.mu.Lock()
	m.last[streamID] = time.Now()
	m.mu.Unlock()
}

func (m *Manager) loop(ctx context.Context) {
	defer close(m.done)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			var idle []string
			m.mu.Lock()
			for id, ts := range m.last {
				if now.Sub(ts) > m.idleGrace {
					delete(m.last, id)
					idle = append(idle, id)
				}
			}
			m.mu.Unlock()
			for _, id := range idle {
				m.mu.Lock()
				_, active := m.last[id]
				m.mu.Unlock()
				if !active {
					go m.stopIdle(id)
				}
			}
		}
	}
}

func (m *Manager) stopIdle(id string) {
	if _, loaded := m.stopping.LoadOrStore(id, struct{}{}); loaded {
		return
	}
	defer m.stopping.Delete(id)
	m.onIdle(id)
}
