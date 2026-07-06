package stream

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"go.uber.org/zap"
)

func TestSchedulerConcurrentResetWhileRunning(t *testing.T) {
	mt := &MTProto{}
	client := NewClient(mt, &tg.User{}, tg.InputGroupCall{ID: 1, AccessHash: 1}, true)
	sched := NewScheduler(client, zap.NewNop())

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		sched.Run(ctx)
		close(runDone)
	}()

	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sched.ResetAfterRejoin()
			_ = sched.Generation()
			_ = sched.BootstrapBackoffRemaining()
		}()
	}
	wg.Wait()

	cancel()
	<-runDone
	for range sched.Parts() {
	}
}

func TestDownloadRegistryConcurrentStartFinish(t *testing.T) {
	reg := newDownloadRegistry()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, id := reg.start(context.Background())
			reg.finish(id)
		}()
	}
	wg.Wait()
	reg.cancelAll()
}

func TestSchedulerGenerationMonotonicAcrossResync(t *testing.T) {
	mt := &MTProto{}
	client := NewClient(mt, &tg.User{}, tg.InputGroupCall{ID: 1, AccessHash: 1}, true)
	sched := NewScheduler(client, zap.NewNop())

	before := sched.Generation()
	sched.ResetAfterRejoin()
	after := sched.Generation()
	if after <= before {
		t.Fatalf("generation did not advance: before=%d after=%d", before, after)
	}
}

func TestSchedulerRecoverOutputClearsBuffer(t *testing.T) {
	mt := &MTProto{}
	client := NewClient(mt, &tg.User{}, tg.InputGroupCall{ID: 1, AccessHash: 1}, true)
	sched := NewScheduler(client, zap.NewNop())

	sched.mu.Lock()
	sched.available = []Part{{TimestampMS: 1000}}
	sched.mu.Unlock()

	gen := sched.RecoverOutput()
	if gen <= 0 {
		t.Fatalf("generation = %d", gen)
	}
	sched.mu.Lock()
	defer sched.mu.Unlock()
	if len(sched.available) != 0 {
		t.Fatalf("available len = %d, want 0", len(sched.available))
	}
}

func TestSchedulerResyncKeepsAvailableAndNotifies(t *testing.T) {
	mt := &MTProto{}
	client := NewClient(mt, &tg.User{}, tg.InputGroupCall{ID: 1, AccessHash: 1}, true)
	sched := NewScheduler(client, zap.NewNop())

	var hookGen int
	sched.SetHooks(SchedulerHooks{
		OnResync: func(gen int) { hookGen = gen },
	})

	sched.mu.Lock()
	sched.available = []Part{{TimestampMS: 3000, ResyncGen: 0}}
	before := sched.gen
	sched.handleResync(3106000)
	sched.mu.Unlock()

	if hookGen != before+1 {
		t.Fatalf("hook gen = %d, want %d", hookGen, before+1)
	}
	if sched.nextSegmentTimestamp != -1 {
		t.Fatalf("next_ms = %d, want -1 for unified resync", sched.nextSegmentTimestamp)
	}
	sched.mu.Lock()
	defer sched.mu.Unlock()
	if len(sched.available) != 1 {
		t.Fatalf("available len = %d, want 1 after telegram resync", len(sched.available))
	}
	if sched.postResync {
		t.Fatal("postResync must not be set after telegram resync")
	}
}

func TestSchedulerResyncEmitsBufferedParts(t *testing.T) {
	mt := &MTProto{}
	client := NewClient(mt, &tg.User{}, tg.InputGroupCall{ID: 1, AccessHash: 1}, true)
	sched := NewScheduler(client, zap.NewNop())
	ctx := context.Background()

	sched.mu.Lock()
	sched.playbackRefTime = time.Now().Add(-1100 * time.Millisecond)
	sched.available = []Part{{TimestampMS: 3106000, ResyncGen: 0}}
	sched.handleResync(3106000)
	sched.render(ctx)
	sched.mu.Unlock()

	select {
	case p := <-sched.out:
		if p.TimestampMS != 3106000 {
			t.Fatalf("emitted ts = %d, want 3106000", p.TimestampMS)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected buffered part emit after telegram resync")
	}
}

func TestSchedulerResyncEmitsWithoutRebufferDelay(t *testing.T) {
	mt := &MTProto{}
	client := NewClient(mt, &tg.User{}, tg.InputGroupCall{ID: 1, AccessHash: 1}, true)
	sched := NewScheduler(client, zap.NewNop())
	ctx := context.Background()

	sched.mu.Lock()
	sched.available = []Part{{TimestampMS: 3106000, ResyncGen: 0}}
	sched.handleResync(3106000)
	sched.render(ctx)
	if sched.waitBufferedMS != 0 {
		t.Fatalf("waitBufferedMS = %d with buffered parts, want 0", sched.waitBufferedMS)
	}
	sched.playbackRefTime = time.Now().Add(-1100 * time.Millisecond)
	sched.mu.Unlock()

	done := make(chan struct{})
	go func() {
		sched.mu.Lock()
		sched.render(ctx)
		sched.mu.Unlock()
		close(done)
	}()

	select {
	case <-sched.out:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected immediate part emit after resync with buffered parts")
	}
	<-done
}

func TestSchedulerRunCtxCancelledOnShutdown(t *testing.T) {
	mt := &MTProto{}
	client := NewClient(mt, &tg.User{}, tg.InputGroupCall{ID: 1, AccessHash: 1}, true)
	sched := NewScheduler(client, zap.NewNop())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		sched.Run(ctx)
		close(done)
	}()

	cancel()
	<-done

	if sched.bgCtx().Err() == nil {
		t.Fatal("expected scheduler run context cancelled after shutdown")
	}
}
