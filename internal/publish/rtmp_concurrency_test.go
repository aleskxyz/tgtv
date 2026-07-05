package publish

import (
	"sync"
	"testing"
)

func TestPublisherConcurrentResetAndDiscontinuity(t *testing.T) {
	p := NewPublisher("test", "rtmp://127.0.0.1/nope", "http://127.0.0.1/nope", "secret", nil)

	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.Reset()
			p.ConsumeDiscontinuity()
		}()
	}
	wg.Wait()
	p.Stop()
}

func TestPublisherStopIsIdempotent(t *testing.T) {
	p := NewPublisher("test", "rtmp://127.0.0.1/nope", "http://127.0.0.1/nope", "secret", nil)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.Stop()
		}()
	}
	wg.Wait()
}

func TestPublisherWriteAfterStop(t *testing.T) {
	p := NewPublisher("test", "rtmp://127.0.0.1/nope", "http://127.0.0.1/nope", "secret", nil)
	p.Stop()
	if err := p.Write([]byte{0x00}); err == nil {
		t.Fatal("expected write after stop to fail")
	}
}
