package stream

import (
	"context"
	"testing"
	"time"
)

func TestDownloadRegistryFinishReleasesContext(t *testing.T) {
	r := newDownloadRegistry()
	ctx, id := r.start(context.Background())
	done := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(done)
	}()

	r.finish(id)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("context not cancelled after finish")
	}
}
