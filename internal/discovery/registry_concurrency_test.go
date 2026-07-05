package discovery_test

import (
	"sync"
	"testing"

	"github.com/aleskxyz/tgtv/internal/discovery"
)

func TestRegistryConcurrentSnapshotAndUpdate(t *testing.T) {
	r := discovery.NewRegistry()
	r.Upsert(1, 10, "News", 99)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = r.SnapshotByChat(1)
			r.UpdateTitle(1, "News Live")
			_ = r.ActiveLives()
		}()
	}
	wg.Wait()
}
