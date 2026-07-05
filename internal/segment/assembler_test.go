package segment_test

import (
	"sync"
	"testing"

	"github.com/aleskxyz/tgtv/internal/segment"
	"github.com/aleskxyz/tgtv/internal/stream"
)

func TestAssemblerConcurrentLogoUpdate(t *testing.T) {
	a := segment.NewAssembler("/tmp/logo.jpg")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			a.SetLogoPath("/tmp/logo-" + string(rune('0'+n)) + ".jpg")
		}(i)
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = a.Accept(stream.Part{TimestampMS: 1000, ChannelID: 1, Data: []byte{0}})
		}()
	}
	wg.Wait()
}
