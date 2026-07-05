package stream

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
)

type closeTrackingInvoker struct {
	closed atomic.Int32
}

func (c *closeTrackingInvoker) Invoke(_ context.Context, _ bin.Encoder, _ bin.Decoder) error {
	return errors.New("mock invoker")
}

func (c *closeTrackingInvoker) Close() error {
	c.closed.Add(1)
	return nil
}

func TestMTProtoCloseReleasesPools(t *testing.T) {
	inv1 := &closeTrackingInvoker{}
	inv2 := &closeTrackingInvoker{}
	mt := &MTProto{
		dcClients: map[int]*dcConn{
			4: {client: tg.NewClient(inv1), invoker: inv1},
			5: {client: tg.NewClient(inv2), invoker: inv2},
		},
		dcWait: map[int]time.Time{4: time.Now().Add(time.Minute)},
	}

	mt.Close()

	if got := inv1.closed.Load(); got != 1 {
		t.Fatalf("dc 4 invoker close count=%d", got)
	}
	if got := inv2.closed.Load(); got != 1 {
		t.Fatalf("dc 5 invoker close count=%d", got)
	}
	if mt.dcClients != nil {
		t.Fatal("expected dcClients cleared")
	}
	if mt.dcWait != nil {
		t.Fatal("expected dcWait cleared")
	}
}
