package stream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

// MTProto wraps default and stream-DC Telegram API access.
//
// Official client (LivePlayer.getCallStreamDatacenterId + ConnectionTypeDownload):
// every stream RPC targets call.stream_dc_id; auth is exported to that DC and
// requests wait/retry there — never routed via the main datacenter.
//
// Multi-stream: each ingest Client passes its own streamDC per RPC. Cached DC
// pools (dcClients) are keyed by dcID and shared across ingests on the same DC —
// same idea as the official ConnectionsManager keeping one Datacenter object per
// DC with ConnectionTypeDownload pools, though the official app only plays one
// live stream at a time so this sharing is mostly relevant for us.
type MTProto struct {
	Default  *tg.Client
	Telegram *telegram.Client

	dcMu       sync.Mutex
	dcExportMu sync.Mutex // serializes Telegram.DC auth export (multi-stream)
	dcClients  map[int]*dcConn
	dcWait     map[int]time.Time // do not re-attempt export/connect until
}

type dcConn struct {
	client  *tg.Client
	invoker tg.Invoker
}

// NewMTProto builds shared Telegram transport helpers for all ingests.
func NewMTProto(defaultClient *tg.Client, telegram *telegram.Client) *MTProto {
	return &MTProto{
		Default:  defaultClient,
		Telegram: telegram,
	}
}

// streamDCWait means the stream datacenter is not ready yet (export flood or pending retry).
type streamDCWait struct {
	dcID  int
	until time.Time
}

func (e streamDCWait) Error() string {
	return fmt.Sprintf("stream DC %d not ready until %s", e.dcID, e.until.Format(time.RFC3339))
}

func (e streamDCWait) RetryAfter() time.Duration {
	d := time.Until(e.until)
	if d < 0 {
		return 0
	}
	return d
}

func asStreamDCWait(err error) (streamDCWait, bool) {
	var w streamDCWait
	if errors.As(err, &w) {
		return w, true
	}
	if err != nil {
		w, ok := err.(streamDCWait)
		return w, ok
	}
	return streamDCWait{}, false
}

func streamDCWaitDelay(err error) (time.Duration, bool) {
	fw, ok := asStreamDCWait(err)
	if !ok {
		return 0, false
	}
	delay := fw.RetryAfter()
	if delay < time.Second {
		delay = time.Second
	}
	return delay, true
}

// StreamDCWaitRemaining returns time until a stream DC export/connect can be retried.
func (m *MTProto) StreamDCWaitRemaining(dcID int) (time.Duration, bool) {
	if m == nil || dcID <= 0 {
		return 0, false
	}
	m.dcMu.Lock()
	defer m.dcMu.Unlock()
	until, ok := m.waitUntilLocked(dcID)
	if !ok {
		return 0, false
	}
	return time.Until(until), true
}

func (m *MTProto) waitUntilLocked(dcID int) (time.Time, bool) {
	if m.dcWait == nil {
		return time.Time{}, false
	}
	until, ok := m.dcWait[dcID]
	if !ok || !time.Now().Before(until) {
		return time.Time{}, false
	}
	return until, true
}

func (m *MTProto) deferStreamDC(dcID int, wait time.Duration) streamDCWait {
	if wait < time.Second {
		wait = time.Second
	}
	until := time.Now().Add(wait)
	m.dcMu.Lock()
	if m.dcWait == nil {
		m.dcWait = make(map[int]time.Time)
	}
	if prev, ok := m.dcWait[dcID]; !ok || until.After(prev) {
		m.dcWait[dcID] = until
	} else {
		until = prev
	}
	m.dcMu.Unlock()
	return streamDCWait{dcID: dcID, until: until}
}

func (m *MTProto) streamClient(ctx context.Context, dcID int) (*tg.Client, error) {
	if m == nil || m.Default == nil {
		return nil, ErrNotReady
	}
	// No stream_dc_id flag — official sends to default datacenter.
	if dcID <= 0 || m.Telegram == nil {
		return m.Default, nil
	}
	// gotd Client.DC() always calls auth.exportAuthorization, which Telegram
	// rejects with DC_ID_INVALID when target equals the session's primary DC.
	// Official ConnectionsManager skips export when id == currentDatacenterId;
	// use a shared download pool on the session DC instead (Telegram.Pool).
	if m.Telegram.Config().ThisDC == dcID {
		return m.sameDCPool(ctx, dcID)
	}

	m.dcMu.Lock()
	if until, ok := m.waitUntilLocked(dcID); ok {
		m.dcMu.Unlock()
		return nil, streamDCWait{dcID: dcID, until: until}
	}
	if m.dcClients == nil {
		m.dcClients = make(map[int]*dcConn)
	}
	if conn, ok := m.dcClients[dcID]; ok {
		m.dcMu.Unlock()
		return conn.client, nil
	}
	m.dcMu.Unlock()

	// Official client only plays one live stream; concurrent ingests can hit
	// auth.exportAuthorization FLOOD_WAIT if exports run in parallel.
	m.dcExportMu.Lock()
	defer m.dcExportMu.Unlock()

	m.dcMu.Lock()
	if until, ok := m.waitUntilLocked(dcID); ok {
		m.dcMu.Unlock()
		return nil, streamDCWait{dcID: dcID, until: until}
	}
	if conn, ok := m.dcClients[dcID]; ok {
		m.dcMu.Unlock()
		return conn.client, nil
	}
	m.dcMu.Unlock()

	invoker, err := m.Telegram.DC(ctx, dcID, 1)
	if err != nil {
		if wait, ok := tgerr.AsFloodWait(err); ok {
			return nil, m.deferStreamDC(dcID, wait)
		}
		return nil, fmt.Errorf("stream DC %d connect: %w", dcID, errors.Join(m.deferStreamDC(dcID, time.Second), err))
	}
	client := tg.NewClient(invoker)

	m.dcMu.Lock()
	if conn, ok := m.dcClients[dcID]; ok {
		m.dcMu.Unlock()
		closeInvoker(invoker)
		return conn.client, nil
	}
	m.dcClients[dcID] = &dcConn{client: client, invoker: invoker}
	m.dcMu.Unlock()
	return client, nil
}

func (m *MTProto) sameDCPool(ctx context.Context, dcID int) (*tg.Client, error) {
	m.dcMu.Lock()
	if m.dcClients == nil {
		m.dcClients = make(map[int]*dcConn)
	}
	if conn, ok := m.dcClients[dcID]; ok {
		m.dcMu.Unlock()
		return conn.client, nil
	}
	m.dcMu.Unlock()

	invoker, err := m.Telegram.Pool(1)
	if err != nil {
		return nil, fmt.Errorf("stream DC %d pool: %w", dcID, err)
	}
	client := tg.NewClient(invoker)

	m.dcMu.Lock()
	if conn, ok := m.dcClients[dcID]; ok {
		m.dcMu.Unlock()
		closeInvoker(invoker)
		return conn.client, nil
	}
	m.dcClients[dcID] = &dcConn{client: client, invoker: invoker}
	m.dcMu.Unlock()
	return client, nil
}

func closeInvoker(inv tg.Invoker) {
	if c, ok := inv.(interface{ Close() error }); ok {
		_ = c.Close()
	}
}

// Close releases cached stream-DC connection pools. Call after all ingests have stopped.
func (m *MTProto) Close() {
	if m == nil {
		return
	}
	m.dcExportMu.Lock()
	defer m.dcExportMu.Unlock()

	m.dcMu.Lock()
	defer m.dcMu.Unlock()
	for _, conn := range m.dcClients {
		closeInvoker(conn.invoker)
	}
	m.dcClients = nil
	m.dcWait = nil
}

func (m *MTProto) UploadGetFile(ctx context.Context, dcID int, req *tg.UploadGetFileRequest) (tg.UploadFileClass, error) {
	client, err := m.streamClient(ctx, dcID)
	if err != nil {
		return nil, err
	}
	// upload.getFile FLOOD_WAIT is NotReady in the official client (LivePlayer.java +
	// StreamingMediaContext.cpp) — retry in 100ms, not after the full flood duration.
	// deferStreamDC is only for auth.exportAuthorization / stream-DC connect flood waits.
	return client.UploadGetFile(ctx, req)
}

func (m *MTProto) PhoneGetGroupCallStreamChannels(ctx context.Context, dcID int, call tg.InputGroupCall) (*tg.PhoneGroupCallStreamChannels, error) {
	client, err := m.streamClient(ctx, dcID)
	if err != nil {
		return nil, err
	}
	channels, err := client.PhoneGetGroupCallStreamChannels(ctx, &call)
	if err != nil {
		if wait, ok := tgerr.AsFloodWait(err); ok {
			return nil, m.deferStreamDC(dcID, wait)
		}
	}
	return channels, err
}
