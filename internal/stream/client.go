package stream

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

var (
	ErrNotReady              = errors.New("chunk not ready")
	ErrRejoinRequired        = errors.New("call rejoin required")
	ErrNoStreams             = errors.New("no streams available")
	ErrCallEnded             = errors.New("group call ended")
	ErrEmptyBroadcasterVideo = errors.New("broadcaster video not found")
)

const groupCallParticipantPage = 50

// Part is one downloaded broadcast fragment.
type Part struct {
	TimestampMS int64
	ChannelID   int
	Quality     int
	Kind        PartKind
	Data        []byte
	ResyncGen   int
	// ExpectVideoPartner mirrors StreamingMediaContext scheduling a video part for
	// the same segment timestamp (separate A/V only).
	ExpectVideoPartner bool
}

// CallInfo is metadata from phone.getGroupCall (LivePlayer poll2).
type CallInfo struct {
	Unified    bool
	StreamDCID int
}

// Client downloads Telegram group-call broadcast chunks via MTProto.
type Client struct {
	api      *MTProto
	self     *tg.User
	call     tg.InputGroupCall
	source   int
	unified  bool
	streamDC int
	dialogID int64
}

func NewClient(api *MTProto, self *tg.User, call tg.InputGroupCall, unified bool) *Client {
	return &Client{api: api, self: self, call: call, unified: unified}
}

func (c *Client) SetStreamDC(dcID int) {
	c.streamDC = dcID
}

func (c *Client) StreamDC() int {
	return c.streamDC
}

// StreamDCWaitRemaining reports remaining flood-wait before stream DC auth export.
func (c *Client) StreamDCWaitRemaining() (time.Duration, bool) {
	if c.api == nil {
		return 0, false
	}
	return c.api.StreamDCWaitRemaining(c.streamDC)
}

// SetDialogID sets the Telegram dialog ID used to find the broadcaster participant
// (LivePlayer.dialogId — negative for channels/supergroups).
func (c *Client) SetDialogID(id int64) { c.dialogID = id }

func (c *Client) Unified() bool { return c.unified }

func (c *Client) fetchGroupCall(ctx context.Context) (*tg.GroupCall, error) {
	result, err := c.api.Default.PhoneGetGroupCall(ctx, &tg.PhoneGetGroupCallRequest{
		Call:  &c.call,
		Limit: 1,
	})
	if err != nil {
		if tgerr.Is(err, "GROUPCALL_INVALID") {
			return nil, ErrCallEnded
		}
		return nil, err
	}
	gc, ok := result.Call.(*tg.GroupCall)
	if !ok || gc.ScheduleDate != 0 {
		return nil, ErrCallEnded
	}
	return gc, nil
}

// CheckCallLive verifies the group call is still active without mutating client routing state.
func (c *Client) CheckCallLive(ctx context.Context) error {
	_, err := c.fetchGroupCall(ctx)
	return err
}

// FetchCallInfo loads group call flags (LivePlayer.java poll2 / prepareForStream).
// Unified mode matches prepareForStream(isRtmpStream): GroupCall.rtmp_stream only.
func (c *Client) FetchCallInfo(ctx context.Context) (*CallInfo, error) {
	gc, err := c.fetchGroupCall(ctx)
	if err != nil {
		return nil, err
	}
	unified := gc.GetRtmpStream()
	dc, _ := gc.GetStreamDCID()
	c.unified = unified
	if dc > 0 {
		c.streamDC = dc
	}
	return &CallInfo{Unified: unified, StreamDCID: dc}, nil
}

// Join attaches with minimal WebRTC params (receive-only), mirroring LivePlayer.init joinGroupCall.
// mySource is the SSRC sent in join params (LivePlayer.java:234-235), used for checkGroupCall.
func (c *Client) Join(ctx context.Context) error {
	ssrc := rand.Uint32()
	for ssrc == 0 {
		ssrc = rand.Uint32()
	}
	payload := tg.DataJSON{
		Data: fmt.Sprintf(
			`{"fingerprints":[],"pwd":"","ssrc":%d,"ssrc-groups":[],"ufrag":""}`,
			ssrc,
		),
	}
	_, err := c.api.Default.PhoneJoinGroupCall(ctx, &tg.PhoneJoinGroupCallRequest{
		Call:         &c.call,
		JoinAs:       c.self.AsInputPeer(),
		Muted:        true,
		VideoStopped: true,
		Params:       payload,
	})
	if err != nil {
		return err
	}
	c.source = int(ssrc)
	_ = c.RefreshJoinSource(ctx)
	return nil
}

// RefreshJoinSource loads our participant source from the call roster, mirroring
// Python call_health.our_group_call_source. The join JSON SSRC is not always the
// source ID phone.checkGroupCall expects.
func (c *Client) RefreshJoinSource(ctx context.Context) error {
	if c.self == nil || c.api == nil {
		return nil
	}
	selfID := c.self.ID

	result, err := c.api.Default.PhoneGetGroupCall(ctx, &tg.PhoneGetGroupCallRequest{
		Call:  &c.call,
		Limit: groupCallParticipantPage,
	})
	if err != nil {
		return err
	}
	if src, ok := ourParticipantSource(result.Participants, selfID); ok {
		c.source = src
		return nil
	}

	offset := ""
	for page := 0; page < 20; page++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		gp, err := c.api.Default.PhoneGetGroupParticipants(ctx, &tg.PhoneGetGroupParticipantsRequest{
			Call:   &c.call,
			Offset: offset,
			Limit:  groupCallParticipantPage,
		})
		if err != nil {
			return err
		}
		if src, ok := ourParticipantSource(gp.Participants, selfID); ok {
			c.source = src
			return nil
		}
		offset = gp.NextOffset
		if offset == "" {
			break
		}
	}
	return nil
}

func ourParticipantSource(participants []tg.GroupCallParticipant, selfUserID int64) (int, bool) {
	for _, p := range participants {
		uid, ok := participantUserID(p)
		if !ok || uid != selfUserID || p.Source == 0 {
			continue
		}
		return p.Source, true
	}
	return 0, false
}

func participantUserID(p tg.GroupCallParticipant) (int64, bool) {
	switch peer := p.Peer.(type) {
	case *tg.PeerUser:
		return peer.UserID, true
	default:
		return 0, false
	}
}

func (c *Client) Leave(ctx context.Context) error {
	if c.source == 0 {
		return nil
	}
	_, err := c.api.Default.PhoneLeaveGroupCall(ctx, &tg.PhoneLeaveGroupCallRequest{
		Source: c.source,
		Call:   &c.call,
	})
	if err != nil {
		return err
	}
	c.source = 0
	return nil
}

func (c *Client) CheckJoin(ctx context.Context) error {
	_ = c.RefreshJoinSource(ctx)
	if c.source == 0 {
		return ErrRejoinRequired
	}
	sources, err := c.api.Default.PhoneCheckGroupCall(ctx, &tg.PhoneCheckGroupCallRequest{
		Call:    &c.call,
		Sources: []int{c.source},
	})
	if err != nil {
		if tgerr.Is(err, "GROUPCALL_JOIN_MISSING") {
			return ErrRejoinRequired
		}
		if tgerr.Is(err, "GROUPCALL_INVALID") {
			return ErrCallEnded
		}
		return err
	}
	if !slices.Contains(sources, c.source) {
		return ErrRejoinRequired
	}
	return nil
}

// RequestCurrentTime — LivePlayer.java requestCurrentTime (lines 517-569).
func (c *Client) RequestCurrentTime(ctx context.Context) (int64, error) {
	channels, err := c.api.PhoneGetGroupCallStreamChannels(ctx, c.streamDC, c.call)
	if err != nil {
		return 0, err
	}
	if len(channels.Channels) == 0 {
		return 0, ErrNoStreams
	}
	if c.unified {
		// LivePlayer.java:532 — channels.get(0).last_timestamp_ms
		return channels.Channels[0].LastTimestampMs, nil
	}
	for _, ch := range channels.Channels {
		if ch.Channel == 0 {
			return ch.LastTimestampMs, nil
		}
	}
	primary, ok := SelectPrimaryStreamChannel(channels.Channels)
	if !ok {
		return 0, ErrNoStreams
	}
	return primary.LastTimestampMs, nil
}

// AdjustBootstrapTimestamp: floor(ts/1000)*1000 - 2000 (StreamingMediaContext.cpp:644).
func AdjustBootstrapTimestamp(timestampMS int64) int64 {
	if timestampMS <= 0 {
		return 0
	}
	adjusted := (timestampMS/SegmentDurationMS)*SegmentDurationMS - SegmentBufferMS
	if adjusted <= 0 {
		return 0
	}
	return adjusted
}

// ResyncNextTimestamp picks the next fetch position after TIME_TOO_SMALL / invalid.
// StreamingMediaContext.cpp:873-881 — unified sets -1 (server bootstrap via
// getGroupCallStreamChannels.last_timestamp_ms); non-unified uses response boundary.
func ResyncNextTimestamp(unified bool, responseMS int64) int64 {
	if unified {
		return -1
	}
	return ResyncBoundary(responseMS)
}

// ResyncBoundary floors response time to 1s grid (StreamingMediaContext.cpp:877-880).
func ResyncBoundary(responseMS int64) int64 {
	if responseMS <= 0 {
		return -1
	}
	return (responseMS / SegmentDurationMS) * SegmentDurationMS
}

func (c *Client) StreamChannels(ctx context.Context) ([]tg.GroupCallStreamChannel, error) {
	channels, err := c.api.PhoneGetGroupCallStreamChannels(ctx, c.streamDC, c.call)
	if err != nil {
		return nil, err
	}
	return channels.Channels, nil
}

// BroadcasterVideo finds the broadcaster participant video endpoint (LivePlayer.java:362-380).
func (c *Client) BroadcasterVideo(ctx context.Context) (endpoint string, sources []int, err error) {
	endpoint, sources, found, err := c.findBroadcasterInGroupCall(ctx, groupCallParticipantPage)
	if err != nil {
		return "", nil, err
	}
	if found {
		if endpoint == "" {
			return "", nil, ErrEmptyBroadcasterVideo
		}
		return endpoint, sources, nil
	}

	offset := ""
	for page := 0; page < 20; page++ {
		if err := ctx.Err(); err != nil {
			return "", nil, err
		}
		result, err := c.api.Default.PhoneGetGroupParticipants(ctx, &tg.PhoneGetGroupParticipantsRequest{
			Call:   &c.call,
			Offset: offset,
			Limit:  groupCallParticipantPage,
		})
		if err != nil {
			return "", nil, err
		}
		for _, p := range result.Participants {
			if c.dialogID != 0 && peerDialogID(p.Peer) != c.dialogID {
				continue
			}
			endpoint, sources = videoFromParticipant(p)
			if endpoint == "" {
				return "", nil, ErrEmptyBroadcasterVideo
			}
			return endpoint, sources, nil
		}
		offset = result.NextOffset
		if offset == "" {
			break
		}
	}
	return "", nil, ErrEmptyBroadcasterVideo
}

func (c *Client) findBroadcasterInGroupCall(ctx context.Context, limit int) (endpoint string, sources []int, found bool, err error) {
	result, err := c.api.Default.PhoneGetGroupCall(ctx, &tg.PhoneGetGroupCallRequest{
		Call:  &c.call,
		Limit: limit,
	})
	if err != nil {
		return "", nil, false, err
	}
	for _, p := range result.Participants {
		if c.dialogID != 0 && peerDialogID(p.Peer) != c.dialogID {
			continue
		}
		endpoint, sources = videoFromParticipant(p)
		return endpoint, sources, true, nil
	}
	return "", nil, false, nil
}

func videoFromParticipant(p tg.GroupCallParticipant) (endpoint string, sources []int) {
	video, ok := p.GetVideo()
	if !ok {
		return "", nil
	}
	endpoint = video.Endpoint
	for _, g := range video.SourceGroups {
		if g.Semantics == "SIM" || g.Semantics == "sim" {
			sources = append(sources, g.Sources...)
		}
	}
	return endpoint, sources
}

func peerDialogID(peer tg.PeerClass) int64 {
	switch p := peer.(type) {
	case *tg.PeerUser:
		return p.UserID
	case *tg.PeerChannel:
		return -p.ChannelID
	default:
		return 0
	}
}

// FetchPart issues upload.getFile exactly like LivePlayer.onRequestBroadcastPart (lines 451-503).
func (c *Client) FetchPart(ctx context.Context, timestampMS, duration int64, part *pendingPart) ([]byte, int64, error) {
	scale := 0
	if duration == 500 {
		scale = 1
	}
	loc := &tg.InputGroupCallStream{
		Call:   &c.call,
		TimeMs: timestampMS,
		Scale:  scale,
	}
	channelID := part.channelID
	switch part.kind {
	case PartKindAudio:
		channelID = 0
	case PartKindUnified, PartKindVideo:
		if channelID != 0 {
			loc.SetVideoChannel(channelID)
			loc.SetVideoQuality(part.quality)
		}
	}

	file, err := c.api.UploadGetFile(ctx, c.streamDC, &tg.UploadGetFileRequest{
		Location:     loc,
		Offset:       0,
		Limit:        GetFileLimitBytes,
		CDNSupported: false,
		// LivePlayer.java TL_upload_getFile — precise flag not set.
		Precise: false,
	})
	responseMS := time.Now().UnixMilli()

	if err != nil {
		switch {
		case tgerr.Is(err, "TIME_TOO_BIG"):
			return nil, responseMS, ErrNotReady
		case tgerr.Is(err, "TIME_TOO_SMALL", "TIME_INVALID", "VIDEO_CHANNEL_INVALID"):
			return nil, responseMS, fmt.Errorf("%w: %v", ErrResyncNeeded, err)
		case tgerr.Is(err, "GROUPCALL_JOIN_MISSING"):
			return nil, responseMS, ErrRejoinRequired
		case tgerr.Is(err, "GROUPCALL_INVALID"):
			return nil, responseMS, ErrCallEnded
		default:
			if _, ok := tgerr.AsFloodWait(err); ok {
				return nil, responseMS, fmt.Errorf("%w: %w", ErrNotReady, err)
			}
			if w, ok := asStreamDCWait(err); ok {
				return nil, responseMS, fmt.Errorf("%w: %w", ErrNotReady, w)
			}
			return nil, responseMS, fmt.Errorf("%w: %w", ErrResyncNeeded, err)
		}
	}

	upload, ok := file.(*tg.UploadFile)
	if !ok {
		return nil, responseMS, ErrNotReady
	}
	return upload.Bytes, responseMS, nil
}

func IsCallEnded(err error) bool {
	return errors.Is(err, ErrCallEnded) || tgerr.Is(err, "GROUPCALL_INVALID")
}
