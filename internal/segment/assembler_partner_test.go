package segment

import (
	"encoding/binary"
	"testing"

	"github.com/aleskxyz/tgtv/internal/stream"
)

func TestAssemblerWaitsForVideoWhenPartnerExpected(t *testing.T) {
	a := NewAssembler("")
	a.separateAV = true
	a.pending[1000] = &pendingSegment{
		audio: &pendingSlice{container: "ogg", payload: []byte{1}},
	}
	out, err := a.flushReadyLocked()
	if len(out) != 0 {
		t.Fatalf("expected no output while waiting for video, got %d chunks", len(out))
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := a.pending[1000]; !ok {
		t.Fatal("pending segment should remain until video arrives")
	}
}

func TestAssemblerBuffersVideoBeforeAudio(t *testing.T) {
	a := NewAssembler("")
	part := stream.Part{
		TimestampMS: 1000,
		ChannelID:   1,
		Quality:     stream.QualityFull,
		Kind:        stream.PartKindVideo,
		Data:        mustBroadcastWrapper(t, "mp4", []byte{1, 2, 3}),
	}
	out, err := a.Accept(part)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Fatalf("expected no RTMP output before audio, got %d chunks", len(out))
	}
	if !a.separateAV {
		t.Fatal("expected separate A/V mode")
	}
	if _, ok := a.pending[1000]; !ok {
		t.Fatal("video should be buffered pending audio partner")
	}
}

func TestAssemblerEvictsOldestIncompletePending(t *testing.T) {
	a := NewAssembler("")
	a.separateAV = true
	for i := range maxPendingSegments + 2 {
		ts := int64(i * 1000)
		a.pending[ts] = &pendingSegment{
			audio: &pendingSlice{container: "ogg", payload: []byte{byte(i)}},
		}
	}
	a.evictPendingIfNeededLocked()
	if len(a.pending) > maxPendingSegments {
		t.Fatalf("pending len = %d want <= %d", len(a.pending), maxPendingSegments)
	}
}

func TestAssemblerDiscardsBootstrapAudioWithoutVideoPartner(t *testing.T) {
	a := NewAssembler("")
	part := stream.Part{
		TimestampMS:        1000,
		ChannelID:          0,
		ExpectVideoPartner: false,
		Data:               mustBroadcastWrapper(t, "ogg", []byte{1, 2, 3}),
	}
	out, err := a.Accept(part)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Fatalf("expected no RTMP output, got %d chunks", len(out))
	}
	if _, ok := a.pending[1000]; ok {
		t.Fatal("bootstrap audio should not be retained")
	}
	if !a.separateAV {
		t.Fatal("expected separate A/V mode")
	}
}

func TestAssemblerOnlyFlushesMuxedPairs(t *testing.T) {
	a := NewAssembler("")
	a.separateAV = true
	a.pending[1000] = &pendingSegment{
		audio: &pendingSlice{container: "ogg", payload: []byte{1}},
	}
	a.pending[2000] = &pendingSegment{
		audio: &pendingSlice{container: "ogg", payload: []byte{2}},
		video: &pendingSlice{container: "mp4", payload: []byte{3}},
	}
	var ready int
	for _, p := range a.pending {
		if p.audio != nil && p.video != nil {
			ready++
		}
	}
	if ready != 1 {
		t.Fatalf("ready count = %d want 1", ready)
	}
}

func mustBroadcastWrapper(t *testing.T, container string, payload []byte) []byte {
	t.Helper()
	return buildMinimalPart(container, payload)
}

func buildMinimalPart(container string, payload []byte) []byte {
	var buf []byte
	sig := make([]byte, 4)
	binary.LittleEndian.PutUint32(sig, 0xA12E810D)
	buf = append(buf, sig...)

	cBytes := []byte(container)
	buf = append(buf, byte(len(cBytes)))
	buf = append(buf, cBytes...)
	for len(buf)%4 != 0 {
		buf = append(buf, 0)
	}

	mask := make([]byte, 4)
	binary.LittleEndian.PutUint32(mask, 0)
	buf = append(buf, mask...)

	events := make([]byte, 4)
	binary.LittleEndian.PutUint32(events, 1)
	buf = append(buf, events...)

	offset := make([]byte, 4)
	binary.LittleEndian.PutUint32(offset, 0)
	buf = append(buf, offset...)

	ep := []byte("e")
	buf = append(buf, byte(len(ep)))
	buf = append(buf, ep...)
	for len(buf)%4 != 0 {
		buf = append(buf, 0)
	}

	rot := make([]byte, 4)
	binary.LittleEndian.PutUint32(rot, 0)
	buf = append(buf, rot...)
	extra := make([]byte, 4)
	binary.LittleEndian.PutUint32(extra, 0)
	buf = append(buf, extra...)

	buf = append(buf, payload...)
	return buf
}
