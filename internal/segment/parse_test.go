package segment_test

import (
	"encoding/binary"
	"testing"

	"github.com/aleskxyz/tgtv/internal/segment"
)

func TestParseBroadcastPartSignature(t *testing.T) {
	data := buildMinimalPart("mp4", []byte{0, 1, 2, 3})
	parsed, err := segment.ParseBroadcastPart(data)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Container != "mp4" {
		t.Fatalf("container=%q", parsed.Container)
	}
	if len(parsed.Payloads) != 1 || len(parsed.Payloads[0]) != 4 {
		t.Fatalf("payloads=%v", parsed.Payloads)
	}
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

func TestMakeStreamIDStable(t *testing.T) {
	// registry test lives in discovery package
}
