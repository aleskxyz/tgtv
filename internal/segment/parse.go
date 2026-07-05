package segment

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/aleskxyz/tgtv/internal/media"
)

const streamSignature = 0xA12E810D

type StreamEvent struct {
	Offset     int32
	EndpointID string
	Rotation   int32
	Extra      int32
}

type ParsedSegment struct {
	Container  string
	ActiveMask int32
	Events     []StreamEvent
	Payloads   [][]byte
}

func ParseBroadcastPart(data []byte) (*ParsedSegment, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty part")
	}
	buf := bytes.NewReader(data)

	var sig uint32
	if err := binary.Read(buf, binary.LittleEndian, &sig); err != nil {
		return nil, err
	}
	if sig != streamSignature {
		return nil, fmt.Errorf("bad signature %x", sig)
	}

	container, err := readString(buf)
	if err != nil {
		return nil, err
	}

	var activeMask int32
	if err := binary.Read(buf, binary.LittleEndian, &activeMask); err != nil {
		return nil, err
	}

	var eventCount int32
	if err := binary.Read(buf, binary.LittleEndian, &eventCount); err != nil {
		return nil, err
	}
	if eventCount <= 0 {
		return nil, fmt.Errorf("no events")
	}
	const maxStreamEvents = 64
	if eventCount > maxStreamEvents {
		return nil, fmt.Errorf("too many stream events: %d", eventCount)
	}

	events := make([]StreamEvent, 0, eventCount)
	for i := int32(0); i < eventCount; i++ {
		ev, err := readEvent(buf)
		if err != nil {
			return nil, err
		}
		events = append(events, ev)
	}

	headerLen := len(data) - buf.Len()
	media := data[headerLen:]
	payloads := make([][]byte, 0, 1)
	first := events[0]
	if first.Offset >= 0 && int(first.Offset) < len(media) {
		payloads = append(payloads, media[first.Offset:])
	} else if len(media) > 0 {
		payloads = append(payloads, media)
	}

	return &ParsedSegment{
		Container:  container,
		ActiveMask: activeMask,
		Events:     events,
		Payloads:   payloads,
	}, nil
}

func readEvent(r *bytes.Reader) (StreamEvent, error) {
	var ev StreamEvent
	if err := binary.Read(r, binary.LittleEndian, &ev.Offset); err != nil {
		return ev, err
	}
	endpoint, err := readString(r)
	if err != nil {
		return ev, err
	}
	ev.EndpointID = endpoint
	if err := binary.Read(r, binary.LittleEndian, &ev.Rotation); err != nil {
		return ev, err
	}
	if err := binary.Read(r, binary.LittleEndian, &ev.Extra); err != nil {
		return ev, err
	}
	return ev, nil
}

func readString(r *bytes.Reader) (string, error) {
	first, err := r.ReadByte()
	if err != nil {
		return "", err
	}
	var length int
	var padding int
	if first == 254 {
		lenBuf := make([]byte, 3)
		if _, err := r.Read(lenBuf); err != nil {
			return "", err
		}
		length = int(lenBuf[0]) | int(lenBuf[1])<<8 | int(lenBuf[2])<<16
		padding = (4 - length%4) % 4
	} else {
		length = int(first)
		padding = (4 - (length+1)%4) % 4
	}
	if length < 0 || length > math.MaxUint16 {
		return "", fmt.Errorf("invalid string length")
	}
	buf := make([]byte, length)
	if _, err := r.Read(buf); err != nil {
		return "", err
	}
	if padding > 0 {
		skip := make([]byte, padding)
		_, _ = r.Read(skip)
	}
	return string(buf), nil
}

func FirstPayload(data []byte) (container string, payload []byte, err error) {
	parsed, err := ParseBroadcastPart(data)
	if err != nil {
		return "", nil, err
	}
	if len(parsed.Payloads) == 0 || len(parsed.Payloads[0]) == 0 {
		return "", nil, fmt.Errorf("no payload")
	}
	return media.NormalizeContainer(parsed.Container), parsed.Payloads[0], nil
}
