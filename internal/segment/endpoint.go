package segment

// EndpointMappingFromAudio extracts endpoint→channel index from an audio broadcast part.
func EndpointMappingFromAudio(data []byte) map[string]int32 {
	if m := endpointMappingFromOggMetadata(data); len(m) > 0 {
		return m
	}
	parsed, err := ParseBroadcastPart(data)
	if err != nil {
		return nil
	}
	if parsed.Container != "ogg" {
		return nil
	}
	mapping := make(map[string]int32)
	idx := int32(0)
	for _, ev := range parsed.Events {
		if ev.EndpointID == "" {
			continue
		}
		if _, exists := mapping[ev.EndpointID]; exists {
			continue
		}
		mapping[ev.EndpointID] = idx
		idx++
	}
	if len(mapping) == 0 {
		return nil
	}
	return mapping
}

func endpointMappingFromOggMetadata(data []byte) map[string]int32 {
	parsed, err := ParseBroadcastPart(data)
	if err != nil || len(parsed.Payloads) == 0 {
		return nil
	}
	return EndpointMappingFromOggPayload(parsed.Payloads[0])
}

// EndpointMappingFromOggPayload reads ENDPOINTS metadata from ogg (AudioStreamingPartInternal.cpp).
func EndpointMappingFromOggPayload(payload []byte) map[string]int32 {
	idx := 0
	for {
		pos := indexOf(payload[idx:], "ENDPOINTS=")
		if pos < 0 {
			return nil
		}
		idx += pos + len("ENDPOINTS=")
		end := idx
		for end < len(payload) && payload[end] != 0 && payload[end] != ' ' && payload[end] != '\n' {
			end++
		}
		raw := string(payload[idx:end])
		mapping := make(map[string]int32)
		fieldIdx := int32(0)
		for _, ep := range splitFields(raw) {
			if ep == "" {
				continue
			}
			mapping[ep] = fieldIdx
			fieldIdx++
		}
		if len(mapping) > 0 {
			return mapping
		}
	}
}

func indexOf(b []byte, s string) int {
	for i := 0; i+len(s) <= len(b); i++ {
		match := true
		for j := 0; j < len(s); j++ {
			if b[i+j] != s[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

func splitFields(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ' ' {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	return out
}
