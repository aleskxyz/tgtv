package media

import "strings"

func NormalizeContainer(container string) string {
	n := strings.ToLower(strings.TrimSpace(container))
	switch n {
	case "", "mkv":
		if n == "mkv" {
			return "matroska"
		}
		return "mpegts"
	case "h265":
		return "hevc"
	default:
		return n
	}
}

func ContainerSuffix(container string) string {
	switch NormalizeContainer(container) {
	case "mpegts":
		return ".ts"
	case "mp4":
		return ".mp4"
	case "mov":
		return ".mov"
	case "ogg":
		return ".ogg"
	case "webm":
		return ".webm"
	case "matroska":
		return ".mkv"
	case "h264":
		return ".h264"
	case "hevc":
		return ".hevc"
	case "3gp":
		return ".3gp"
	default:
		return ".bin"
	}
}

// FFmpegInputArgs returns FFmpeg input flags for a broadcast container payload.
func FFmpegInputArgs(container, path string) []string {
	switch NormalizeContainer(container) {
	case "h264", "hevc":
		return []string{"-f", NormalizeContainer(container), "-i", path}
	case "mpegts":
		return []string{"-f", "mpegts", "-i", path}
	default:
		return []string{"-i", path}
	}
}

func NeedsAudioTranscode(container string) bool {
	switch NormalizeContainer(container) {
	case "ogg", "webm", "matroska":
		return true
	default:
		return false
	}
}

// MayNeedVideoTranscode reports containers where -c:v copy to MPEG-TS often fails (VP9/AV1).
func MayNeedVideoTranscode(container string) bool {
	switch NormalizeContainer(container) {
	case "webm", "matroska":
		return true
	default:
		return false
	}
}

func VideoCodecNeedsTranscode(codec string) bool {
	switch codec {
	case "vp9", "av1", "av01":
		return true
	default:
		return false
	}
}
