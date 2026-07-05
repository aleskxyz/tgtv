package remux

import (
	"context"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const (
	defaultVideoPadSec = 0.04
	maxVideoPadSec     = 0.2
	probeTimeout       = 3 * time.Second
)

// VideoPadFromDuration computes last-frame hold for the 1 s Telegram segment window.
func VideoPadFromDuration(videoDur float64) float64 {
	if videoDur <= 0 {
		return defaultVideoPadSec
	}
	pad := partDurationSec - videoDur
	if pad < 0 {
		return 0
	}
	if pad > maxVideoPadSec {
		return maxVideoPadSec
	}
	return pad
}

func probeStreamDuration(path, stream string) (float64, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "ffprobe",
		"-v", "error",
		"-select_streams", stream+":0",
		"-show_entries", "stream=duration",
		"-of", "default=noprint_wrappers=1:nokey=1",
		path,
	)
	out, err := cmd.Output()
	if err != nil {
		return 0, false
	}
	dur, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil || dur < 0 {
		return 0, false
	}
	return dur, true
}

func probeVideoCodec(path string) (string, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "ffprobe",
		"-v", "error",
		"-select_streams", "v:0",
		"-show_entries", "stream=codec_name",
		"-of", "default=noprint_wrappers=1:nokey=1",
		path,
	)
	out, err := cmd.Output()
	if err != nil {
		return "", false
	}
	codec := strings.ToLower(strings.TrimSpace(string(out)))
	if codec == "" {
		return "", false
	}
	return codec, true
}

func computeVideoPadSeconds(videoPath string) float64 {
	if dur, ok := probeStreamDuration(videoPath, "v"); ok {
		return VideoPadFromDuration(dur)
	}
	return defaultVideoPadSec
}

func videoPadApplies(container string) bool {
	switch container {
	case "mp4", "mov":
		return true
	default:
		return false
	}
}

func videoPadEncodeArgs() []string {
	return []string{
		"-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency",
		"-g", "25", "-keyint_min", "25", "-sc_threshold", "0",
		"-t", formatDuration(partDurationSec),
	}
}

func logoFileExists(path string) bool {
	if path == "" {
		return false
	}
	st, err := os.Stat(path)
	return err == nil && st.Size() > 0
}

func videoPadFilterArgs(padSeconds float64, logoPath string, separateAudio bool) (extraInputs, filterArgs []string) {
	padFilter := formatPadFilter(padSeconds)
	if !logoFileExists(logoPath) {
		return nil, []string{"-vf", padFilter, "-map", "0:v:0?"}
	}
	extraInputs = []string{"-i", logoPath}
	logoInput := 1
	if separateAudio {
		logoInput = 2
	}
	filter := "[0:v]" + padFilter + "[base];[" + strconv.Itoa(logoInput) + ":v]scale=80:-1[logo];[base][logo]overlay=main_w-overlay_w-10:10[out]"
	return extraInputs, []string{"-filter_complex", filter, "-map", "[out]"}
}

func formatPadFilter(padSeconds float64) string {
	return "tpad=stop_mode=clone:stop_duration=" + formatDuration(padSeconds)
}

func formatDuration(sec float64) string {
	return strconv.FormatFloat(sec, 'f', 3, 64)
}
