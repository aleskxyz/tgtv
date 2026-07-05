package remux

import (
	"bytes"
	"fmt"
	"os/exec"
	"time"

	"github.com/aleskxyz/tgtv/internal/media"
)

const (
	partDurationSec = 1.0
	remuxTimeout    = 8 * time.Second
)

type Result struct {
	MPEGTS   []byte
	Duration float64
}

func PayloadToMPEGTS(container string, payload []byte, tsOffset float64, logoPath string) (Result, error) {
	return remux(container, payload, nil, "", tsOffset, logoPath)
}

func MuxAV(vContainer string, vPayload []byte, aContainer string, aPayload []byte, tsOffset float64, logoPath string) (Result, error) {
	return remux(vContainer, vPayload, aPayload, aContainer, tsOffset, logoPath)
}

func remux(primaryContainer string, primaryPayload, audioPayload []byte, audioContainer string, tsOffset float64, logoPath string) (Result, error) {
	primary, err := newSeekableBytes(primaryPayload, media.ContainerSuffix(primaryContainer))
	if err != nil {
		return Result{}, err
	}
	defer primary.cleanup()

	var audio *seekableFile
	if audioPayload != nil {
		audio, err = newSeekableBytes(audioPayload, media.ContainerSuffix(audioContainer))
		if err != nil {
			return Result{}, err
		}
		defer audio.cleanup()
	}

	container := media.NormalizeContainer(primaryContainer)
	usePad := videoPadApplies(container)
	copyVideo := !usePad
	if copyVideo && media.MayNeedVideoTranscode(container) {
		if codec, ok := probeVideoCodec(primary.path); ok && media.VideoCodecNeedsTranscode(codec) {
			copyVideo = false
		}
	}

	result, err := runRemux(primary, audio, primaryContainer, audioContainer, tsOffset, logoPath, usePad, copyVideo)
	if err != nil && copyVideo && shouldRetryVideoTranscode(container, primary.path) {
		if retry, retryErr := runRemux(primary, audio, primaryContainer, audioContainer, tsOffset, logoPath, usePad, false); retryErr == nil {
			return retry, nil
		}
	}
	return result, err
}

func shouldRetryVideoTranscode(container string, videoPath string) bool {
	if videoPadApplies(container) {
		return false
	}
	if media.MayNeedVideoTranscode(container) {
		return true
	}
	codec, ok := probeVideoCodec(videoPath)
	return ok && media.VideoCodecNeedsTranscode(codec)
}

func runRemux(primary, audio *seekableFile, primaryContainer, audioContainer string, tsOffset float64, logoPath string, usePad, copyVideo bool) (Result, error) {
	args := []string{
		"-hide_banner", "-loglevel", "error",
		"-fflags", "+genpts+discardcorrupt",
	}

	if audio != nil {
		args = append(args, media.FFmpegInputArgs(primaryContainer, primary.path)...)
		args = append(args, media.FFmpegInputArgs(audioContainer, audio.path)...)
	} else {
		args = append(args, media.FFmpegInputArgs(primaryContainer, primary.path)...)
	}

	separateAudio := audio != nil
	if usePad {
		pad := computeVideoPadSeconds(primary.path)
		extra, filterArgs := videoPadFilterArgs(pad, logoPath, separateAudio)
		args = append(args, extra...)
		args = append(args, filterArgs...)
		if separateAudio {
			args = append(args, "-map", "1:a:0?")
		} else {
			args = append(args, "-map", "0:a:0?")
		}
		args = append(args, videoPadEncodeArgs()...)
	} else if separateAudio {
		args = append(args, "-map", "0:v:0?", "-map", "1:a:0?")
		args = append(args, videoOutputArgs(copyVideo)...)
	} else {
		args = append(args, "-map", "0:v:0?", "-map", "0:a:0?")
		args = append(args, videoOutputArgs(copyVideo)...)
	}

	if media.NeedsAudioTranscode(primaryContainer) || audioContainer != "" && media.NeedsAudioTranscode(audioContainer) {
		args = append(args, "-c:a", "aac", "-b:a", "128k", "-ar", "48000")
	} else {
		args = append(args, "-c:a", "copy")
	}

	if tsOffset > 0 {
		args = append(args, "-output_ts_offset", fmt.Sprintf("%f", tsOffset))
	}

	args = append(args,
		"-mpegts_flags", "+resend_headers+initial_discontinuity",
		"-f", "mpegts",
		"pipe:1",
	)

	cmd := exec.Command("ffmpeg", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	done := make(chan error, 1)
	go func() { done <- cmd.Run() }()

	select {
	case err := <-done:
		if err != nil {
			return Result{}, fmt.Errorf("ffmpeg remux: %w: %s", err, stderr.String())
		}
	case <-time.After(remuxTimeout):
		_ = cmd.Process.Kill()
		<-done
		return Result{}, fmt.Errorf("ffmpeg remux timeout")
	}

	return Result{MPEGTS: stdout.Bytes(), Duration: partDurationSec}, nil
}

func videoOutputArgs(copyVideo bool) []string {
	if copyVideo {
		return []string{"-c:v", "copy", "-t", formatDuration(partDurationSec)}
	}
	return videoTranscodeArgs()
}

func videoTranscodeArgs() []string {
	return []string{
		"-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency",
		"-g", "25", "-keyint_min", "25", "-sc_threshold", "0",
		"-t", formatDuration(partDurationSec),
	}
}
