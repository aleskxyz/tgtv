package publish

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"
)

const staleCleanupInterval = 60 * time.Second

// KillStaleRTMPPublishers terminates ffmpeg processes whose command line includes
// rtmpURL but whose PID is not exceptPID (0 means kill all matches).
func KillStaleRTMPPublishers(rtmpURL string, exceptPID int) int {
	if rtmpURL == "" {
		return 0
	}
	killed := 0
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0
	}
	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(ent.Name())
		if err != nil || pid <= 1 || pid == exceptPID {
			continue
		}
		cmdline, err := os.ReadFile(filepath.Join("/proc", ent.Name(), "cmdline"))
		if err != nil {
			continue
		}
		if !isStaleRTMPProcess(string(cmdline), rtmpURL) {
			continue
		}
		proc, err := os.FindProcess(pid)
		if err != nil {
			continue
		}
		_ = proc.Signal(syscall.SIGINT)
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if err := proc.Signal(syscall.Signal(0)); err != nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		_ = proc.Kill()
		killed++
	}
	return killed
}

func isStaleRTMPProcess(cmdline, rtmpURL string) bool {
	if !strings.Contains(cmdline, "ffmpeg") {
		return false
	}
	return strings.Contains(strings.ReplaceAll(cmdline, "\x00", " "), rtmpURL)
}

// StartStaleCleanup periodically kills orphan ffmpeg RTMP publishers for streams
// that no longer have an active ingest session.
func StartStaleCleanup(ctx context.Context, rtmpBase string, activeStreams func() map[string]struct{}, log *zap.Logger) {
	if log == nil {
		log = zap.NewNop()
	}
	log = log.Named("rtmp-cleanup")
	rtmpBase = strings.TrimRight(rtmpBase, "/")
	ticker := time.NewTicker(staleCleanupInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sweepStaleRTMP(rtmpBase, activeStreams(), log)
			}
		}
	}()
}

func sweepStaleRTMP(rtmpBase string, active map[string]struct{}, log *zap.Logger) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return
	}
	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		cmdline, err := os.ReadFile(filepath.Join("/proc", ent.Name(), "cmdline"))
		if err != nil {
			continue
		}
		flat := strings.ReplaceAll(string(cmdline), "\x00", " ")
		if !strings.Contains(flat, "ffmpeg") || !strings.Contains(flat, rtmpBase+"/") {
			continue
		}
		streamID := streamIDFromRTMPCmd(flat, rtmpBase)
		if streamID == "" {
			continue
		}
		if _, ok := active[streamID]; ok {
			continue
		}
		pid, err := strconv.Atoi(ent.Name())
		if err != nil || pid <= 1 {
			continue
		}
		if n := KillStaleRTMPPublishers(rtmpBase+"/"+streamID, 0); n > 0 {
			log.Info("killed orphan rtmp publisher",
				zap.String("stream", streamID),
				zap.Int("pid", pid),
			)
		}
	}
}

func streamIDFromRTMPCmd(cmdline, rtmpBase string) string {
	prefix := rtmpBase + "/"
	idx := strings.Index(cmdline, prefix)
	if idx < 0 {
		return ""
	}
	rest := cmdline[idx+len(prefix):]
	if end := strings.IndexAny(rest, " \t"); end >= 0 {
		rest = rest[:end]
	}
	return strings.TrimSpace(rest)
}
