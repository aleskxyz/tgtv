package publish

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

const rtmpWriteTimeout = 8 * time.Second
const hlsProbeTimeout = 10 * time.Second

var hlsProbeHTTPClient = &http.Client{Timeout: hlsProbeTimeout}

// Publisher pipes MPEG-TS into a long-lived FFmpeg RTMP process.
type Publisher struct {
	streamID  string
	rtmpURL   string
	hlsURL    string
	cdnSecret string
	log       *zap.Logger

	mu         sync.Mutex
	cmd        *exec.Cmd
	stdin      io.WriteCloser
	stderrDone chan struct{}
	closed     bool
	needsDisc  bool
}

func NewPublisher(streamID, rtmpBase, hlsBase, cdnSecret string, log *zap.Logger) *Publisher {
	if log == nil {
		log = zap.NewNop()
	}
	return &Publisher{
		streamID:  streamID,
		rtmpURL:   fmt.Sprintf("%s/%s", rtmpBase, streamID),
		hlsURL:    fmt.Sprintf("%s/live/%s/index.m3u8", hlsBase, streamID),
		cdnSecret: cdnSecret,
		log:       log.Named("rtmp").With(zap.String("stream", streamID)),
	}
}

func (p *Publisher) Write(data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("publisher closed")
	}
	if err := p.ensureProcess(); err != nil {
		return err
	}
	if err := writeAll(p.stdin, data); err != nil {
		p.stopLocked()
		return err
	}
	return nil
}

const rtmpInputFFlags = "+genpts+igndts"

func rtmpCommand(rtmpURL string) []string {
	return []string{
		"-hide_banner", "-loglevel", "warning",
		"-fflags", rtmpInputFFlags,
		"-f", "mpegts", "-i", "pipe:0",
		"-c:v", "copy", "-c:a", "copy",
		"-f", "flv", "-flvflags", "no_duration_filesize",
		rtmpURL,
	}
}

func (p *Publisher) ensureProcess() error {
	if p.cmd != nil && p.cmd.ProcessState == nil {
		return nil
	}
	hadProcess := p.cmd != nil
	p.stopLocked()
	if hadProcess {
		p.needsDisc = true
	}
	if n := KillStaleRTMPPublishers(p.rtmpURL, 0); n > 0 {
		p.log.Info("cleaned stale rtmp publishers before restart", zap.Int("killed", n))
	}
	cmd := exec.Command("ffmpeg", rtmpCommand(p.rtmpURL)...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		_ = stdin.Close()
		return err
	}
	cmd.Stdout = nil
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		return err
	}
	p.stderrDone = make(chan struct{})
	go p.drainStderr(stderr, p.stderrDone)
	p.cmd = cmd
	p.stdin = stdin
	return nil
}

func (p *Publisher) drainStderr(r io.Reader, done chan struct{}) {
	defer close(done)
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		p.log.Debug("ffmpeg", zap.String("stderr", line))
	}
}

func (p *Publisher) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.needsDisc = true
	p.stopLocked()
}

// ConsumeDiscontinuity reports whether the next proxied media playlist needs
// #EXT-X-DISCONTINUITY after an FFmpeg/RTMP restart.
func (p *Publisher) ConsumeDiscontinuity() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.needsDisc {
		return false
	}
	p.needsDisc = false
	return true
}

func (p *Publisher) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	p.stopLocked()
	KillStaleRTMPPublishers(p.rtmpURL, 0)
}

func (p *Publisher) stopLocked() {
	if p.stdin != nil {
		_ = p.stdin.Close()
		p.stdin = nil
	}
	if p.cmd != nil && p.cmd.Process != nil {
		proc := p.cmd.Process
		done := p.stderrDone
		_ = proc.Signal(os.Interrupt)
		waitDone := make(chan struct{})
		go func() {
			_ = p.cmd.Wait()
			close(waitDone)
		}()
		select {
		case <-waitDone:
		case <-time.After(5 * time.Second):
			_ = proc.Kill()
			_ = p.cmd.Wait()
		}
		if done != nil {
			select {
			case <-done:
			case <-time.After(time.Second):
			}
		}
	}
	p.cmd = nil
	p.stderrDone = nil
}

func (p *Publisher) IsReady(ctx context.Context) bool {
	probeCtx, cancel := context.WithTimeout(ctx, hlsProbeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, p.hlsURL, nil)
	if err != nil {
		return false
	}
	req.Header.Set("Authorization", "Bearer "+p.cdnSecret)
	resp, err := hlsProbeHTTPClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func writeAll(w io.Writer, data []byte) error {
	deadline := time.Now().Add(rtmpWriteTimeout)
	for len(data) > 0 {
		if time.Now().After(deadline) {
			return fmt.Errorf("rtmp write timeout")
		}
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}
