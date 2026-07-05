package api

import (
	"context"
	"crypto/subtle"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"

	"github.com/aleskxyz/tgtv/internal/config"
	"github.com/aleskxyz/tgtv/internal/discovery"
	"github.com/aleskxyz/tgtv/internal/ingest"
	"github.com/aleskxyz/tgtv/internal/thumbnails"
)

type Server struct {
	cfg        config.Settings
	secret     string
	registry   *discovery.Registry
	thumbnails *thumbnails.Store
	ingest     *ingest.Supervisor
	viewers    ViewerTracker
	httpClient *http.Client
	log        *zap.Logger
}

type ViewerTracker interface {
	RecordActivity(streamID string)
}

func NewServer(
	cfg config.Settings,
	secret string,
	registry *discovery.Registry,
	thumbnails *thumbnails.Store,
	ingest *ingest.Supervisor,
	viewers ViewerTracker,
	log *zap.Logger,
) *Server {
	return &Server{
		cfg:        cfg,
		secret:     secret,
		registry:   registry,
		thumbnails: thumbnails,
		ingest:     ingest,
		viewers:    viewers,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		log:        log.Named("http"),
	}
}

func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()
	prefix := "/p/{secret}"
	r.Get(prefix+"/playlist.m3u", s.playlist)
	r.Get(prefix+"/thumbnails/{streamID}.jpg", s.thumbnail)
	r.Get(prefix+"/streams/{streamID}/play.m3u8", s.play)
	r.Get(prefix+"/streams/{streamID}/{filename}", s.hlsFile)
	r.Get(prefix+"/hls/{streamID}/{filename}", s.hlsFile)
	r.Get("/health", s.health)
	return r
}

func (s *Server) checkSecret(w http.ResponseWriter, r *http.Request) bool {
	got := chi.URLParam(r, "secret")
	if subtle.ConstantTimeCompare([]byte(got), []byte(s.secret)) != 1 {
		http.NotFound(w, r)
		return false
	}
	return true
}

func (s *Server) hlsBase(streamID string) string {
	return fmt.Sprintf("%s/p/%s/hls/%s", s.cfg.PublicBaseURL, s.secret, streamID)
}

func (s *Server) mediamtxURL(streamID, filename string) string {
	base := strings.Split(filename, "?")[0]
	return fmt.Sprintf("%s/live/%s/%s", s.cfg.MediamtxHLSURL, streamID, base)
}

func (s *Server) playlist(w http.ResponseWriter, r *http.Request) {
	if !s.checkSecret(w, r) {
		return
	}
	body := BuildPlaylist(s.registry, s.thumbnails, s.cfg, s.secret, s.ingest.IsRecoveryFailed)
	w.Header().Set("Content-Type", "application/x-mpegurl; charset=utf-8")
	_, _ = w.Write([]byte(body))
}

func (s *Server) thumbnail(w http.ResponseWriter, r *http.Request) {
	if !s.checkSecret(w, r) {
		return
	}
	if s.thumbnails == nil {
		http.NotFound(w, r)
		return
	}
	streamID := chi.URLParam(r, "streamID")
	entry, ok := s.registry.Snapshot(streamID)
	if !ok {
		http.NotFound(w, r)
		return
	}
	data, err := s.thumbnails.GetJPEG(r.Context(), entry.ChatID)
	if err != nil || len(data) == 0 {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "image/jpeg")
	if v := s.thumbnails.PhotoVersion(entry.ChatID); v != 0 {
		w.Header().Set("ETag", fmt.Sprintf(`"%d"`, v))
		w.Header().Set("Cache-Control", "public, max-age=86400")
	} else {
		w.Header().Set("Cache-Control", "public, max-age=300")
	}
	_, _ = w.Write(data)
}

func (s *Server) play(w http.ResponseWriter, r *http.Request) {
	if !s.checkSecret(w, r) {
		return
	}
	streamID := chi.URLParam(r, "streamID")
	if _, ok := s.registry.Get(streamID); !ok {
		http.NotFound(w, r)
		return
	}
	s.viewers.RecordActivity(streamID)
	if err := s.ingest.EnsureIngest(r.Context(), streamID); s.respondIngestError(w, err) {
		return
	}

	if !s.waitReady(r.Context(), streamID) {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl; charset=utf-8")
		_, _ = w.Write([]byte(SlateMasterPlaylist(s.hlsBase(streamID))))
		return
	}

	url := s.mediamtxURL(streamID, "index.m3u8")
	body, _, status, err := ProxyMediamtx(s.httpClient, url, MediamtxHeaders(s.cfg.MediamtxHLSCDNSecret))
	if err != nil || status != http.StatusOK {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl; charset=utf-8")
		_, _ = w.Write([]byte(SlateMasterPlaylist(s.hlsBase(streamID))))
		return
	}
	text := RewriteHLSPlaylist(string(body), s.hlsBase(streamID))
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl; charset=utf-8")
	_, _ = w.Write([]byte(text))
}

func (s *Server) hlsFile(w http.ResponseWriter, r *http.Request) {
	if !s.checkSecret(w, r) {
		return
	}
	streamID := chi.URLParam(r, "streamID")
	filename := chi.URLParam(r, "filename")
	if filename == "" {
		filename = "index.m3u8"
	}
	if !validHLSFilename(filename) {
		http.Error(w, "invalid filename", http.StatusBadRequest)
		return
	}

	if _, ok := s.registry.Get(streamID); !ok && !s.ingest.IsIngesting(streamID) {
		http.NotFound(w, r)
		return
	}
	if s.ingest.IsRecoveryFailed(streamID) {
		http.NotFound(w, r)
		return
	}

	s.viewers.RecordActivity(streamID)
	if err := s.ingest.EnsureIngest(r.Context(), streamID); s.respondIngestError(w, err) {
		return
	}
	if s.ingest.IsRecoveryFailed(streamID) {
		http.NotFound(w, r)
		return
	}
	if s.ingest.ShouldHoldHLS(streamID) && strings.HasSuffix(filename, ".m3u8") {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl; charset=utf-8")
		if strings.Contains(filename, "index") || strings.Contains(filename, "play") {
			_, _ = w.Write([]byte(SlateMasterPlaylist(s.hlsBase(streamID))))
		} else {
			_, _ = w.Write([]byte(SlateMediaPlaylist()))
		}
		return
	}

	url := s.mediamtxURL(streamID, filename)
	headers := MediamtxHeaders(s.cfg.MediamtxHLSCDNSecret)
	retries := 3
	if s.ingest.IsIngesting(streamID) && strings.HasSuffix(filename, ".ts") {
		retries = 8
	}

	var body []byte
	var ct string
	var status int
	var err error
	for attempt := 0; attempt < retries; attempt++ {
		body, ct, status, err = ProxyMediamtx(s.httpClient, url, headers)
		if err == nil && status == http.StatusOK {
			break
		}
		if attempt+1 < retries && s.ingest.IsIngesting(streamID) &&
			(status == http.StatusNotFound || status == http.StatusServiceUnavailable) {
			time.Sleep(250 * time.Millisecond)
			continue
		}
		break
	}
	if err != nil || status != http.StatusOK {
		if strings.HasSuffix(filename, ".m3u8") {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl; charset=utf-8")
			if strings.Contains(filename, "index") || strings.Contains(filename, "play") {
				_, _ = w.Write([]byte(SlateMasterPlaylist(s.hlsBase(streamID))))
			} else {
				_, _ = w.Write([]byte(SlateMediaPlaylist()))
			}
			return
		}
		http.NotFound(w, r)
		return
	}

	if strings.HasSuffix(filename, ".m3u8") || strings.Contains(ct, "mpegurl") {
		text := RewriteHLSPlaylist(string(body), s.hlsBase(streamID))
		if s.ingest.ConsumeHLSDiscontinuity(streamID) {
			text = InjectDiscontinuity(text)
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl; charset=utf-8")
		_, _ = w.Write([]byte(text))
		return
	}
	w.Header().Set("Content-Type", ct)
	_, _ = w.Write(body)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprint(w, `{"status":"ok"}`)
}

func (s *Server) waitReady(ctx context.Context, streamID string) bool {
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if s.ingest.IsReady(ctx, streamID) {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(500 * time.Millisecond):
		}
	}
	return s.ingest.IsReady(ctx, streamID)
}
