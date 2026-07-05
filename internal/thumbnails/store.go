package thumbnails

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gotd/td/constant"
	"github.com/gotd/td/fileid"
	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/tg"
	"go.uber.org/zap"

	"github.com/aleskxyz/tgtv/internal/discovery"
)

const maxAge = 24 * time.Hour

type photoState struct {
	id      int64
	tracked bool
}

type inflightFetch struct {
	done chan struct{}
	data []byte
	err  error
}

// Store caches Telegram chat profile photos as JPEG for M3U metadata and remux overlays.
type Store struct {
	api *tg.Client
	dir string
	log *zap.Logger
	dl  *downloader.Downloader

	mu         sync.Mutex
	access     map[int64]int64
	photoState map[int64]photoState
	fetchMu    sync.Mutex
	inflight   map[int64]*inflightFetch
}

func NewStore(api *tg.Client, dir string, log *zap.Logger) *Store {
	if log == nil {
		log = zap.NewNop()
	}
	return &Store{
		api:        api,
		dir:        dir,
		log:        log.Named("thumbnails"),
		dl:         downloader.NewDownloader(),
		access:     make(map[int64]int64),
		photoState: make(map[int64]photoState),
		inflight:   make(map[int64]*inflightFetch),
	}
}

func (t *Store) RememberChannel(ch *tg.Channel) {
	if ch == nil {
		return
	}
	t.mu.Lock()
	t.access[ch.ID] = ch.AccessHash
	t.mu.Unlock()
}

func (t *Store) cachePath(chatID int64) string {
	return filepath.Join(t.dir, fmt.Sprintf("%d.jpg", chatID))
}

func (t *Store) photoSidecarPath(chatID int64) string {
	return filepath.Join(t.dir, fmt.Sprintf("%d.photo", chatID))
}

// PhotoVersion returns the cached Telegram profile photo id for playlist cache-busting.
func (t *Store) PhotoVersion(chatID int64) int64 {
	t.mu.Lock()
	state := t.photoState[chatID]
	t.mu.Unlock()
	if state.tracked {
		return state.id
	}
	if id, ok := t.readPhotoSidecar(chatID); ok {
		return id
	}
	return 0
}

// LogoPath returns a fresh cached thumbnail path suitable for FFmpeg overlay, or "" if missing/stale.
func (t *Store) LogoPath(chatID int64) string {
	path := t.cachePath(chatID)
	if cacheFileFresh(path) {
		return path
	}
	return ""
}

// Invalidate removes a cached thumbnail so the next fetch downloads fresh bytes.
func (t *Store) Invalidate(chatID int64) {
	_ = os.Remove(t.cachePath(chatID))
	_ = os.Remove(t.photoSidecarPath(chatID))
}

// SyncPhotoFromChat invalidates the JPEG cache when the remote profile photo id changed.
func (t *Store) SyncPhotoFromChat(chat tg.ChatClass) bool {
	chatID := discovery.ChatIDOf(chat)
	if chatID == 0 {
		return false
	}
	if ch, ok := chat.(*tg.Channel); ok {
		t.RememberChannel(ch)
	}
	return t.syncPhotoID(chatID, photoIDFromChat(chat))
}

// ShouldPrefetch reports whether a thumbnail download is worth attempting.
func (t *Store) ShouldPrefetch(chatID int64) bool {
	if cacheFileFresh(t.cachePath(chatID)) {
		return false
	}
	t.mu.Lock()
	state := t.photoState[chatID]
	t.mu.Unlock()
	return !(state.tracked && state.id == 0)
}

func (t *Store) Prefetch(ctx context.Context, chatID int64) {
	go func() {
		prefetchCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		if _, err := t.GetJPEG(prefetchCtx, chatID); err != nil {
			t.log.Debug("thumbnail prefetch failed", zap.Int64("chat", chatID), zap.Error(err))
		}
	}()
}

func (t *Store) GetJPEG(ctx context.Context, chatID int64) ([]byte, error) {
	path := t.cachePath(chatID)
	if data, ok := t.readFreshCache(path); ok {
		if t.cacheMatchesRemote(ctx, chatID) {
			return data, nil
		}
		t.log.Info("thumbnail cache stale, re-downloading", zap.Int64("chat", chatID))
		t.Invalidate(chatID)
	}

	t.fetchMu.Lock()
	if f, ok := t.inflight[chatID]; ok {
		t.fetchMu.Unlock()
		select {
		case <-f.done:
			return f.data, f.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f := &inflightFetch{done: make(chan struct{})}
	t.inflight[chatID] = f
	t.fetchMu.Unlock()

	defer func() {
		close(f.done)
		t.fetchMu.Lock()
		delete(t.inflight, chatID)
		t.fetchMu.Unlock()
	}()

	if data, ok := t.readFreshCache(path); ok {
		f.data = data
		return data, nil
	}

	data, err := t.download(ctx, chatID)
	if err != nil {
		if cached, ok := t.readFreshCache(path); ok {
			f.data = cached
			f.err = nil
			return cached, nil
		}
		f.data = nil
		f.err = err
		return nil, err
	}
	if writeErr := writeCacheFile(path, data); writeErr != nil {
		t.log.Warn("could not write thumbnail cache", zap.Int64("chat", chatID), zap.Error(writeErr))
	}
	f.data = data
	f.err = nil
	return data, nil
}

func cacheFileFresh(path string) bool {
	st, err := os.Stat(path)
	if err != nil || st.Size() == 0 {
		return false
	}
	return time.Since(st.ModTime()) < maxAge
}

func (t *Store) readFreshCache(path string) ([]byte, bool) {
	if !cacheFileFresh(path) {
		return nil, false
	}
	data, err := os.ReadFile(path)
	if err != nil || len(data) == 0 {
		return nil, false
	}
	return data, true
}

func writeCacheFile(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (t *Store) download(ctx context.Context, chatID int64) ([]byte, error) {
	chat, err := t.fetchChat(ctx, chatID)
	if err != nil {
		return nil, err
	}

	photo, ok := profilePhoto(chat)
	if !ok {
		t.notePhotoID(chatID, 0)
		return nil, fmt.Errorf("chat %d has no profile photo", chatID)
	}

	var peerID constant.TDLibPeerID
	var accessHash int64
	switch c := chat.(type) {
	case *tg.Channel:
		peerID.Channel(c.ID)
		accessHash = c.AccessHash
	default:
		return nil, fmt.Errorf("unsupported chat type for chat %d", chatID)
	}

	fid := fileid.FromChatPhoto(peerID, accessHash, photo, true)
	loc, ok := fid.AsInputFileLocation()
	if !ok {
		return nil, fmt.Errorf("no file location for chat %d photo", chatID)
	}

	var buf bytes.Buffer
	if _, err := t.dl.Download(t.api, loc).Stream(ctx, &buf); err != nil {
		return nil, err
	}
	if buf.Len() == 0 {
		return nil, fmt.Errorf("empty profile photo for chat %d", chatID)
	}
	t.notePhotoID(chatID, photoIDFromChat(chat))
	t.writePhotoSidecar(chatID, photoIDFromChat(chat))
	return buf.Bytes(), nil
}

func (t *Store) notePhotoID(chatID, photoID int64) {
	t.mu.Lock()
	t.photoState[chatID] = photoState{id: photoID, tracked: true}
	t.mu.Unlock()
}

func (t *Store) fetchChat(ctx context.Context, chatID int64) (tg.ChatClass, error) {
	t.mu.Lock()
	accessHash := t.access[chatID]
	t.mu.Unlock()

	if accessHash != 0 {
		res, err := t.api.ChannelsGetChannels(ctx, []tg.InputChannelClass{
			&tg.InputChannel{ChannelID: chatID, AccessHash: accessHash},
		})
		if err == nil {
			if chats := res.GetChats(); len(chats) > 0 {
				if ch, ok := chats[0].(*tg.Channel); ok {
					t.RememberChannel(ch)
					return ch, nil
				}
			}
		}
	}
	return nil, fmt.Errorf("chat %d not found", chatID)
}

func photoIDFromChat(chat tg.ChatClass) int64 {
	photo, ok := profilePhoto(chat)
	if !ok {
		return 0
	}
	if cp, ok := photo.(*tg.ChatPhoto); ok {
		return cp.PhotoID
	}
	return 0
}

func profilePhoto(chat tg.ChatClass) (fileid.ChatPhoto, bool) {
	if ch, ok := chat.(*tg.Channel); ok {
		return ch.Photo.AsNotEmpty()
	}
	return nil, false
}

func (t *Store) syncPhotoID(chatID, newID int64) bool {
	prevID, tracked := t.knownPhotoID(chatID)
	t.mu.Lock()
	t.photoState[chatID] = photoState{id: newID, tracked: true}
	t.mu.Unlock()
	if tracked && prevID == newID {
		return false
	}
	if tracked && prevID != newID {
		t.log.Info("profile photo changed, refreshing thumbnail",
			zap.Int64("chat", chatID),
			zap.Int64("prev", prevID),
			zap.Int64("new", newID),
		)
		t.Invalidate(chatID)
		return true
	}
	return false
}

func (t *Store) knownPhotoID(chatID int64) (id int64, tracked bool) {
	t.mu.Lock()
	state := t.photoState[chatID]
	t.mu.Unlock()
	if state.tracked {
		return state.id, true
	}
	if id, ok := t.readPhotoSidecar(chatID); ok {
		return id, true
	}
	return 0, false
}

func (t *Store) cacheMatchesRemote(ctx context.Context, chatID int64) bool {
	cachedID, ok := t.readPhotoSidecar(chatID)
	if !ok {
		return false
	}
	chat, err := t.fetchChat(ctx, chatID)
	if err != nil {
		t.log.Debug("thumbnail cache verify failed, serving cache", zap.Int64("chat", chatID), zap.Error(err))
		return true
	}
	return photoIDFromChat(chat) == cachedID
}

func (t *Store) readPhotoSidecar(chatID int64) (int64, bool) {
	data, err := os.ReadFile(t.photoSidecarPath(chatID))
	if err != nil || len(data) == 0 {
		return 0, false
	}
	var id int64
	if _, err := fmt.Sscanf(string(bytes.TrimSpace(data)), "%d", &id); err != nil {
		return 0, false
	}
	return id, true
}

func (t *Store) writePhotoSidecar(chatID, photoID int64) {
	_ = os.WriteFile(t.photoSidecarPath(chatID), []byte(fmt.Sprintf("%d\n", photoID)), 0o600)
}
