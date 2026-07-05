package service

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gotd/log/logzap"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/updates"
	updhook "github.com/gotd/td/telegram/updates/hook"
	"github.com/gotd/td/tg"
	"go.uber.org/zap"

	"github.com/aleskxyz/tgtv/internal/api"
	"github.com/aleskxyz/tgtv/internal/auth"
	"github.com/aleskxyz/tgtv/internal/config"
	"github.com/aleskxyz/tgtv/internal/discovery"
	"github.com/aleskxyz/tgtv/internal/ingest"
	"github.com/aleskxyz/tgtv/internal/stream"
	"github.com/aleskxyz/tgtv/internal/thumbnails"
	"github.com/aleskxyz/tgtv/internal/viewer"
)

func Run(ctx context.Context, cfg config.Settings, pathSecret string, log *zap.Logger) error {
	if !config.SessionExists(cfg) {
		return fmt.Errorf("no Telegram session — run: tgtv login")
	}
	log.Info("starting", zap.String("log_level", cfg.LogLevel))

	registry := discovery.NewRegistry()
	dispatcher := tg.NewUpdateDispatcher()
	gaps := updates.New(updates.Config{
		Handler: &dispatcher,
		Logger:  logzap.New(log.Named("gaps")),
	})

	client := auth.NewClient(cfg, log, telegram.Options{
		UpdateHandler: gaps,
		Middlewares: []telegram.Middleware{
			updhook.UpdateHook(gaps.Handle),
		},
	})

	return client.Run(ctx, func(ctx context.Context) error {
		status, err := client.Auth().Status(ctx)
		if err != nil {
			return err
		}
		if !status.Authorized {
			return fmt.Errorf("telegram session expired — run: tgtv login")
		}

		self, err := client.Self(ctx)
		if err != nil {
			return err
		}
		apiClient := client.API()
		mt := stream.NewMTProto(apiClient, client)

		thumbs := thumbnails.NewStore(apiClient, cfg.ThumbnailsDir(), log)
		supervisor := ingest.NewSupervisor(mt, self, registry, thumbs, cfg, ctx, log)
		supervisor.StartStaleProcessCleanup(ctx)
		viewers := viewer.NewManager(cfg.IdleGraceSeconds, func(streamID string) {
			if supervisor.IsIngesting(streamID) {
				supervisor.StopIngest(streamID)
			}
		})
		scanner := discovery.NewScanner(apiClient, registry, cfg, self.ID, log)
		scanner.SetOnChannelSeen(thumbs.RememberChannel)
		scanner.SetOnLiveDiscovered(func(chatID int64) {
			thumbs.Prefetch(ctx, chatID)
		})
		scanner.SetOnLiveMetadataUpdated(func(chatID int64, chat tg.ChatClass) {
			photoChanged := thumbs.SyncPhotoFromChat(chat)
			if photoChanged {
				supervisor.RefreshLogoForChat(chatID)
			}
			if !photoChanged && !thumbs.ShouldPrefetch(chatID) {
				return
			}
			prefetchCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			if _, err := thumbs.GetJPEG(prefetchCtx, chatID); err != nil {
				return
			}
			if photoChanged {
				supervisor.RefreshLogoForChat(chatID)
			}
		})
		scanner.SetOnLiveEnded(func(streamID string) {
			if supervisor.IsIngesting(streamID) {
				supervisor.StopIngest(streamID)
			}
		})
		scanner.SetOnCallSuperseded(func(streamID string) {
			supervisor.RestartIngest(streamID)
		})
		scanner.Register(&dispatcher)

		go func() {
			backoff := time.Second
			for {
				err := gaps.Run(ctx, apiClient, self.ID, updates.AuthOptions{
					OnStart: func(ctx context.Context) {
						log.Info("telegram updates connected")
					},
				})
				if ctx.Err() != nil {
					return
				}
				log.Error("updates manager stopped, restarting",
					zap.Error(err),
					zap.Duration("backoff", backoff),
				)
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				if backoff < 60*time.Second {
					backoff *= 2
				}
			}
		}()

		scanner.Start(ctx)
		viewers.Start(ctx)

		httpServer := api.NewServer(cfg, pathSecret, registry, thumbs, supervisor, viewers, log)
		srv := &http.Server{
			Addr:    fmt.Sprintf("%s:%d", cfg.HTTPHost, cfg.HTTPPort),
			Handler: httpServer.Handler(),
		}

		errCh := make(chan error, 1)
		go func() {
			log.Info("HTTP listening",
				zap.String("addr", srv.Addr),
				zap.String("playlist", fmt.Sprintf("%s/p/<secret>/playlist.m3u", cfg.PublicBaseURL)),
			)
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				errCh <- err
			}
		}()

		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		defer signal.Stop(sigCh)

		select {
		case <-ctx.Done():
		case <-sigCh:
			log.Info("shutdown requested")
		case err := <-errCh:
			return err
		}

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		supervisor.PrepareShutdown()
		_ = srv.Shutdown(shutdownCtx)
		scanner.Stop()
		viewers.Stop()
		supervisor.StopAll()
		mt.Close()
		return nil
	})
}
