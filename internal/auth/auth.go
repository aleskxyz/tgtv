package auth

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/tg"
	"go.uber.org/zap"

	"github.com/aleskxyz/tgtv/internal/config"
)

func NewClient(cfg config.Settings, log *zap.Logger, extra telegram.Options) *telegram.Client {
	storage := &session.FileStorage{Path: cfg.SessionPath()}
	opts := telegram.Options{
		SessionStorage: storage,
		Logger:         config.TelegramLogger(log, cfg),
	}
	if extra.UpdateHandler != nil {
		opts.UpdateHandler = extra.UpdateHandler
	}
	if len(extra.Middlewares) > 0 {
		opts.Middlewares = extra.Middlewares
	}
	return telegram.NewClient(cfg.APIID, cfg.APIHash, opts)
}

type terminalAuth struct {
	phone string
}

func (t terminalAuth) Phone(_ context.Context) (string, error) {
	if t.phone != "" {
		return t.phone, nil
	}
	fmt.Print("Phone number: ")
	text, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(text), nil
}

func (t terminalAuth) Password(_ context.Context) (string, error) {
	fmt.Print("Two-step password: ")
	text, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(text), nil
}

func (t terminalAuth) Code(_ context.Context, _ *tg.AuthSentCode) (string, error) {
	fmt.Print("Code: ")
	text, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(text), nil
}

func (t terminalAuth) AcceptTermsOfService(_ context.Context, tos tg.HelpTermsOfService) error {
	return nil
}

func (t terminalAuth) SignUp(_ context.Context) (auth.UserInfo, error) {
	return auth.UserInfo{}, fmt.Errorf("sign up not supported")
}

func LoginInteractive(ctx context.Context, cfg config.Settings, phone string, log *zap.Logger) error {
	client := NewClient(cfg, log, telegram.Options{})
	return client.Run(ctx, func(ctx context.Context) error {
		status, err := client.Auth().Status(ctx)
		if err != nil {
			return err
		}
		if status.Authorized {
			self, err := client.Self(ctx)
			if err != nil {
				return err
			}
			fmt.Printf("Already logged in as %s\n", displayUser(self))
			return nil
		}

		flow := auth.NewFlow(terminalAuth{phone: phone}, auth.SendCodeOptions{})
		if err := flow.Run(ctx, client.Auth()); err != nil {
			return err
		}
		self, err := client.Self(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("Logged in as %s\n", displayUser(self))
		return nil
	})
}

func Logout(ctx context.Context, cfg config.Settings, log *zap.Logger) error {
	client := NewClient(cfg, log, telegram.Options{})
	var removeSession bool
	err := client.Run(ctx, func(ctx context.Context) error {
		status, err := client.Auth().Status(ctx)
		if err != nil {
			return err
		}
		if !status.Authorized {
			removeSession = true
			return nil
		}
		if _, err := client.API().AuthLogOut(ctx); err != nil {
			return err
		}
		removeSession = true
		return nil
	})
	if err != nil {
		return err
	}
	if removeSession {
		_ = os.Remove(cfg.SessionPath())
	}
	return nil
}

func Status(ctx context.Context, cfg config.Settings, log *zap.Logger) error {
	if !config.SessionExists(cfg) {
		fmt.Println("telegram_session: missing")
		return fmt.Errorf("session missing")
	}
	client := NewClient(cfg, log, telegram.Options{})
	return client.Run(ctx, func(ctx context.Context) error {
		status, err := client.Auth().Status(ctx)
		if err != nil {
			return err
		}
		if !status.Authorized {
			fmt.Println("telegram_session: expired")
			return fmt.Errorf("session expired")
		}
		self, err := client.Self(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("telegram_session: active, user: %s\n", displayUser(self))
		return nil
	})
}

func displayUser(u *tg.User) string {
	if u == nil {
		return "unknown"
	}
	if u.Username != "" {
		return "@" + u.Username
	}
	return fmt.Sprintf("%d", u.ID)
}
