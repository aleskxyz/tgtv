package config

import (
	"strings"

	"github.com/gotd/log"
	"github.com/gotd/log/logzap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func NewLogger(level, format string) (*zap.Logger, error) {
	zlevel, err := parseLevel(level)
	if err != nil {
		return nil, err
	}

	switch strings.ToLower(strings.TrimSpace(format)) {
	case "json", "jsonl":
		cfg := zap.Config{
			Level:       zap.NewAtomicLevelAt(zlevel),
			Development: false,
			Encoding:    "json",
			EncoderConfig: zapcore.EncoderConfig{
				TimeKey:        "timestamp",
				LevelKey:       "level",
				NameKey:        "logger",
				CallerKey:      "caller",
				MessageKey:     "message",
				StacktraceKey:  "stacktrace",
				LineEnding:     zapcore.DefaultLineEnding,
				EncodeLevel:    zapcore.LowercaseLevelEncoder,
				EncodeTime:     zapcore.ISO8601TimeEncoder,
				EncodeDuration: zapcore.SecondsDurationEncoder,
				EncodeCaller:   zapcore.ShortCallerEncoder,
			},
			OutputPaths:      []string{"stdout"},
			ErrorOutputPaths: []string{"stderr"},
		}
		return cfg.Build()

	default:
		cfg := zap.Config{
			Level:       zap.NewAtomicLevelAt(zlevel),
			Development: zlevel <= zap.DebugLevel,
			Encoding:    "console",
			EncoderConfig: zapcore.EncoderConfig{
				TimeKey:        "T",
				LevelKey:       "L",
				NameKey:        "N",
				CallerKey:      "C",
				MessageKey:     "M",
				StacktraceKey:  "S",
				LineEnding:     zapcore.DefaultLineEnding,
				EncodeLevel:    zapcore.CapitalLevelEncoder,
				EncodeTime:     zapcore.TimeEncoderOfLayout("2006-01-02 15:04:05.000"),
				EncodeDuration: zapcore.StringDurationEncoder,
				EncodeCaller:   zapcore.ShortCallerEncoder,
			},
			OutputPaths:      []string{"stdout"},
			ErrorOutputPaths: []string{"stderr"},
		}
		return cfg.Build()
	}
}

// TelegramLogger returns a gotd logger at TELEGRAM_LOG_LEVEL (default warn).
func TelegramLogger(root *zap.Logger, cfg Settings) log.Logger {
	levelStr := cfg.TelegramLogLevel
	if levelStr == "" {
		levelStr = "warn"
	}
	zlevel, err := parseLevel(levelStr)
	if err != nil {
		zlevel = zap.WarnLevel
	}
	return logzap.New(root.Named("telegram").WithOptions(zap.IncreaseLevel(zlevel)))
}

func parseLevel(s string) (zapcore.Level, error) {
	var l zapcore.Level
	if err := l.UnmarshalText([]byte(strings.ToLower(strings.TrimSpace(s)))); err != nil {
		return zap.InfoLevel, err
	}
	return l, nil
}

func (s Settings) Debug() bool {
	return strings.EqualFold(s.LogLevel, "debug")
}
