package config

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Settings struct {
	APIID                              int
	APIHash                            string
	SessionDir                         string
	SessionName                        string
	ConfigDir                          string
	PublicBaseURL                      string
	IdleGraceSeconds                   int
	MaxConcurrentIngests               int
	IngestStartStaggerSeconds          float64
	IngestInputRejoinSeconds           float64
	IngestRebufferSeconds              float64
	IngestStartupGraceSeconds          float64
	IngestOutputRecoverCooldownSeconds float64
	IngestRecoveryHoldSeconds          float64
	ScanDialogDelaySeconds             float64
	FullScanIntervalSeconds            int
	RTMPBaseURL                        string
	MediamtxHLSURL                     string
	MediamtxHLSCDNSecret               string
	HTTPHost                           string
	HTTPPort                           int
	PathSecret                         string
	LogLevel                           string
	LogFormat                          string
	TelegramLogLevel                   string
}

func (s Settings) SessionPath() string {
	return filepath.Join(s.SessionDir, s.SessionName+".json")
}

func (s Settings) PathSecretFile() string {
	return filepath.Join(s.ConfigDir, "path_secret")
}

func (s Settings) MediamtxHLSCDNSecretFile() string {
	return filepath.Join(s.ConfigDir, "mediamtx_hls_cdn_secret")
}

func (s Settings) ThumbnailsDir() string {
	return filepath.Join(s.ConfigDir, "thumbnails")
}

func Load() (Settings, error) {
	for _, path := range []string{".env", "../.env"} {
		_ = godotenv.Load(path)
	}

	apiIDStr := os.Getenv("TELEGRAM_API_ID")
	apiHash := os.Getenv("TELEGRAM_API_HASH")
	if apiIDStr == "" || apiHash == "" {
		return Settings{}, fmt.Errorf("TELEGRAM_API_ID and TELEGRAM_API_HASH are required")
	}
	apiID, err := strconv.Atoi(apiIDStr)
	if err != nil {
		return Settings{}, fmt.Errorf("invalid TELEGRAM_API_ID: %w", err)
	}

	sessionDir := env("SESSION_DIR", "/data/session")
	configDir := env("CONFIG_DIR", "/data/config")

	return Settings{
		APIID:                              apiID,
		APIHash:                            apiHash,
		SessionDir:                         sessionDir,
		SessionName:                        env("SESSION_NAME", "tgtv"),
		ConfigDir:                          configDir,
		PublicBaseURL:                      strings.TrimRight(env("PUBLIC_BASE_URL", "http://localhost:8090"), "/"),
		IdleGraceSeconds:                   envInt("IDLE_GRACE_SECONDS", 60),
		MaxConcurrentIngests:               envInt("MAX_CONCURRENT_INGESTS", 1),
		IngestStartStaggerSeconds:          envFloat("INGEST_START_STAGGER_SECONDS", 3),
		IngestInputRejoinSeconds:           envFloat("INGEST_INPUT_REJOIN_SECONDS", 30),
		IngestRebufferSeconds:              envFloat("INGEST_REBUFFER_SECONDS", 3),
		IngestStartupGraceSeconds:          envFloat("INGEST_STARTUP_GRACE_SECONDS", 15),
		IngestOutputRecoverCooldownSeconds: envFloat("INGEST_OUTPUT_RECOVER_COOLDOWN_SECONDS", 1),
		IngestRecoveryHoldSeconds:          envFloat("INGEST_RECOVERY_HOLD_SECONDS", 90),
		ScanDialogDelaySeconds:             envFloat("SCAN_DIALOG_DELAY_SECONDS", 0.35),
		FullScanIntervalSeconds:            envInt("FULL_SCAN_INTERVAL_SECONDS", 3600),
		RTMPBaseURL:                        strings.TrimRight(env("RTMP_BASE_URL", "rtmp://127.0.0.1:1935/live"), "/"),
		MediamtxHLSURL:                     strings.TrimRight(env("MEDIAMTX_HLS_URL", "http://127.0.0.1:8888"), "/"),
		MediamtxHLSCDNSecret:               strings.TrimSpace(os.Getenv("MEDIAMTX_HLS_CDN_SECRET")),
		HTTPHost:                           env("HTTP_HOST", "0.0.0.0"),
		HTTPPort:                           envInt("HTTP_PORT", 8090),
		PathSecret:                         os.Getenv("PATH_SECRET"),
		LogLevel:                           env("LOG_LEVEL", "info"),
		LogFormat:                          env("LOG_FORMAT", "console"),
		TelegramLogLevel:                   env("TELEGRAM_LOG_LEVEL", "warn"),
	}, nil
}

func EnsureDirs(s Settings) error {
	for _, dir := range []string{s.SessionDir, s.ConfigDir, s.ThumbnailsDir()} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	return nil
}

func LoadOrCreatePathSecret(s Settings) (string, error) {
	if s.PathSecret != "" {
		return s.PathSecret, nil
	}
	path := s.PathSecretFile()
	if data, err := os.ReadFile(path); err == nil {
		return strings.TrimSpace(string(data)), nil
	}
	secret, err := randomSecret(32)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, []byte(secret), 0o600); err != nil {
		return "", err
	}
	return secret, nil
}

// LoadOrCreateMediamtxHLSCDNSecret returns MEDIAMTX_HLS_CDN_SECRET from env, a persisted
// file under ConfigDir, or a newly generated random secret (never a hardcoded default).
func LoadOrCreateMediamtxHLSCDNSecret(s Settings) (string, error) {
	if s.MediamtxHLSCDNSecret != "" {
		return s.MediamtxHLSCDNSecret, nil
	}
	path := s.MediamtxHLSCDNSecretFile()
	if data, err := os.ReadFile(path); err == nil {
		if secret := strings.TrimSpace(string(data)); secret != "" {
			return secret, nil
		}
	}
	secret, err := randomSecret(32)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, []byte(secret), 0o600); err != nil {
		return "", err
	}
	return secret, nil
}

func RotatePathSecret(s Settings) (string, error) {
	if err := os.MkdirAll(s.ConfigDir, 0o755); err != nil {
		return "", err
	}
	secret, err := randomSecret(32)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(s.PathSecretFile(), []byte(secret), 0o600); err != nil {
		return "", err
	}
	return secret, nil
}

func SessionExists(s Settings) bool {
	_, err := os.Stat(s.SessionPath())
	return err == nil
}

func randomSecret(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func envFloat(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil {
			return n
		}
	}
	return fallback
}
