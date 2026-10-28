package config

import (
	"log/slog"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	App   AppConfig
	Log   LogConfig
	Kafka KafkaConfig
	Email EmailConfig
}

type AppConfig struct {
	Env  string
	Port int
}

type LogConfig struct {
	Level  string
	Format string
}

type KafkaConfig struct {
	Brokers       string
	Username      string
	Password      string
	TLS           bool
	ConsumerGroup string
}

type EmailConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	From     string
}

func Load() (*Config, error) {
	_ = godotenv.Load()
	return &Config{
		App:   AppConfig{Env: getEnv("APP_ENV", "local"), Port: getEnvInt("APP_PORT", 8080)},
		Log:   LogConfig{Level: getEnv("LOG_LEVEL", "info"), Format: getEnv("LOG_FORMAT", "json")},
		Kafka: KafkaConfig{
			Brokers:       getEnv("KAFKA_BROKERS", "localhost:9092"),
			Username:      getEnv("KAFKA_USERNAME", ""),
			Password:      getEnv("KAFKA_PASSWORD", ""),
			TLS:           getEnvBool("KAFKA_TLS", false),
			ConsumerGroup: getEnv("KAFKA_CONSUMER_GROUP", "worker-service"),
		},
		Email: EmailConfig{
			Host:     getEnv("EMAIL_HOST", "smtp.gmail.com"),
			Port:     getEnvInt("EMAIL_PORT", 587),
			Username: mustEnv("EMAIL_USERNAME"),
			Password: mustEnv("EMAIL_PASSWORD"),
			From:     getEnv("EMAIL_FROM", "Rusuh Booking Tiket <noreply@gmail.com>"),
		},
	}, nil
}

func SetupLogger(cfg LogConfig) {
	level := slog.LevelInfo
	switch cfg.Level {
	case "debug": level = slog.LevelDebug
	case "warn":  level = slog.LevelWarn
	case "error": level = slog.LevelError
	}
	var h slog.Handler
	if cfg.Format == "text" {
		h = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	} else {
		h = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	}
	slog.SetDefault(slog.New(h))
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" { return v }
	return fallback
}
func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil { return i }
	}
	return fallback
}
func getEnvBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil { return b }
	}
	return fallback
}
func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" { slog.Error("required env not set", "key", key); os.Exit(1) }
	return v
}
