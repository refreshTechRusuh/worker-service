package config_test

import (
	"testing"

	"github.com/refreshTechRusuh/worker-service/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setRequired(t *testing.T) {
	t.Helper()
	t.Setenv("EMAIL_USERNAME", "test@example.com")
	t.Setenv("EMAIL_PASSWORD", "test-password")
}

func TestLoad_defaults(t *testing.T) {
	setRequired(t)

	cfg, err := config.Load()
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, "local", cfg.App.Env)
	assert.Equal(t, "info", cfg.Log.Level)
	assert.Equal(t, "json", cfg.Log.Format)
	assert.Equal(t, "localhost:9092", cfg.Kafka.Brokers)
	assert.Equal(t, "test@example.com", cfg.Email.Username)
	assert.Equal(t, "test-password", cfg.Email.Password)
}

func TestLoad_customKafka(t *testing.T) {
	setRequired(t)
	t.Setenv("KAFKA_BROKERS", "kafka1:9092,kafka2:9092")
	t.Setenv("KAFKA_TLS", "true")

	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, "kafka1:9092,kafka2:9092", cfg.Kafka.Brokers)
	assert.True(t, cfg.Kafka.TLS)
}

func TestLoad_customEmail(t *testing.T) {
	setRequired(t)
	t.Setenv("EMAIL_HOST", "smtp.myserver.com")
	t.Setenv("EMAIL_PORT", "465")
	t.Setenv("EMAIL_FROM", "no-reply@rusuh.id")

	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, "smtp.myserver.com", cfg.Email.Host)
	assert.Equal(t, 465, cfg.Email.Port)
	assert.Equal(t, "no-reply@rusuh.id", cfg.Email.From)
}

func TestLoad_customPort(t *testing.T) {
	setRequired(t)
	t.Setenv("PORT", "8084")

	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, 8084, cfg.App.Port)
}

func TestSetupLogger_levels(t *testing.T) {
	for _, level := range []string{"debug", "info", "warn", "error", "other"} {
		t.Run(level, func(t *testing.T) {
			config.SetupLogger(config.LogConfig{Level: level, Format: "json"})
		})
	}
	config.SetupLogger(config.LogConfig{Level: "info", Format: "text"})
}
