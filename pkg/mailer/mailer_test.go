package mailer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/smtp"
	"strings"
	"testing"

	"github.com/refreshTechRusuh/worker-service/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBuildsSMTPMailer(t *testing.T) {
	cfg := config.EmailConfig{Host: "smtp.example.test", Port: 587, Username: "user", Password: "pass", From: "from@example.test"}
	m := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	assert.Equal(t, cfg, m.cfg)
	require.NotNil(t, m.sendMail)
}

func TestSMTPMailerSendSuccess(t *testing.T) {
	cfg := config.EmailConfig{Host: "smtp.example.test", Port: 2525, Username: "sender@example.test", Password: "pass", From: "Rusuh <sender@example.test>"}
	m := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	calls := 0
	m.sendMail = func(addr string, auth smtp.Auth, from string, to []string, body []byte) error {
		calls++
		assert.Equal(t, "smtp.example.test:2525", addr)
		assert.NotNil(t, auth)
		assert.Equal(t, "sender@example.test", from)
		assert.Equal(t, []string{"buyer@example.test"}, to)
		assert.Contains(t, string(body), "To: buyer@example.test")
		assert.Contains(t, string(body), "Subject: Booking confirmed")
		assert.Contains(t, string(body), "<strong>ticket</strong>")
		return nil
	}

	err := m.Send(context.Background(), Message{To: "buyer@example.test", Subject: "Booking confirmed", HTML: "<strong>ticket</strong>"})
	require.NoError(t, err)
	assert.Equal(t, 1, calls)
}

func TestSMTPMailerSendStopsRetryWhenContextCancelled(t *testing.T) {
	m := New(config.EmailConfig{Host: "smtp.example.test", Port: 587, Username: "sender@example.test"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	m.sendMail = func(string, smtp.Auth, string, []string, []byte) error {
		calls++
		cancel()
		return errors.New("temporary SMTP failure")
	}

	err := m.Send(ctx, Message{To: "buyer@example.test", Subject: "Subject", HTML: "body"})
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, calls)
}

func TestSMTPMailerSendReturnsErrorAfterRetries(t *testing.T) {
	wantErr := errors.New("SMTP rejected message")
	m := New(config.EmailConfig{Host: "smtp.example.test", Port: 587, Username: "sender@example.test"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	calls := 0
	m.sendMail = func(string, smtp.Auth, string, []string, []byte) error {
		calls++
		return wantErr
	}

	err := m.Send(context.Background(), Message{To: "buyer@example.test", Subject: "Subject", HTML: "body"})
	require.Error(t, err)
	assert.ErrorIs(t, err, wantErr)
	assert.True(t, strings.Contains(err.Error(), "failed after 2 attempts"))
	assert.Equal(t, 2, calls)
}
