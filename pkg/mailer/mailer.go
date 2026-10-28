// Package mailer menyediakan Gmail SMTP client untuk worker-service.
package mailer

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/smtp"
	"time"

	"github.com/refreshTechRusuh/worker-service/internal/config"
)

// Message adalah satu email yang akan dikirim.
type Message struct {
	To      string
	Subject string
	HTML    string
}

// IMailer adalah interface pengiriman email.
// Memungkinkan inject mock di unit test tanpa koneksi SMTP nyata.
type IMailer interface {
	Send(ctx context.Context, msg Message) error
}

// SMTPMailer mengimplementasikan IMailer via Gmail SMTP (port 587 STARTTLS).
type SMTPMailer struct {
	cfg config.EmailConfig
	log *slog.Logger
}

// New membuat instance SMTPMailer baru.
func New(cfg config.EmailConfig, log *slog.Logger) *SMTPMailer {
	return &SMTPMailer{cfg: cfg, log: log}
}

// Send mengirim email HTML via SMTP dengan retry 1x.
func (m *SMTPMailer) Send(ctx context.Context, msg Message) error {
	addr := fmt.Sprintf("%s:%d", m.cfg.Host, m.cfg.Port)
	auth := smtp.PlainAuth("", m.cfg.Username, m.cfg.Password, m.cfg.Host)

	headers := fmt.Sprintf(
		"From: %s\r\nTo: %s\r\nSubject: %s\r\nMIME-Version: 1.0\r\nContent-Type: text/html; charset=UTF-8\r\n\r\n",
		m.cfg.From, msg.To, msg.Subject,
	)
	body := headers + msg.HTML

	_ = &tls.Config{ServerName: m.cfg.Host} // TLS config for future dial variant

	var lastErr error
	for attempt := 1; attempt <= 2; attempt++ {
		lastErr = smtp.SendMail(addr, auth, m.cfg.Username, []string{msg.To}, []byte(body))
		if lastErr == nil {
			m.log.InfoContext(ctx, "email sent",
				slog.String("to", msg.To),
				slog.String("subject", msg.Subject),
				slog.Int("attempt", attempt))
			return nil
		}
		m.log.WarnContext(ctx, "smtp send failed",
			slog.Int("attempt", attempt),
			slog.String("to", msg.To),
			slog.String("error", lastErr.Error()))
		if attempt < 2 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
			}
		}
	}
	m.log.ErrorContext(ctx, "email failed after all attempts",
		slog.String("to", msg.To),
		slog.String("subject", msg.Subject),
		slog.String("error", lastErr.Error()))
	return fmt.Errorf("mailer: failed after 2 attempts: %w", lastErr)
}

// Compile-time check: SMTPMailer implements IMailer.
var _ IMailer = (*SMTPMailer)(nil)
