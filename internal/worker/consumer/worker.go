// Package consumer mengimplementasikan Kafka consumer untuk worker-service.
package consumer

import (
	"context"
	"crypto/tls"
	"log/slog"
	"strings"
	"time"

	"github.com/refreshTechRusuh/worker-service/internal/config"
	"github.com/refreshTechRusuh/worker-service/internal/worker/handler"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

const (
	TopicBookingCreated   = "booking.created"
	TopicPaymentSucceeded = "payment.succeeded"
	TopicPaymentExpired   = "payment.expired"
)

// Worker adalah Kafka consumer runner untuk notification worker.
type Worker struct {
	readers []*kafkago.Reader
	handler *handler.NotificationHandler
	log     *slog.Logger
}

func New(cfg config.KafkaConfig, h *handler.NotificationHandler, log *slog.Logger) *Worker {
	topics := []string{TopicBookingCreated, TopicPaymentSucceeded, TopicPaymentExpired}
	readers := make([]*kafkago.Reader, 0, len(topics))

	for _, topic := range topics {
		dialer := &kafkago.Dialer{Timeout: 10 * time.Second}
		if cfg.TLS || cfg.Username != "" {
			dialer.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		if cfg.Username != "" {
			dialer.SASLMechanism = plain.Mechanism{Username: cfg.Username, Password: cfg.Password}
		}

		r := kafkago.NewReader(kafkago.ReaderConfig{
			Brokers:     strings.Split(cfg.Brokers, ","),
			Topic:       topic,
			GroupID:     cfg.ConsumerGroup,
			MinBytes:    1,
			MaxBytes:    10e6,
			StartOffset: kafkago.LastOffset,
			Dialer:      dialer,
		})
		readers = append(readers, r)
	}

	return &Worker{readers: readers, handler: h, log: log}
}

// Start memulai semua consumer dalam goroutine terpisah.
func (w *Worker) Start(ctx context.Context) {
	for _, r := range w.readers {
		go w.consume(ctx, r)
	}
}

// Close menutup semua reader.
func (w *Worker) Close() {
	for _, r := range w.readers {
		r.Close()
	}
}

func (w *Worker) consume(ctx context.Context, r *kafkago.Reader) {
	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // context cancelled — graceful shutdown
			}
			w.log.Error("kafka read error", slog.String("topic", r.Config().Topic), slog.Any("err", err))
			time.Sleep(5 * time.Second) // backoff sebelum retry
			continue
		}

		var handlerErr error
		switch msg.Topic {
		case TopicBookingCreated:
			handlerErr = w.handler.HandleBookingCreated(ctx, msg.Value)
		case TopicPaymentSucceeded:
			handlerErr = w.handler.HandlePaymentSucceeded(ctx, msg.Value)
		case TopicPaymentExpired:
			handlerErr = w.handler.HandlePaymentExpired(ctx, msg.Value)
		}

		if handlerErr != nil {
			w.log.Error("handler error",
				slog.String("topic", msg.Topic),
				slog.Int64("offset", msg.Offset),
				slog.Any("err", handlerErr),
			)
			// TODO: publish ke retry topic setelah N kali gagal → dead-letter topic (DLT)
		}
	}
}
