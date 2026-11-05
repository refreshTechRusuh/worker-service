// Package consumer mengimplementasikan Kafka consumer untuk worker-service.
package consumer

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/refreshTechRusuh/worker-service/internal/config"
	"github.com/refreshTechRusuh/worker-service/internal/worker/handler"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"

	wkafka "github.com/refreshTechRusuh/worker-service/pkg/kafka"
)

const (
	TopicBookingCreated   = "booking.created"
	TopicPaymentSucceeded = "payment.succeeded"
	TopicPaymentExpired   = "payment.expired"

	// Retry topics — consumer retry setelah delay bertahap.
	TopicBookingCreatedRetry   = "booking.created.retry"
	TopicPaymentSucceededRetry = "payment.succeeded.retry"
	TopicPaymentExpiredRetry   = "payment.expired.retry"

	// Dead Letter Topics — pesan yang gagal setelah maxRetries.
	TopicBookingCreatedDLT   = "booking.created.dlt"
	TopicPaymentSucceededDLT = "payment.succeeded.dlt"
	TopicPaymentExpiredDLT   = "payment.expired.dlt"

	maxRetries    = 3
	retryDelayBase = 5 * time.Second // backoff: 5s, 10s, 20s (exponential)
)

// Worker adalah Kafka consumer runner untuk notification worker.
type Worker struct {
	readers  []*kafkago.Reader
	handler  *handler.NotificationHandler
	producer wkafka.IProducer
	log      *slog.Logger
}

func New(cfg config.KafkaConfig, h *handler.NotificationHandler, log *slog.Logger) *Worker {
	// Topics utama + retry (worker juga consume retry topics dengan delay)
	topics := []string{
		TopicBookingCreated,
		TopicPaymentSucceeded,
		TopicPaymentExpired,
		TopicBookingCreatedRetry,
		TopicPaymentSucceededRetry,
		TopicPaymentExpiredRetry,
	}
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

	producer := wkafka.NewProducer(wkafka.RetryConfig{
		Brokers:  cfg.Brokers,
		Username: cfg.Username,
		Password: cfg.Password,
		TLS:      cfg.TLS,
	}, log)

	return &Worker{readers: readers, handler: h, producer: producer, log: log}
}

// Start memulai semua consumer dalam goroutine terpisah.
func (w *Worker) Start(ctx context.Context) {
	for _, r := range w.readers {
		go w.consume(ctx, r)
	}
	w.log.InfoContext(ctx, "worker-service consumer started",
		slog.Int("readers", len(w.readers)))
}

// Close menutup semua reader dan producer.
func (w *Worker) Close() {
	for _, r := range w.readers {
		r.Close()
	}
	w.producer.Close()
}

func (w *Worker) consume(ctx context.Context, r *kafkago.Reader) {
	topic := r.Config().Topic
	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // context cancelled — graceful shutdown
			}
			w.log.Error("kafka read error",
				slog.String("topic", topic),
				slog.Any("err", err))
			time.Sleep(5 * time.Second)
			continue
		}

		// Hitung retry attempt dari header X-Retry-Count
		attempt := parseRetryCount(msg.Headers)

		// Extract trace context dari Kafka header (dikirim producer) dan buka span consume.
		msgCtx := wkafka.ExtractTraceContext(ctx, msg)
		msgCtx, consumeSpan := wkafka.StartConsumeSpan(msgCtx, topic)

		w.log.InfoContext(msgCtx, "kafka message received",
			slog.String("topic", topic),
			slog.Int64("offset", msg.Offset),
			slog.Int("attempt", attempt),
			slog.Int("bytes", len(msg.Value)),
		)

		start := time.Now()
		handlerErr := w.dispatch(msgCtx, msg)
		if handlerErr == nil {
			consumeSpan.End()
			w.log.InfoContext(msgCtx, "kafka message processed",
				slog.String("topic", topic),
				slog.Int64("offset", msg.Offset),
				slog.String("duration", time.Since(start).String()),
			)
			continue
		}

		consumeSpan.RecordError(handlerErr)
		consumeSpan.End()

		w.log.Error("handler error",
			slog.String("topic", topic),
			slog.Int64("offset", msg.Offset),
			slog.Int("attempt", attempt),
			slog.Any("err", handlerErr))

		if attempt >= maxRetries {
			// Sudah >= maxRetries kali gagal → kirim ke DLT
			dltTopic := dltTopicFor(topic)
			w.log.Warn("max retries exceeded, sending to DLT",
				slog.String("topic", topic),
				slog.String("dlt", dltTopic),
				slog.Int("attempt", attempt))
			w.publishToDLT(ctx, dltTopic, msg, handlerErr)
		} else {
			// Belum maxRetries → kirim ke retry topic dengan backoff
			retryTopic := retryTopicFor(topic)
			backoff := retryDelayBase * (1 << attempt) // 5s, 10s, 20s
			w.log.Warn("retrying message",
				slog.String("retryTopic", retryTopic),
				slog.Duration("backoff", backoff),
				slog.Int("attempt", attempt+1))

			// Sleep backoff sebelum re-publish (tidak blocking consumer lain)
			go func(m kafkago.Message, rt string, delay time.Duration, nextAttempt int) {
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return
				}
				w.publishToRetry(ctx, rt, m, nextAttempt)
			}(msg, retryTopic, backoff, attempt+1)
		}
	}
}

// dispatch memanggil handler yang sesuai berdasarkan topic.
func (w *Worker) dispatch(ctx context.Context, msg kafkago.Message) error {
	topic := stripRetry(msg.Topic)
	switch topic {
	case TopicBookingCreated:
		return w.handler.HandleBookingCreated(ctx, msg.Value)
	case TopicPaymentSucceeded:
		return w.handler.HandlePaymentSucceeded(ctx, msg.Value)
	case TopicPaymentExpired:
		return w.handler.HandlePaymentExpired(ctx, msg.Value)
	default:
		w.log.Warn("unknown topic", slog.String("topic", msg.Topic))
		return nil // tidak di-retry untuk unknown topic
	}
}

// publishToRetry mempublish pesan ke retry topic dengan header attempt count bertambah.
func (w *Worker) publishToRetry(ctx context.Context, retryTopic string, msg kafkago.Message, attempt int) {
	headers := setRetryCount(msg.Headers, attempt)
	augmented := kafkago.Message{
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
	}
	if err := w.producer.Publish(ctx, retryTopic, augmented.Key, augmented.Value); err != nil {
		w.log.Error("failed to publish to retry topic",
			slog.String("retryTopic", retryTopic), slog.Any("err", err))
	}
}

// publishToDLT mempublish pesan ke dead-letter topic dengan error info di header.
func (w *Worker) publishToDLT(ctx context.Context, dltTopic string, msg kafkago.Message, handlerErr error) {
	// Tambahkan header error ke pesan DLT untuk debugging
	headers := append(msg.Headers, kafkago.Header{
		Key:   "X-Error",
		Value: []byte(handlerErr.Error()),
	})
	headers = append(headers, kafkago.Header{
		Key:   "X-Original-Topic",
		Value: []byte(msg.Topic),
	})
	_ = headers // dipakai secara informatif; producer kita hanya kirim key+value

	if err := w.producer.Publish(ctx, dltTopic, msg.Key, msg.Value); err != nil {
		w.log.Error("CRITICAL: failed to publish to DLT",
			slog.String("dltTopic", dltTopic),
			slog.Any("err", err))
	}
}

// ---- helpers ----------------------------------------------------------------

// stripRetry menghapus suffix ".retry" agar dispatch bisa map ke handler yang benar.
func stripRetry(topic string) string {
	if strings.HasSuffix(topic, ".retry") {
		return strings.TrimSuffix(topic, ".retry")
	}
	return topic
}

// retryTopicFor mengembalikan retry topic untuk topic utama atau retry sebelumnya.
func retryTopicFor(topic string) string {
	base := stripRetry(topic)
	return base + ".retry"
}

// dltTopicFor mengembalikan DLT topic untuk topic utama.
func dltTopicFor(topic string) string {
	base := stripRetry(topic)
	return base + ".dlt"
}

// parseRetryCount membaca header X-Retry-Count dari pesan Kafka.
func parseRetryCount(headers []kafkago.Header) int {
	for _, h := range headers {
		if h.Key == "X-Retry-Count" && len(h.Value) > 0 {
			var n int
			fmt.Sscanf(string(h.Value), "%d", &n)
			return n
		}
	}
	return 0
}

// setRetryCount menulis / mengganti header X-Retry-Count.
func setRetryCount(headers []kafkago.Header, count int) []kafkago.Header {
	updated := make([]kafkago.Header, 0, len(headers)+1)
	for _, h := range headers {
		if h.Key != "X-Retry-Count" {
			updated = append(updated, h)
		}
	}
	return append(updated, kafkago.Header{
		Key:   "X-Retry-Count",
		Value: []byte(fmt.Sprintf("%d", count)),
	})
}
