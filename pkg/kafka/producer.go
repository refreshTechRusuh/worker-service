// Package kafka menyediakan Kafka producer wrapper untuk worker-service.
// Dipakai untuk publish ke retry topic dan dead-letter topic (DLT).
package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

// RetryConfig adalah konfigurasi koneksi Kafka untuk producer.
type RetryConfig struct {
	Brokers  string
	Username string
	Password string
	TLS      bool
}

// Producer menulis pesan ke satu topic Kafka.
type Producer struct {
	writer messageWriter
	log    *slog.Logger
}

type messageWriter interface {
	WriteMessages(context.Context, ...kafkago.Message) error
	Close() error
}

// NewProducer membuat Producer baru yang terhubung ke broker.
func NewProducer(cfg RetryConfig, log *slog.Logger) *Producer {
	transport := &kafkago.Transport{}
	if cfg.TLS || cfg.Username != "" {
		transport.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if cfg.Username != "" {
		transport.SASL = plain.Mechanism{
			Username: cfg.Username,
			Password: cfg.Password,
		}
	}

	w := &kafkago.Writer{
		Addr:         kafkago.TCP(strings.Split(cfg.Brokers, ",")...),
		Balancer:     &kafkago.LeastBytes{},
		RequiredAcks: kafkago.RequireOne,
		Async:        false,
		Transport:    transport,
	}

	return &Producer{writer: w, log: log}
}

// Publish mengirim satu raw message ke topic.
func (p *Producer) Publish(ctx context.Context, topic string, key, value []byte) error {
	msg := kafkago.Message{
		Topic: topic,
		Key:   key,
		Value: value,
		Time:  time.Now(),
	}

	// Buka span kafka.produce dan inject traceparent ke message header.
	ctx, span := StartProduceSpan(ctx, topic)
	defer span.End()
	InjectTraceContext(ctx, &msg)

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		span.RecordError(err)
		p.log.ErrorContext(ctx, "kafka publish failed",
			slog.String("topic", topic),
			slog.String("error", err.Error()))
		return fmt.Errorf("kafka producer: publish %s: %w", topic, err)
	}
	return nil
}

// Close menutup koneksi writer.
func (p *Producer) Close() error { return p.writer.Close() }

// IProducer abstracts Kafka producer untuk testability.
// *Producer memenuhi interface ini di production; mock di test.
type IProducer interface {
	Publish(ctx context.Context, topic string, key, value []byte) error
	Close() error
}
