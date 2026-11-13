package kafka

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingWriter struct {
	messages []kafkago.Message
	writeErr error
	closeErr error
	closed   bool
	ctx      context.Context
}

func (w *recordingWriter) WriteMessages(ctx context.Context, messages ...kafkago.Message) error {
	w.ctx = ctx
	w.messages = append(w.messages, messages...)
	return w.writeErr
}

func (w *recordingWriter) Close() error {
	w.closed = true
	return w.closeErr
}

func TestNewProducerConfiguresPlainTransport(t *testing.T) {
	producer := NewProducer(RetryConfig{Brokers: "broker-a:9092,broker-b:9092"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	writer, ok := producer.writer.(*kafkago.Writer)
	require.True(t, ok)
	transport, ok := writer.Transport.(*kafkago.Transport)
	require.True(t, ok)
	assert.Equal(t, "broker-a:9092,broker-b:9092", writer.Addr.String())
	assert.Nil(t, transport.TLS)
	assert.Nil(t, transport.SASL)
	assert.Equal(t, kafkago.RequireOne, writer.RequiredAcks)
	assert.False(t, writer.Async)
}

func TestNewProducerConfiguresTLSAndSASL(t *testing.T) {
	producer := NewProducer(RetryConfig{Brokers: "broker:9093", Username: "worker", Password: "secret"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	writer, ok := producer.writer.(*kafkago.Writer)
	require.True(t, ok)
	transport, ok := writer.Transport.(*kafkago.Transport)
	require.True(t, ok)
	require.NotNil(t, transport.TLS)
	assert.Equal(t, uint16(0x0303), transport.TLS.MinVersion)
	mechanism, ok := transport.SASL.(plain.Mechanism)
	require.True(t, ok)
	assert.Equal(t, "worker", mechanism.Username)
	assert.Equal(t, "secret", mechanism.Password)
}

func TestProducerPublishWritesMessage(t *testing.T) {
	writer := &recordingWriter{}
	producer := &Producer{writer: writer, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx := context.Background()
	key, value := []byte("booking-1"), []byte(`{"id":"booking-1"}`)

	require.NoError(t, producer.Publish(ctx, "booking.created.retry", key, value))
	require.Len(t, writer.messages, 1)
	assert.Equal(t, "booking.created.retry", writer.messages[0].Topic)
	assert.Equal(t, key, writer.messages[0].Key)
	assert.Equal(t, value, writer.messages[0].Value)
	assert.False(t, writer.messages[0].Time.IsZero())
}

func TestProducerPublishWrapsWriterError(t *testing.T) {
	wantErr := errors.New("broker unavailable")
	writer := &recordingWriter{writeErr: wantErr}
	producer := &Producer{writer: writer, log: slog.New(slog.NewTextHandler(io.Discard, nil))}

	err := producer.Publish(context.Background(), "booking.created.retry", nil, []byte("payload"))
	require.Error(t, err)
	assert.ErrorIs(t, err, wantErr)
	assert.ErrorContains(t, err, "kafka producer: publish booking.created.retry")
	require.Len(t, writer.messages, 1)
}

func TestProducerCloseDelegatesToWriter(t *testing.T) {
	wantErr := errors.New("close failed")
	writer := &recordingWriter{closeErr: wantErr}
	producer := &Producer{writer: writer}

	assert.ErrorIs(t, producer.Close(), wantErr)
	assert.True(t, writer.closed)
}

func TestProducerPublishUsesCallerContext(t *testing.T) {
	writer := &recordingWriter{}
	producer := &Producer{writer: writer, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Minute))
	defer cancel()

	require.NoError(t, producer.Publish(ctx, "topic", nil, nil))
	require.NotNil(t, writer.ctx)
	gotDeadline, gotHasDeadline := writer.ctx.Deadline()
	wantDeadline, wantHasDeadline := ctx.Deadline()
	assert.Equal(t, wantHasDeadline, gotHasDeadline)
	assert.Equal(t, wantDeadline, gotDeadline)
}
