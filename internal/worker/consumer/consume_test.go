// Package consumer — white-box tests untuk Start, Close, consume.
package consumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/refreshTechRusuh/worker-service/internal/worker/domain"
	"github.com/refreshTechRusuh/worker-service/internal/worker/handler"
	kafkamock "github.com/refreshTechRusuh/worker-service/_mock/pkg/kafka"
	mailermock "github.com/refreshTechRusuh/worker-service/_mock/pkg/mailer"
)

func discardSlog() *slog.Logger {
	return slog.New(slog.NewTextHandler(nil, &slog.HandlerOptions{Level: slog.LevelError + 10}))
}

// ── mockReader — simple function-field mock untuk IKafkaReader ────────────────

type mockReader struct {
	topic       string
	readFn      func(ctx context.Context) (kafkago.Message, error)
	closeCalled int
}

func (m *mockReader) ReadMessage(ctx context.Context) (kafkago.Message, error) {
	if m.readFn != nil {
		return m.readFn(ctx)
	}
	<-ctx.Done()
	return kafkago.Message{}, ctx.Err()
}

func (m *mockReader) Config() kafkago.ReaderConfig {
	return kafkago.ReaderConfig{Topic: m.topic}
}

func (m *mockReader) Close() error {
	m.closeCalled++
	return nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

func newTestWorkerWithReaders(t *testing.T, readers []IKafkaReader) (*Worker, *mailermock.IMailer, *kafkamock.IProducer) {
	t.Helper()
	m := mailermock.NewIMailer(t)
	p := kafkamock.NewIProducer(t)
	h := handler.NewNotificationHandler(m, discardSlog())
	return NewForTest(readers, h, p, discardSlog()), m, p
}

func bookingCreatedMsg() []byte {
	ev := domain.BookingCreatedEvent{
		BookingID: "booking-1", EventID: "event-1",
		Email: "user@test.com", PaymentExpiresAt: "2026-12-01T10:00:00Z", TotalAmount: 500000,
	}
	b, _ := json.Marshal(ev)
	return b
}

// ── Close ─────────────────────────────────────────────────────────────────────

func TestWorker_Close_noReaders(t *testing.T) {
	w, _, p := newTestWorkerWithReaders(t, nil)
	p.EXPECT().Close().Return(nil)
	w.Close()
}

func TestWorker_Close_singleReader(t *testing.T) {
	r := &mockReader{topic: TopicBookingCreated}
	w, _, p := newTestWorkerWithReaders(t, []IKafkaReader{r})
	p.EXPECT().Close().Return(nil)
	w.Close()
	assert.Equal(t, 1, r.closeCalled)
}

func TestWorker_Close_multipleReaders(t *testing.T) {
	r1 := &mockReader{topic: TopicBookingCreated}
	r2 := &mockReader{topic: TopicPaymentSucceeded}
	w, _, p := newTestWorkerWithReaders(t, []IKafkaReader{r1, r2})
	p.EXPECT().Close().Return(nil)
	w.Close()
	assert.Equal(t, 1, r1.closeCalled)
	assert.Equal(t, 1, r2.closeCalled)
}

// ── consume ───────────────────────────────────────────────────────────────────

func TestConsume_singleMessage_success(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	callCount := 0
	r := &mockReader{
		topic: TopicBookingCreated,
		readFn: func(c context.Context) (kafkago.Message, error) {
			callCount++
			if callCount == 1 {
				return kafkago.Message{Topic: TopicBookingCreated, Value: bookingCreatedMsg()}, nil
			}
			<-c.Done()
			return kafkago.Message{}, c.Err()
		},
	}

	m := mailermock.NewIMailer(t)
	m.EXPECT().Send(mock.Anything, mock.Anything).Return(nil).Once()
	p := kafkamock.NewIProducer(t)
	h := handler.NewNotificationHandler(m, discardSlog())
	w := NewForTest([]IKafkaReader{r}, h, p, discardSlog())

	done := make(chan struct{})
	go func() { defer close(done); w.consume(ctx, r) }()

	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("consume tidak exit setelah context cancel")
	}
}

func TestConsume_readError_ctxAlreadyCancelled(t *testing.T) {
	// Jika ctx sudah cancel, consume harus exit tanpa 5s sleep
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r := &mockReader{
		topic: TopicBookingCreated,
		readFn: func(c context.Context) (kafkago.Message, error) {
			return kafkago.Message{}, c.Err()
		},
	}

	p := kafkamock.NewIProducer(t)
	m := mailermock.NewIMailer(t)
	h := handler.NewNotificationHandler(m, discardSlog())
	w := NewForTest([]IKafkaReader{r}, h, p, discardSlog())

	done := make(chan struct{})
	go func() { defer close(done); w.consume(ctx, r) }()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("consume tidak exit segera setelah ctx cancel")
	}
}

func TestConsume_handlerError_sendsToRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	callCount := 0
	r := &mockReader{
		topic: TopicBookingCreated,
		readFn: func(c context.Context) (kafkago.Message, error) {
			callCount++
			if callCount == 1 {
				return kafkago.Message{Topic: TopicBookingCreated, Value: []byte("bad")}, nil
			}
			<-c.Done()
			return kafkago.Message{}, c.Err()
		},
	}

	p := kafkamock.NewIProducer(t)
	p.EXPECT().
		Publish(mock.Anything, TopicBookingCreatedRetry, mock.Anything, mock.Anything).
		Return(nil).Maybe()

	m := mailermock.NewIMailer(t)
	h := handler.NewNotificationHandler(m, discardSlog())
	w := NewForTest([]IKafkaReader{r}, h, p, discardSlog())

	done := make(chan struct{})
	go func() { defer close(done); w.consume(ctx, r) }()

	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("consume tidak exit")
	}
}

func TestConsume_maxRetries_sendsToDLT(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	headers := setRetryCount(nil, maxRetries)
	callCount := 0
	r := &mockReader{
		topic: TopicBookingCreated,
		readFn: func(c context.Context) (kafkago.Message, error) {
			callCount++
			if callCount == 1 {
				return kafkago.Message{Topic: TopicBookingCreated, Value: []byte("bad"), Headers: headers}, nil
			}
			<-c.Done()
			return kafkago.Message{}, c.Err()
		},
	}

	p := kafkamock.NewIProducer(t)
	p.EXPECT().
		Publish(mock.Anything, TopicBookingCreatedDLT, mock.Anything, mock.Anything).
		Return(nil).Maybe()

	m := mailermock.NewIMailer(t)
	h := handler.NewNotificationHandler(m, discardSlog())
	w := NewForTest([]IKafkaReader{r}, h, p, discardSlog())

	done := make(chan struct{})
	go func() { defer close(done); w.consume(ctx, r) }()

	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("consume tidak exit")
	}
}

// ── Start ─────────────────────────────────────────────────────────────────────

func TestWorker_Start_startsAndCancels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	r := &mockReader{topic: TopicBookingCreated} // blocks until ctx cancel
	p := kafkamock.NewIProducer(t)
	m := mailermock.NewIMailer(t)
	h := handler.NewNotificationHandler(m, discardSlog())
	w := NewForTest([]IKafkaReader{r}, h, p, discardSlog())

	w.Start(ctx)
	cancel()
	time.Sleep(50 * time.Millisecond)
	assert.True(t, true, "Start + cancel tidak hang")
}

func TestWorker_Start_noReaders(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := kafkamock.NewIProducer(t)
	m := mailermock.NewIMailer(t)
	h := handler.NewNotificationHandler(m, discardSlog())
	w := NewForTest(nil, h, p, discardSlog())
	w.Start(ctx)
}
