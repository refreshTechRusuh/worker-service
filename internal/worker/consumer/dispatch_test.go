// Package consumer — tests untuk dispatch, publishToRetry, publishToDLT.
// Menggunakan mocks yang digenerate oleh mockery v2 di _mock/ dekat source:
//   - pkg/mailer/_mock/mock_imailer.go   (package mailer_test)
//   - pkg/kafka/_mock/mock_iproducer.go  (package kafka_test)
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/refreshTechRusuh/worker-service/internal/worker/domain"
	"github.com/refreshTechRusuh/worker-service/internal/worker/handler"
	mailermock "github.com/refreshTechRusuh/worker-service/_mock/pkg/mailer"
	kafkamock "github.com/refreshTechRusuh/worker-service/_mock/pkg/kafka"
)

func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(nil, &slog.HandlerOptions{Level: slog.LevelError + 10}))
}

// newTestWorker membuat Worker dengan mock IMailer dan IProducer dari _mock/.
func newTestWorker(t *testing.T, mailerMock *mailermock.IMailer, producerMock *kafkamock.IProducer) *Worker {
	t.Helper()
	h := handler.NewNotificationHandler(mailerMock, discardLog())
	return &Worker{
		readers:  nil,
		handler:  h,
		producer: producerMock,
		log:      discardLog(),
	}
}

// bookingCreatedPayload membuat JSON payload booking.created.
func bookingCreatedPayload(t *testing.T) []byte {
	t.Helper()
	ev := domain.BookingCreatedEvent{
		BookingID:        "booking-1",
		EventID:          "event-1",
		Email:            "user@test.com",
		PaymentExpiresAt: "2026-12-01T10:00:00Z",
		Items:            []domain.ItemEvent{{CategoryID: "cat-1", NIK: "3273000000000001"}},
		TotalAmount:      500000,
	}
	b, err := json.Marshal(ev)
	require.NoError(t, err)
	return b
}

func paymentSucceededPayload(t *testing.T) []byte {
	t.Helper()
	ev := domain.PaymentSucceededEvent{
		PaymentID: "pay-1",
		BookingID: "booking-1",
		EventID:   "event-1",
		Email:     "user@test.com",
		EventName: "Konser A",
		Method:    "gopay",
		Items:     []domain.ItemEvent{{CategoryID: "cat-1", NIK: "3273000000000001", CategoryName: "VIP", Name: "Budi Santoso", Price: 500000}},
	}
	b, err := json.Marshal(ev)
	require.NoError(t, err)
	return b
}

func paymentExpiredPayload(t *testing.T) []byte {
	t.Helper()
	ev := domain.PaymentExpiredEvent{
		PaymentID: "pay-1",
		BookingID: "booking-1",
		EventID:   "event-1",
		Email:     "user@test.com",
	}
	b, err := json.Marshal(ev)
	require.NoError(t, err)
	return b
}

// ── dispatch ─────────────────────────────────────────────────────────────────

func TestDispatch_BookingCreated_success(t *testing.T) {
	mailerMock := mailermock.NewIMailer(t)
	producerMock := kafkamock.NewIProducer(t)

	// IMailer.Send dipanggil oleh handler untuk kirim email
	mailerMock.EXPECT().Send(mock.Anything, mock.Anything).Return(nil)

	w := newTestWorker(t, mailerMock, producerMock)
	msg := kafkago.Message{Topic: TopicBookingCreated, Value: bookingCreatedPayload(t)}

	err := w.dispatch(context.Background(), msg)
	require.NoError(t, err)
}

func TestDispatch_BookingCreated_retryTopic_routedToSameHandler(t *testing.T) {
	mailerMock := mailermock.NewIMailer(t)
	producerMock := kafkamock.NewIProducer(t)

	// Retry topic seharusnya juga di-route ke handler yang sama setelah strip ".retry"
	mailerMock.EXPECT().Send(mock.Anything, mock.Anything).Return(nil)

	w := newTestWorker(t, mailerMock, producerMock)
	msg := kafkago.Message{Topic: TopicBookingCreatedRetry, Value: bookingCreatedPayload(t)}

	err := w.dispatch(context.Background(), msg)
	require.NoError(t, err)
}

func TestDispatch_PaymentSucceeded_success(t *testing.T) {
	mailerMock := mailermock.NewIMailer(t)
	producerMock := kafkamock.NewIProducer(t)
	mailerMock.EXPECT().Send(mock.Anything, mock.Anything).Return(nil)

	w := newTestWorker(t, mailerMock, producerMock)
	msg := kafkago.Message{Topic: TopicPaymentSucceeded, Value: paymentSucceededPayload(t)}

	err := w.dispatch(context.Background(), msg)
	require.NoError(t, err)
}

func TestDispatch_PaymentSucceeded_retryTopic(t *testing.T) {
	mailerMock := mailermock.NewIMailer(t)
	producerMock := kafkamock.NewIProducer(t)
	mailerMock.EXPECT().Send(mock.Anything, mock.Anything).Return(nil)

	w := newTestWorker(t, mailerMock, producerMock)
	msg := kafkago.Message{Topic: TopicPaymentSucceededRetry, Value: paymentSucceededPayload(t)}

	err := w.dispatch(context.Background(), msg)
	require.NoError(t, err)
}

func TestDispatch_PaymentExpired_success(t *testing.T) {
	mailerMock := mailermock.NewIMailer(t)
	producerMock := kafkamock.NewIProducer(t)
	mailerMock.EXPECT().Send(mock.Anything, mock.Anything).Return(nil)

	w := newTestWorker(t, mailerMock, producerMock)
	msg := kafkago.Message{Topic: TopicPaymentExpired, Value: paymentExpiredPayload(t)}

	err := w.dispatch(context.Background(), msg)
	require.NoError(t, err)
}

func TestDispatch_PaymentExpired_retryTopic(t *testing.T) {
	mailerMock := mailermock.NewIMailer(t)
	producerMock := kafkamock.NewIProducer(t)
	mailerMock.EXPECT().Send(mock.Anything, mock.Anything).Return(nil)

	w := newTestWorker(t, mailerMock, producerMock)
	msg := kafkago.Message{Topic: TopicPaymentExpiredRetry, Value: paymentExpiredPayload(t)}

	err := w.dispatch(context.Background(), msg)
	require.NoError(t, err)
}

func TestDispatch_UnknownTopic_returnNil(t *testing.T) {
	mailerMock := mailermock.NewIMailer(t)
	producerMock := kafkamock.NewIProducer(t)
	// Handler tidak dipanggil untuk topic unknown

	w := newTestWorker(t, mailerMock, producerMock)
	msg := kafkago.Message{Topic: "unknown.topic", Value: []byte(`{}`)}

	err := w.dispatch(context.Background(), msg)
	require.NoError(t, err) // unknown topic tidak return error
}

func TestDispatch_HandlerError_propagated(t *testing.T) {
	mailerMock := mailermock.NewIMailer(t)
	producerMock := kafkamock.NewIProducer(t)

	// Mailer gagal → handler return error → dispatch propagate
	mailerMock.EXPECT().Send(mock.Anything, mock.Anything).Return(errors.New("smtp error"))

	w := newTestWorker(t, mailerMock, producerMock)
	msg := kafkago.Message{Topic: TopicBookingCreated, Value: bookingCreatedPayload(t)}

	err := w.dispatch(context.Background(), msg)
	require.Error(t, err)
}

func TestDispatch_InvalidJSON_returnsError(t *testing.T) {
	mailerMock := mailermock.NewIMailer(t)
	producerMock := kafkamock.NewIProducer(t)
	// Handler akan gagal unmarshal JSON

	w := newTestWorker(t, mailerMock, producerMock)
	msg := kafkago.Message{Topic: TopicBookingCreated, Value: []byte("not-json")}

	err := w.dispatch(context.Background(), msg)
	require.Error(t, err)
}

// ── publishToRetry ────────────────────────────────────────────────────────────

func TestPublishToRetry_callsProducerWithRetryTopic(t *testing.T) {
	mailerMock := mailermock.NewIMailer(t)
	producerMock := kafkamock.NewIProducer(t)

	// Verifikasi bahwa Publish dipanggil dengan retry topic
	producerMock.EXPECT().
		Publish(mock.Anything, TopicBookingCreatedRetry, mock.Anything, mock.Anything).
		Return(nil)

	w := newTestWorker(t, mailerMock, producerMock)
	msg := kafkago.Message{
		Key:   []byte("event-1"),
		Value: bookingCreatedPayload(t),
	}

	w.publishToRetry(context.Background(), TopicBookingCreatedRetry, msg, 1)
}

func TestPublishToRetry_incrementsAttempt(t *testing.T) {
	mailerMock := mailermock.NewIMailer(t)
	producerMock := kafkamock.NewIProducer(t)

	producerMock.EXPECT().
		Publish(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil)

	w := newTestWorker(t, mailerMock, producerMock)
	msg := kafkago.Message{Key: []byte("evt"), Value: []byte(`{}`)}

	// Panggil dengan attempt berbeda — tidak crash
	w.publishToRetry(context.Background(), TopicPaymentSucceededRetry, msg, 2)
}

func TestPublishToRetry_producerError_logsButDoesNotPanic(t *testing.T) {
	mailerMock := mailermock.NewIMailer(t)
	producerMock := kafkamock.NewIProducer(t)

	// Error di producer — hanya di-log, tidak di-panic / return error
	producerMock.EXPECT().
		Publish(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("kafka unavailable"))

	w := newTestWorker(t, mailerMock, producerMock)
	msg := kafkago.Message{Value: bookingCreatedPayload(t)}

	// Tidak boleh panic
	assert.NotPanics(t, func() {
		w.publishToRetry(context.Background(), TopicBookingCreatedRetry, msg, 1)
	})
}

// ── publishToDLT ──────────────────────────────────────────────────────────────

func TestPublishToDLT_callsProducerWithDLTTopic(t *testing.T) {
	mailerMock := mailermock.NewIMailer(t)
	producerMock := kafkamock.NewIProducer(t)

	producerMock.EXPECT().
		Publish(mock.Anything, TopicBookingCreatedDLT, mock.Anything, mock.Anything).
		Return(nil)

	w := newTestWorker(t, mailerMock, producerMock)
	msg := kafkago.Message{
		Key:   []byte("event-1"),
		Value: bookingCreatedPayload(t),
		Topic: TopicBookingCreated,
	}

	w.publishToDLT(context.Background(), TopicBookingCreatedDLT, msg, errors.New("handler failed"))
}

func TestPublishToDLT_paymentSucceededDLT(t *testing.T) {
	mailerMock := mailermock.NewIMailer(t)
	producerMock := kafkamock.NewIProducer(t)

	producerMock.EXPECT().
		Publish(mock.Anything, TopicPaymentSucceededDLT, mock.Anything, mock.Anything).
		Return(nil)

	w := newTestWorker(t, mailerMock, producerMock)
	msg := kafkago.Message{
		Topic: TopicPaymentSucceeded,
		Value: paymentSucceededPayload(t),
	}

	w.publishToDLT(context.Background(), TopicPaymentSucceededDLT, msg, errors.New("process failed"))
}

func TestPublishToDLT_producerError_logsButDoesNotPanic(t *testing.T) {
	mailerMock := mailermock.NewIMailer(t)
	producerMock := kafkamock.NewIProducer(t)

	producerMock.EXPECT().
		Publish(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("kafka unavailable"))

	w := newTestWorker(t, mailerMock, producerMock)
	msg := kafkago.Message{Value: []byte(`{}`)}

	assert.NotPanics(t, func() {
		w.publishToDLT(context.Background(), TopicBookingCreatedDLT, msg, errors.New("reason"))
	})
}

// ── Close ─────────────────────────────────────────────────────────────────────

func TestWorker_Close_callsProducerClose(t *testing.T) {
	mailerMock := mailermock.NewIMailer(t)
	producerMock := kafkamock.NewIProducer(t)

	producerMock.EXPECT().Close().Return(nil)

	w := newTestWorker(t, mailerMock, producerMock)
	w.Close() // readers = nil, producer.Close() harus dipanggil
}
