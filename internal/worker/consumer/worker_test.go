// Package consumer tests — menggunakan white-box testing (package consumer)
// untuk mengakses fungsi helper yang tidak diekspor.
package consumer

import (
	"io"
	"log/slog"
	"testing"

	"github.com/refreshTechRusuh/worker-service/internal/config"
	"github.com/refreshTechRusuh/worker-service/internal/worker/handler"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBuildsReadersWithKafkaSecuritySettings(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	notificationHandler := handler.NewNotificationHandler(nil, logger)
	worker := New(config.KafkaConfig{
		Brokers:       "broker-a:9092,broker-b:9092",
		Username:      "worker",
		Password:      "secret",
		TLS:           true,
		ConsumerGroup: "notifications",
	}, notificationHandler, logger)
	t.Cleanup(worker.Close)

	expectedTopics := []string{
		TopicBookingCreated, TopicPaymentSucceeded, TopicPaymentExpired,
		TopicBookingCreatedRetry, TopicPaymentSucceededRetry, TopicPaymentExpiredRetry,
	}
	require.Len(t, worker.readers, len(expectedTopics))
	for i, reader := range worker.readers {
		cfg := reader.Config()
		assert.Equal(t, expectedTopics[i], cfg.Topic)
		assert.Equal(t, "notifications", cfg.GroupID)
		assert.Equal(t, []string{"broker-a:9092", "broker-b:9092"}, cfg.Brokers)
		require.NotNil(t, cfg.Dialer.TLS)
		assert.Equal(t, uint16(0x0303), cfg.Dialer.TLS.MinVersion)
		mechanism, ok := cfg.Dialer.SASLMechanism.(plain.Mechanism)
		require.True(t, ok)
		assert.Equal(t, "worker", mechanism.Username)
		assert.Equal(t, "secret", mechanism.Password)
	}
}

func TestNewBuildsReadersWithoutKafkaSecurity(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	worker := New(config.KafkaConfig{Brokers: "localhost:9092", ConsumerGroup: "default"}, nil, logger)
	t.Cleanup(worker.Close)

	require.Len(t, worker.readers, 6)
	for _, reader := range worker.readers {
		cfg := reader.Config()
		assert.Equal(t, []string{"localhost:9092"}, cfg.Brokers)
		assert.Nil(t, cfg.Dialer.TLS)
		assert.Nil(t, cfg.Dialer.SASLMechanism)
	}
}

// ── stripRetry ────────────────────────────────────────────────────────────────

func TestStripRetry_noSuffix(t *testing.T) {
	assert.Equal(t, "booking.created", stripRetry("booking.created"))
	assert.Equal(t, "payment.succeeded", stripRetry("payment.succeeded"))
}

func TestStripRetry_withRetry(t *testing.T) {
	assert.Equal(t, "booking.created", stripRetry("booking.created.retry"))
	assert.Equal(t, "payment.expired", stripRetry("payment.expired.retry"))
}

func TestStripRetry_emptyString(t *testing.T) {
	assert.Equal(t, "", stripRetry(""))
}

// ── retryTopicFor ─────────────────────────────────────────────────────────────

func TestRetryTopicFor_mainTopic(t *testing.T) {
	assert.Equal(t, "booking.created.retry", retryTopicFor("booking.created"))
	assert.Equal(t, "payment.succeeded.retry", retryTopicFor("payment.succeeded"))
}

func TestRetryTopicFor_retryTopic(t *testing.T) {
	// Retry dari retry topic harus tetap sama retry topic
	assert.Equal(t, "booking.created.retry", retryTopicFor("booking.created.retry"))
}

// ── dltTopicFor ───────────────────────────────────────────────────────────────

func TestDltTopicFor_mainTopic(t *testing.T) {
	assert.Equal(t, "booking.created.dlt", dltTopicFor("booking.created"))
	assert.Equal(t, "payment.expired.dlt", dltTopicFor("payment.expired"))
}

func TestDltTopicFor_retryTopic(t *testing.T) {
	// DLT dari retry topic → strip .retry dulu
	assert.Equal(t, "booking.created.dlt", dltTopicFor("booking.created.retry"))
}

// ── parseRetryCount ───────────────────────────────────────────────────────────

func TestParseRetryCount_noHeader(t *testing.T) {
	count := parseRetryCount([]kafkago.Header{})
	assert.Equal(t, 0, count)
}

func TestParseRetryCount_nilHeaders(t *testing.T) {
	count := parseRetryCount(nil)
	assert.Equal(t, 0, count)
}

func TestParseRetryCount_withHeader(t *testing.T) {
	headers := []kafkago.Header{
		{Key: "X-Retry-Count", Value: []byte("2")},
	}
	count := parseRetryCount(headers)
	assert.Equal(t, 2, count)
}

func TestParseRetryCount_withOtherHeaders(t *testing.T) {
	headers := []kafkago.Header{
		{Key: "X-Other", Value: []byte("value")},
		{Key: "X-Retry-Count", Value: []byte("3")},
	}
	count := parseRetryCount(headers)
	assert.Equal(t, 3, count)
}

func TestParseRetryCount_firstAttempt(t *testing.T) {
	headers := []kafkago.Header{
		{Key: "X-Retry-Count", Value: []byte("0")},
	}
	count := parseRetryCount(headers)
	assert.Equal(t, 0, count)
}

// ── setRetryCount ─────────────────────────────────────────────────────────────

func TestSetRetryCount_addsHeader(t *testing.T) {
	headers := setRetryCount(nil, 1)
	require.Len(t, headers, 1)
	assert.Equal(t, "X-Retry-Count", headers[0].Key)
	assert.Equal(t, "1", string(headers[0].Value))
}

func TestSetRetryCount_replacesExisting(t *testing.T) {
	existing := []kafkago.Header{
		{Key: "X-Retry-Count", Value: []byte("1")},
		{Key: "X-Other", Value: []byte("val")},
	}
	headers := setRetryCount(existing, 2)

	// Harus tetap ada X-Other
	var retryVal, otherVal string
	for _, h := range headers {
		if h.Key == "X-Retry-Count" {
			retryVal = string(h.Value)
		}
		if h.Key == "X-Other" {
			otherVal = string(h.Value)
		}
	}
	assert.Equal(t, "2", retryVal, "X-Retry-Count harus diupdate")
	assert.Equal(t, "val", otherVal, "X-Other harus tetap ada")
}

func TestSetRetryCount_preservesOtherHeaders(t *testing.T) {
	existing := []kafkago.Header{
		{Key: "traceparent", Value: []byte("trace-id")},
	}
	headers := setRetryCount(existing, 0)
	assert.Len(t, headers, 2, "harus ada header asli + X-Retry-Count baru")
}

// ── topic constants ───────────────────────────────────────────────────────────

func TestTopicConstants(t *testing.T) {
	assert.Equal(t, "booking.created", TopicBookingCreated)
	assert.Equal(t, "payment.succeeded", TopicPaymentSucceeded)
	assert.Equal(t, "payment.expired", TopicPaymentExpired)

	assert.Equal(t, TopicBookingCreated+".retry", TopicBookingCreatedRetry)
	assert.Equal(t, TopicPaymentSucceeded+".retry", TopicPaymentSucceededRetry)
	assert.Equal(t, TopicPaymentExpired+".retry", TopicPaymentExpiredRetry)

	assert.Equal(t, TopicBookingCreated+".dlt", TopicBookingCreatedDLT)
	assert.Equal(t, TopicPaymentSucceeded+".dlt", TopicPaymentSucceededDLT)
	assert.Equal(t, TopicPaymentExpired+".dlt", TopicPaymentExpiredDLT)
}

// ── maxRetries constant ───────────────────────────────────────────────────────

func TestMaxRetries_value(t *testing.T) {
	assert.Equal(t, 3, maxRetries, "maxRetries harus 3 sesuai desain DLT")
}
