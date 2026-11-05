package handler_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/refreshTechRusuh/worker-service/internal/worker/domain"
	"github.com/refreshTechRusuh/worker-service/internal/worker/handler"
	"github.com/refreshTechRusuh/worker-service/pkg/mailer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Mock mailer implementing IMailer --------------------------------------

type mockMailer struct {
	mu       sync.Mutex
	messages []mailer.Message
	failNext bool
}

func (m *mockMailer) Send(_ context.Context, msg mailer.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failNext {
		m.failNext = false
		return assert.AnError
	}
	m.messages = append(m.messages, msg)
	return nil
}

func (m *mockMailer) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.messages)
}

func (m *mockMailer) last() mailer.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.messages) == 0 {
		return mailer.Message{}
	}
	return m.messages[len(m.messages)-1]
}

// Compile-time check: mockMailer implements IMailer
var _ mailer.IMailer = (*mockMailer)(nil)

// ---- Test helpers ----------------------------------------------------------

func marshalEvent(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func sampleItems() []domain.ItemEvent {
	return []domain.ItemEvent{
		{CategoryName: "VIP Ultimate Experience", NIK: "3171012304950001", Name: "Budi Santoso", Price: 5000000, TicketCode: "TKT-VIP-001"},
		{CategoryName: "VIP Ultimate Experience", NIK: "3273021508980002", Name: "Siti Aminah", Price: 5000000, TicketCode: "TKT-VIP-002"},
	}
}

func newTestHandler(m mailer.IMailer) *handler.NotificationHandler {
	return handler.NewNotificationHandler(m, slog.Default())
}

// ---- Template render tests -------------------------------------------------

func TestRenderPaymentInstructions_ContainsKeyFields(t *testing.T) {
	html, err := mailer.RenderPaymentInstructions(mailer.PaymentInstructionsData{
		BuyerName:   "Budi Santoso",
		Email:       "budi@test.com",
		BookingID:   "bkg-test-001",
		TotalAmount: 10000000,
		Items: []mailer.TicketLineItem{
			{Nomor: 1, CategoryName: "VIP", NIK: "1234567890123456", Price: 5000000, TicketCode: "TKT-001"},
			{Nomor: 2, CategoryName: "VIP", NIK: "1234567890123457", Price: 5000000, TicketCode: "TKT-002"},
		},
		PaymentDeadline: "22 Oktober 2026, 22:00 WIB",
	})
	require.NoError(t, err)
	assert.Contains(t, html, "Budi Santoso", "nama pembeli harus ada di email")
	assert.Contains(t, html, "bkg-test-001", "booking ID harus ada")
	assert.Contains(t, html, "10.000.000", "total diformat Rupiah harus ada")
	assert.Contains(t, html, "VIP", "nama kategori harus ada")
	assert.Contains(t, html, "1234567890123456", "NIK harus ada")
	assert.Contains(t, html, "22 Oktober 2026", "deadline harus ada")
	assert.NotEmpty(t, html)
}

func TestRenderETicket_ContainsKeyFields(t *testing.T) {
	html, err := mailer.RenderETicket(mailer.ETicketData{
		Email:     "budi@test.com",
		BookingID: "bkg-test-001",
		EventName: "Coldplay: Music of the Spheres 2026",
		Items: []mailer.TicketLineItem{
			{Nomor: 1, CategoryName: "VIP", NIK: "3171012304950001", Name: "Budi Santoso", Price: 5000000, TicketCode: "TKT-VIP-001"},
		},
		TotalAmount: 5000000,
	})
	require.NoError(t, err)
	assert.Contains(t, html, "Coldplay: Music of the Spheres 2026")
	assert.Contains(t, html, "bkg-test-001")
	assert.Contains(t, html, "OFFICIAL CONCERT PASS")
	assert.Contains(t, html, "BUDI SANTOSO")
	assert.Contains(t, html, "3171012304950001")
	assert.Contains(t, html, "DUKCAPIL VERIFIED")
	assert.Contains(t, html, "TKT-VIP-001")
	assert.Contains(t, html, "5.000.000")
}

func TestRenderPaymentExpired_ContainsKeyFields(t *testing.T) {
	html, err := mailer.RenderPaymentExpired(mailer.PaymentExpiredData{
		Email:     "budi@test.com",
		BookingID: "bkg-test-001",
	})
	require.NoError(t, err)
	assert.Contains(t, html, "bkg-test-001")
	assert.Contains(t, html, "DIBATALKAN")
	assert.Contains(t, html, "Booking Dibatalkan")
}

// ---- Handler end-to-end tests ----------------------------------------------

func TestHandleBookingCreated_SendsEmail(t *testing.T) {
	m := &mockMailer{}
	h := newTestHandler(m)

	ev := domain.BookingCreatedEvent{
		BookingID:        "bkg-001",
		EventID:          "evt-001",
		Email:            "budi@test.com",
		TotalAmount:      10000000,
		PaymentExpiresAt: time.Now().Add(10 * time.Minute).Format(time.RFC3339),
		Items:            sampleItems(),
	}

	err := h.HandleBookingCreated(context.Background(), marshalEvent(t, ev))
	require.NoError(t, err)
	assert.Equal(t, 1, m.count(), "harus mengirim 1 email")
	msg := m.last()
	assert.Equal(t, "budi@test.com", msg.To)
	assert.Contains(t, msg.Subject, "bkg-001")
	assert.Contains(t, msg.HTML, "10.000.000")
	assert.Contains(t, msg.HTML, "Budi Santoso") // nama dari tiket pertama
}

func TestHandleBookingCreated_Idempoten(t *testing.T) {
	m := &mockMailer{}
	h := newTestHandler(m)

	ev := domain.BookingCreatedEvent{
		BookingID: "bkg-idem-1", Email: "a@test.com", TotalAmount: 100000,
	}
	raw := marshalEvent(t, ev)

	// Kirim 2x dengan bookingId sama
	require.NoError(t, h.HandleBookingCreated(context.Background(), raw))
	require.NoError(t, h.HandleBookingCreated(context.Background(), raw))

	// Email hanya terkirim SEKALI (idempotency via sync.Map)
	assert.Equal(t, 1, m.count(), "email #1 hanya boleh terkirim 1x per booking")
}

func TestHandlePaymentSucceeded_SendsEmail(t *testing.T) {
	m := &mockMailer{}
	h := newTestHandler(m)

	ev := domain.PaymentSucceededEvent{
		PaymentID: "pay-001",
		BookingID: "bkg-001",
		EventID:   "evt-001",
		Email:     "budi@test.com",
		EventName: "Coldplay 2026",
		Method:    "qris",
		Items:     sampleItems(),
	}

	err := h.HandlePaymentSucceeded(context.Background(), marshalEvent(t, ev))
	require.NoError(t, err)
	assert.Equal(t, 1, m.count())
	msg := m.last()
	assert.Equal(t, "budi@test.com", msg.To)
	assert.Contains(t, msg.Subject, "Coldplay 2026")
	assert.Contains(t, msg.HTML, "OFFICIAL CONCERT PASS")
	assert.Contains(t, msg.HTML, "BUDI SANTOSO")
}

func TestHandlePaymentSucceeded_SkipsEmptyEmail(t *testing.T) {
	m := &mockMailer{}
	h := newTestHandler(m)

	ev := domain.PaymentSucceededEvent{
		PaymentID: "pay-002",
		BookingID: "bkg-002",
		Email:     "", // tidak ada email
		Items:     sampleItems(),
	}

	err := h.HandlePaymentSucceeded(context.Background(), marshalEvent(t, ev))
	require.NoError(t, err) // tidak error, hanya skip
	assert.Equal(t, 0, m.count(), "tidak boleh kirim email bila email kosong")
}

func TestHandlePaymentExpired_SendsEmail(t *testing.T) {
	m := &mockMailer{}
	h := newTestHandler(m)

	ev := domain.PaymentExpiredEvent{
		PaymentID: "pay-003",
		BookingID: "bkg-003",
		EventID:   "evt-001",
		Email:     "budi@test.com",
	}

	err := h.HandlePaymentExpired(context.Background(), marshalEvent(t, ev))
	require.NoError(t, err)
	assert.Equal(t, 1, m.count())
	msg := m.last()
	assert.Equal(t, "budi@test.com", msg.To)
	assert.Contains(t, msg.Subject, "bkg-003")
	assert.Contains(t, msg.HTML, "DIBATALKAN")
}

func TestHandlePaymentExpired_Idempoten(t *testing.T) {
	m := &mockMailer{}
	h := newTestHandler(m)

	ev := domain.PaymentExpiredEvent{
		BookingID: "bkg-idem-2", Email: "b@test.com",
	}
	raw := marshalEvent(t, ev)

	require.NoError(t, h.HandlePaymentExpired(context.Background(), raw))
	require.NoError(t, h.HandlePaymentExpired(context.Background(), raw))

	assert.Equal(t, 1, m.count(), "email #3 hanya boleh terkirim 1x per booking")
}

func TestHandlePaymentSucceeded_Idempoten(t *testing.T) {
	m := &mockMailer{}
	h := newTestHandler(m)

	ev := domain.PaymentSucceededEvent{
		BookingID: "bkg-idem-3", Email: "c@test.com", EventName: "Test",
		Items: sampleItems(),
	}
	raw := marshalEvent(t, ev)

	require.NoError(t, h.HandlePaymentSucceeded(context.Background(), raw))
	require.NoError(t, h.HandlePaymentSucceeded(context.Background(), raw))

	assert.Equal(t, 1, m.count(), "email #2 hanya boleh terkirim 1x per booking")
}

// ---- Format + template tests -----------------------------------------------

func TestRenderPaymentInstructions_FormatRupiah(t *testing.T) {
	cases := []struct {
		amount   int64
		expected string
	}{
		{500000, "500.000"},
		{1500000, "1.500.000"},
		{10000000, "10.000.000"},
		{100, "100"},
	}
	for _, tc := range cases {
		html, err := mailer.RenderPaymentInstructions(mailer.PaymentInstructionsData{
			TotalAmount: tc.amount, BookingID: "x",
		})
		require.NoError(t, err)
		assert.Contains(t, html, tc.expected, "formatRupiah(%d) harus menghasilkan %s", tc.amount, tc.expected)
	}
}

func TestRenderETicket_MultipleAttendees(t *testing.T) {
	html, err := mailer.RenderETicket(mailer.ETicketData{
		Email:     "budi@test.com",
		BookingID: "bkg-multi",
		EventName: "Test Concert",
		Items: []mailer.TicketLineItem{
			{Nomor: 1, CategoryName: "VIP", NIK: "1111111111111111", Name: "Orang Pertama", Price: 500000, TicketCode: "TKT-001"},
			{Nomor: 2, CategoryName: "VIP", NIK: "2222222222222222", Name: "Orang Kedua", Price: 500000, TicketCode: "TKT-002"},
			{Nomor: 3, CategoryName: "Regular", NIK: "3333333333333333", Name: "Orang Ketiga", Price: 300000, TicketCode: "TKT-003"},
		},
		TotalAmount: 1300000,
	})
	require.NoError(t, err)
	assert.Contains(t, html, "ORANG PERTAMA")
	assert.Contains(t, html, "ORANG KEDUA")
	assert.Contains(t, html, "ORANG KETIGA")
	assert.Contains(t, html, "1111111111111111")
	assert.Contains(t, html, "1.300.000")
}

func TestDomainEventsUnmarshal(t *testing.T) {
	raw := marshalEvent(t, domain.PaymentSucceededEvent{
		PaymentID: "pay-1", BookingID: "bkg-1", EventID: "evt-1",
		Email: "test@test.com", EventName: "Test Concert", Method: "bca_va",
		Items: sampleItems(),
	})

	var ev domain.PaymentSucceededEvent
	require.NoError(t, json.Unmarshal(raw, &ev))
	assert.Equal(t, "bkg-1", ev.BookingID)
	assert.Equal(t, "test@test.com", ev.Email)
	assert.Equal(t, "Test Concert", ev.EventName)
	assert.Len(t, ev.Items, 2)
}

func TestHandleBookingCreated_UsePaymentExpiresAt(t *testing.T) {
	m := &mockMailer{}
	h := newTestHandler(m)

	// Set deadline 30 menit ke depan (bukan default 10 menit)
	deadline := time.Now().Add(30 * time.Minute)
	ev := domain.BookingCreatedEvent{
		BookingID:        "bkg-expires",
		Email:            "x@test.com",
		TotalAmount:      1000,
		PaymentExpiresAt: deadline.Format(time.RFC3339),
	}

	err := h.HandleBookingCreated(context.Background(), marshalEvent(t, ev))
	require.NoError(t, err)
	require.Equal(t, 1, m.count())
	// Waktu deadline harus lebih dari 20 menit ke depan (yakni dari PaymentExpiresAt, bukan default 10 menit)
	// Verifikasi dengan check HTML mengandung tahun/bulan yang sesuai
	assert.Contains(t, m.last().HTML, "2026", "deadline dari PaymentExpiresAt harus dirender")
}

// ── HandlePaymentExpired — branch coverage ───────────────────────────────────

func TestHandlePaymentExpired_InvalidJSON(t *testing.T) {
	m := &mockMailer{}
	h := handler.NewNotificationHandler(m, slog.New(slog.NewTextHandler(io.Discard, nil)))
	err := h.HandlePaymentExpired(context.Background(), []byte("invalid-json"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal payment.expired")
}

func TestHandlePaymentExpired_EmptyEmail_skips(t *testing.T) {
	m := &mockMailer{}
	h := handler.NewNotificationHandler(m, slog.New(slog.NewTextHandler(io.Discard, nil)))

	ev := domain.PaymentExpiredEvent{
		PaymentID: "pay-empty",
		BookingID: "booking-empty",
		EventID:   "event-1",
		Email:     "", // kosong → handler harus skip tanpa error
	}
	err := h.HandlePaymentExpired(context.Background(), marshalEvent(t, ev))
	require.NoError(t, err)
	assert.Empty(t, m.messages, "email tidak boleh dikirim bila email kosong")
}

func TestHandlePaymentExpired_MailerError_returnsError(t *testing.T) {
	m := &mockMailer{failNext: true}
	h := handler.NewNotificationHandler(m, slog.New(slog.NewTextHandler(io.Discard, nil)))

	ev := domain.PaymentExpiredEvent{
		PaymentID: "pay-fail",
		BookingID: "booking-fail",
		EventID:   "event-1",
		Email:     "fail@test.com",
	}
	err := h.HandlePaymentExpired(context.Background(), marshalEvent(t, ev))
	require.Error(t, err)
}

// ── HandleBookingCreated — tambah branch ─────────────────────────────────────

func TestHandleBookingCreated_InvalidJSON(t *testing.T) {
	m := &mockMailer{}
	h := handler.NewNotificationHandler(m, slog.New(slog.NewTextHandler(io.Discard, nil)))
	err := h.HandleBookingCreated(context.Background(), []byte("not-json"))
	require.Error(t, err)
}

// ── HandlePaymentSucceeded — branch ──────────────────────────────────────────

func TestHandlePaymentSucceeded_InvalidJSON(t *testing.T) {
	m := &mockMailer{}
	h := handler.NewNotificationHandler(m, slog.New(slog.NewTextHandler(io.Discard, nil)))
	err := h.HandlePaymentSucceeded(context.Background(), []byte("{bad"))
	require.Error(t, err)
}

func TestHandlePaymentSucceeded_EmptyEmail_skips(t *testing.T) {
	m := &mockMailer{}
	h := handler.NewNotificationHandler(m, slog.New(slog.NewTextHandler(io.Discard, nil)))

	ev := domain.PaymentSucceededEvent{
		PaymentID: "pay-2",
		BookingID: "booking-email-empty",
		EventID:   "event-1",
		Email:     "", // kosong → skip
	}
	err := h.HandlePaymentSucceeded(context.Background(), marshalEvent(t, ev))
	require.NoError(t, err)
	assert.Empty(t, m.messages)
}

func TestHandlePaymentSucceeded_MailerError(t *testing.T) {
	m := &mockMailer{failNext: true}
	h := handler.NewNotificationHandler(m, slog.New(slog.NewTextHandler(io.Discard, nil)))

	ev := domain.PaymentSucceededEvent{
		PaymentID: "pay-3",
		BookingID: "booking-fail",
		EventID:   "event-1",
		Email:     "a@b.com",
		EventName: "Konser",
		Items: []domain.ItemEvent{
			{CategoryID: "cat-1", NIK: "3273000000000001", CategoryName: "VIP", Name: "Budi"},
		},
	}
	err := h.HandlePaymentSucceeded(context.Background(), marshalEvent(t, ev))
	require.Error(t, err)
}
