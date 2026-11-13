package mailer

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateQRDataURL(t *testing.T) {
	got, err := GenerateQRDataURL("booking-123:ticket-456")
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(got, "data:image/png;base64,"))
	assert.NotEmpty(t, strings.TrimPrefix(got, "data:image/png;base64,"))

	_, err = GenerateQRDataURL(strings.Repeat("too-long-", 2000))
	assert.Error(t, err)
}

func TestBuildQRCodeURLQueryEscapesPayload(t *testing.T) {
	got := BuildQRCodeURL("booking 123/ticket?x=1")
	assert.Contains(t, got, "https://api.qrserver.com/v1/create-qr-code/")
	assert.Contains(t, got, "data=booking+123%2Fticket%3Fx%3D1")
}

func TestRenderPaymentInstructionsConditionalFields(t *testing.T) {
	html, err := RenderPaymentInstructions(PaymentInstructionsData{
		BuyerName:      "Buyer",
		Email:          "buyer@example.test",
		BookingID:      "booking-1",
		EventName:      "Concert",
		WaktuTransaksi: time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC),
		TotalAmount:    1234567,
		Items: []TicketLineItem{
			{Nomor: 1, CategoryName: "VIP", Name: "Ayu", NIK: "123", Price: 1234567},
			{Nomor: 2, CategoryName: "Regular", NIK: "456", Price: 0},
		},
		PaymentDeadline: "1 Januari 2026, 07:00 WIB",
	})
	require.NoError(t, err)
	assert.Contains(t, html, "Concert")
	assert.Contains(t, html, "a.n. Ayu")
	assert.Contains(t, html, "1.234.567")
	assert.Contains(t, html, "456")
}

func TestRenderETicketQRCodeAndOptionalTicketCode(t *testing.T) {
	html, err := RenderETicket(ETicketData{
		Email: "buyer@example.test", BookingID: "booking-2", EventName: "Concert", TotalAmount: 2500,
		Items: []TicketLineItem{
			{Nomor: 1, CategoryName: "VIP", Name: "Ayu", NIK: "123", Price: 2500, TicketCode: "TKT-1", QRCodeBase64: "https://example.test/qr.png"},
			{Nomor: 2, CategoryName: "Regular", Name: "Budi", NIK: "456", Price: 0},
		},
	})
	require.NoError(t, err)
	assert.Contains(t, html, `src="https://example.test/qr.png"`)
	assert.Contains(t, html, "<svg")
	assert.Contains(t, html, "TKT-1")
	assert.Contains(t, html, "BUDI")
}

func TestRenderPaymentExpiredOptionalEventName(t *testing.T) {
	withEvent, err := RenderPaymentExpired(PaymentExpiredData{Email: "buyer@example.test", BookingID: "booking-3", EventName: "Concert"})
	require.NoError(t, err)
	assert.Contains(t, withEvent, "Concert")
	withoutEvent, err := RenderPaymentExpired(PaymentExpiredData{BookingID: "booking-4"})
	require.NoError(t, err)
	assert.NotContains(t, withoutEvent, "<p style=\"color:#ef4444")
}

func TestRenderErrorPathsAndHelpers(t *testing.T) {
	_, err := render("{{", nil)
	assert.Error(t, err)
	_, err = render("{{.Missing}}", struct{}{})
	assert.Error(t, err)

	assert.Equal(t, "Rp 1.234.567", formatRupiah(1234567))
	assert.Equal(t, "Rp 0", formatRupiah(0))
	assert.Equal(t, "1 Januari 2026, 07:30 WIB", FormatTimeWIB(time.Date(2026, time.January, 1, 0, 30, 0, 0, time.UTC)))
	assert.Equal(t, []int{1, 2, 3}, templateFuncs["seq"].(func(int) []int)(3))
}
