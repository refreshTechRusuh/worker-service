// Package handler mengimplementasikan logika notifikasi email untuk worker-service.
package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/refreshTechRusuh/worker-service/internal/worker/domain"
	"github.com/refreshTechRusuh/worker-service/pkg/mailer"
)

// NotificationHandler memproses 3 jenis event Kafka dan mengirim email HTML.
type NotificationHandler struct {
	mailer mailer.IMailer
	log    *slog.Logger
	// In-memory idempotency store — key: bookingId+":"+emailType.
	// Production: ganti dengan Redis SET NX atau tabel DB notification_sent_log.
	sent sync.Map
}

func NewNotificationHandler(m mailer.IMailer, log *slog.Logger) *NotificationHandler {
	return &NotificationHandler{mailer: m, log: log}
}

// HandleBookingCreated memproses topic booking.created → kirim email #1 (instruksi bayar).
// Req 9.1
func (h *NotificationHandler) HandleBookingCreated(ctx context.Context, raw []byte) error {
	var ev domain.BookingCreatedEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		return fmt.Errorf("unmarshal booking.created: %w", err)
	}

	// Idempotency — satu email per jenis per booking
	key := ev.BookingID + ":booking_created"
	if _, loaded := h.sent.LoadOrStore(key, true); loaded {
		h.log.InfoContext(ctx, "email #1 already sent (idempoten)", slog.String("bookingId", ev.BookingID))
		return nil
	}

	// Map event items → TicketLineItem
	// Email #1 (instruksi bayar) tidak perlu QR code, pass "" agar tidak di-generate.
	items := mapItems(ev.Items, "")

	// Nama pembeli dari tiket pertama, fallback ke "Pelanggan"
	buyerName := "Pelanggan"
	if len(items) > 0 && items[0].Name != "" {
		buyerName = items[0].Name
	}

	// Deadline bayar: gunakan PaymentExpiresAt dari event bila tersedia,
	// fallback ke sekarang + 10 menit.
	var paymentDeadline string
	if ev.PaymentExpiresAt != "" {
		if t, err := time.Parse(time.RFC3339, ev.PaymentExpiresAt); err == nil {
			paymentDeadline = mailer.FormatTimeWIB(t)
		}
	}
	if paymentDeadline == "" {
		paymentDeadline = mailer.FormatTimeWIB(time.Now().Add(10 * time.Minute))
	}

	html, err := mailer.RenderPaymentInstructions(mailer.PaymentInstructionsData{
		BuyerName:     buyerName,
		Email:           ev.Email,
		BookingID:       ev.BookingID,
		WaktuTransaksi:  time.Now(),
		TotalAmount:     ev.TotalAmount,
		Items:           items,
		PaymentDeadline: paymentDeadline,
	})
	if err != nil {
		h.log.ErrorContext(ctx, "render payment instructions failed", slog.Any("err", err))
		return fmt.Errorf("render email #1: %w", err)
	}

	return h.mailer.Send(ctx, mailer.Message{
		To:      ev.Email,
		Subject: fmt.Sprintf("[Rusuh Booking] Instruksi Pembayaran — %s", ev.BookingID),
		HTML:    html,
	})
}

// HandlePaymentSucceeded memproses topic payment.succeeded → kirim email #2 (e-tiket).
// Email #2 adalah satu-satunya email yang WAJIB terkirim (Req 9.2, Q11 CEO).
func (h *NotificationHandler) HandlePaymentSucceeded(ctx context.Context, raw []byte) error {
	var ev domain.PaymentSucceededEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		return fmt.Errorf("unmarshal payment.succeeded: %w", err)
	}

	key := ev.BookingID + ":payment_succeeded"
	if _, loaded := h.sent.LoadOrStore(key, true); loaded {
		h.log.InfoContext(ctx, "email #2 already sent (idempoten)", slog.String("bookingId", ev.BookingID))
		return nil
	}

	if ev.Email == "" {
		h.log.WarnContext(ctx, "payment.succeeded event has no email field, skipping",
			slog.String("bookingId", ev.BookingID))
		return nil // tidak bisa kirim tanpa email — log warning, jangan error (tidak retry)
	}

	items := mapItems(ev.Items, ev.BookingID)
	// Hitung total dari items
	var total int64
	for _, it := range items {
		total += it.Price
	}

	eventName := ev.EventName
	if eventName == "" {
		eventName = "Konser" // fallback
	}

	html, err := mailer.RenderETicket(mailer.ETicketData{
		Email:       ev.Email,
		BookingID:   ev.BookingID,
		EventName:   eventName,
		Items:       items,
		TotalAmount: total,
	})
	if err != nil {
		h.log.ErrorContext(ctx, "render e-ticket failed", slog.Any("err", err))
		return fmt.Errorf("render email #2: %w", err)
	}

	return h.mailer.Send(ctx, mailer.Message{
		To:      ev.Email,
		Subject: fmt.Sprintf("[Rusuh Booking] E-Tiket Resmi — %s", eventName),
		HTML:    html,
	})
}

// HandlePaymentExpired memproses topic payment.expired → kirim email #3 (booking dibatalkan).
func (h *NotificationHandler) HandlePaymentExpired(ctx context.Context, raw []byte) error {
	var ev domain.PaymentExpiredEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		return fmt.Errorf("unmarshal payment.expired: %w", err)
	}

	key := ev.BookingID + ":payment_expired"
	if _, loaded := h.sent.LoadOrStore(key, true); loaded {
		h.log.InfoContext(ctx, "email #3 already sent (idempoten)", slog.String("bookingId", ev.BookingID))
		return nil
	}

	if ev.Email == "" {
		h.log.WarnContext(ctx, "payment.expired event has no email field, skipping",
			slog.String("bookingId", ev.BookingID))
		return nil
	}

	html, err := mailer.RenderPaymentExpired(mailer.PaymentExpiredData{
		Email:     ev.Email,
		BookingID: ev.BookingID,
	})
	if err != nil {
		h.log.ErrorContext(ctx, "render payment expired failed", slog.Any("err", err))
		return fmt.Errorf("render email #3: %w", err)
	}

	return h.mailer.Send(ctx, mailer.Message{
		To:      ev.Email,
		Subject: fmt.Sprintf("[Rusuh Booking] Booking Dibatalkan — %s", ev.BookingID),
		HTML:    html,
	})
}

// ---- helpers ----------------------------------------------------------------

// mapItems mengkonversi []domain.ItemEvent ke []mailer.TicketLineItem.
// bookingID dipakai sebagai konten QR code (kalau kosong, QR tidak di-generate).
func mapItems(domainItems []domain.ItemEvent, bookingID string) []mailer.TicketLineItem {
	items := make([]mailer.TicketLineItem, 0, len(domainItems))
	for i, it := range domainItems {
		ticketCode := it.TicketCode
		if ticketCode == "" {
			ticketCode = fmt.Sprintf("TKT-%d", i+1)
		}

		// QR content: "<bookingID>:<ticketCode>" sehingga unik per tiket.
		// Scanner di gerbang bisa parse bookingID dan ticketCode dari satu scan.
		qrContent := bookingID
		if ticketCode != "" {
			qrContent = bookingID + ":" + ticketCode
		}
		qrDataURL, err := mailer.GenerateQRDataURL(qrContent)
		if err != nil {
			// Kalau gagal generate QR, fallback ke SVG placeholder di template.
			qrDataURL = ""
		}

		items = append(items, mailer.TicketLineItem{
			Nomor:        i + 1,
			CategoryName: it.CategoryName,
			Name:         it.Name,
			NIK:          it.NIK,
			Price:        it.Price,
			TicketCode:   ticketCode,
			QRCodeBase64: qrDataURL,
		})
	}
	return items
}
