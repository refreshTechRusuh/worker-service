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
	mailer *mailer.SMTPMailer
	log    *slog.Logger
	// In-memory idempotency store — key: bookingId+":"+emailType.
	// Production: ganti dengan Redis SET NX atau tabel DB notification_sent_log.
	sent sync.Map
}

func NewNotificationHandler(m *mailer.SMTPMailer, log *slog.Logger) *NotificationHandler {
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
	items := mapItems(ev.Items)

	// Nama pembeli dari tiket pertama, fallback ke "Pelanggan"
	namaPembeli := "Pelanggan"
	if len(items) > 0 && items[0].Nama != "" {
		namaPembeli = items[0].Nama
	}

	// Deadline bayar = sekarang + 10 menit (sesuai TTL server)
	// Di production lebih akurat: sertakan payment_expires_at di event payload.
	deadline := mailer.FormatTimeWIB(time.Now().Add(10 * time.Minute))

	html, err := mailer.RenderPaymentInstructions(mailer.PaymentInstructionsData{
		NamaPembeli:     namaPembeli,
		Email:           ev.Email,
		BookingID:       ev.BookingID,
		WaktuTransaksi:  time.Now(),
		TotalAmount:     ev.TotalAmount,
		Items:           items,
		PaymentDeadline: deadline,
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

	items := mapItems(ev.Items)
	// Hitung total dari items
	var total int64
	for _, it := range items {
		total += it.Price
	}

	eventNama := ev.EventNama
	if eventNama == "" {
		eventNama = "Konser" // fallback
	}

	html, err := mailer.RenderETicket(mailer.ETicketData{
		Email:       ev.Email,
		BookingID:   ev.BookingID,
		EventNama:   eventNama,
		Items:       items,
		TotalAmount: total,
	})
	if err != nil {
		h.log.ErrorContext(ctx, "render e-ticket failed", slog.Any("err", err))
		return fmt.Errorf("render email #2: %w", err)
	}

	return h.mailer.Send(ctx, mailer.Message{
		To:      ev.Email,
		Subject: fmt.Sprintf("[Rusuh Booking] E-Tiket Resmi — %s", eventNama),
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
func mapItems(domainItems []domain.ItemEvent) []mailer.TicketLineItem {
	items := make([]mailer.TicketLineItem, 0, len(domainItems))
	for i, it := range domainItems {
		ticketCode := it.TicketCode
		if ticketCode == "" {
			ticketCode = fmt.Sprintf("TKT-%d", i+1)
		}
		items = append(items, mailer.TicketLineItem{
			Nomor:        i + 1,
			CategoryNama: it.CategoryNama,
			Nama:         it.Nama,
			NIK:          it.NIK,
			Price:        it.Price,
			TicketCode:   ticketCode,
		})
	}
	return items
}
