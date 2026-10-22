// Package handler mengimplementasikan logika notifikasi email untuk worker-service.
package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/refreshTechRusuh/worker-service/internal/worker/domain"
	"github.com/refreshTechRusuh/worker-service/pkg/mailer"
)

// NotificationHandler memproses 3 jenis event Kafka dan mengirim email.
type NotificationHandler struct {
	mailer *mailer.SMTPMailer
	log    *slog.Logger
	// In-memory idempotency store (production: ganti dengan Redis atau DB).
	// Key: bookingId + ":" + emailType → sudah terkirim.
	sent sync.Map
}

func NewNotificationHandler(m *mailer.SMTPMailer, log *slog.Logger) *NotificationHandler {
	return &NotificationHandler{mailer: m, log: log}
}

// HandleBookingCreated memproses topic booking.created → kirim email #1 (instruksi bayar).
func (h *NotificationHandler) HandleBookingCreated(ctx context.Context, raw []byte) error {
	var ev domain.BookingCreatedEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		return fmt.Errorf("unmarshal booking.created: %w", err)
	}

	key := ev.BookingID + ":booking_created"
	if _, loaded := h.sent.LoadOrStore(key, true); loaded {
		h.log.InfoContext(ctx, "email #1 already sent (idempoten)", "bookingId", ev.BookingID)
		return nil
	}

	// TODO: render template email #1 (instruksi bayar) menggunakan mailer.RenderPaymentInstructions
	// Untuk Fase 0 skeleton: kirim placeholder body
	html := fmt.Sprintf("<h1>Booking %s dibuat</h1><p>Segera bayar sebelum batas waktu.</p><p>Total: Rp %d</p>",
		ev.BookingID, ev.TotalAmount)

	return h.mailer.Send(ctx, mailer.Message{
		To:      ev.Email,
		Subject: fmt.Sprintf("[Rusuh Booking] Instruksi Pembayaran — %s", ev.BookingID),
		HTML:    html,
	})
}

// HandlePaymentSucceeded memproses topic payment.succeeded → kirim email #2 (e-tiket).
func (h *NotificationHandler) HandlePaymentSucceeded(ctx context.Context, raw []byte) error {
	var ev domain.PaymentSucceededEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		return fmt.Errorf("unmarshal payment.succeeded: %w", err)
	}

	key := ev.BookingID + ":payment_succeeded"
	if _, loaded := h.sent.LoadOrStore(key, true); loaded {
		h.log.InfoContext(ctx, "email #2 already sent (idempoten)", "bookingId", ev.BookingID)
		return nil
	}

	// TODO: render full e-tiket HTML menggunakan mailer.RenderETicket(ETicketData{...})
	// dan embed QR code per tiket menggunakan github.com/skip2/go-qrcode.
	// Untuk Fase 0 skeleton: kirim placeholder e-tiket.
	html := fmt.Sprintf("<h1>E-Tiket — Booking %s</h1><p>Pembayaran berhasil. Simpan email ini untuk masuk venue.</p>",
		ev.BookingID)

	// Email tujuan diambil dari DB booking (TODO: inject BookingReader ke handler).
	// Sementara mock dari event payload (TODO: tambahkan field email ke PaymentSucceededEvent).
	email := "placeholder@email.com" // ganti dengan lookup dari DB

	return h.mailer.Send(ctx, mailer.Message{
		To:      email,
		Subject: fmt.Sprintf("[Rusuh Booking] E-Tiket Resmi — %s", ev.BookingID),
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
		h.log.InfoContext(ctx, "email #3 already sent (idempoten)", "bookingId", ev.BookingID)
		return nil
	}

	// TODO: render email #3 (expired notice).
	html := fmt.Sprintf("<h1>Booking %s Dibatalkan</h1><p>Waktu pembayaran telah habis. Silakan coba kembali.</p>",
		ev.BookingID)

	email := "placeholder@email.com" // ganti dengan lookup dari DB

	return h.mailer.Send(ctx, mailer.Message{
		To:      email,
		Subject: fmt.Sprintf("[Rusuh Booking] Booking Dibatalkan — %s", ev.BookingID),
		HTML:    html,
	})
}
