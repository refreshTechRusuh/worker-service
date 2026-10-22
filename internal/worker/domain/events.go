// Package domain mendefinisikan tipe event yang dikonsumsi worker-service dari Kafka.
package domain

// BookingCreatedEvent adalah payload topic booking.created.
// Dikirim Booking Service setelah POST /bookings sukses.
type BookingCreatedEvent struct {
	BookingID   string      `json:"bookingId"`
	EventID     string      `json:"eventId"`
	Email       string      `json:"email"`       // tujuan email instruksi bayar
	Items       []ItemEvent `json:"items"`
	TotalAmount int64       `json:"totalAmount"` // total dalam rupiah
}

// PaymentSucceededEvent adalah payload topic payment.succeeded.
// Dikirim Payment Service setelah callback Midtrans settlement/capture.
type PaymentSucceededEvent struct {
	PaymentID string      `json:"paymentId"`
	BookingID string      `json:"bookingId"`
	EventID   string      `json:"eventId"`
	Email     string      `json:"email"`       // tujuan email e-tiket
	EventNama string      `json:"eventNama"`   // nama event untuk e-tiket
	Method    string      `json:"method"`
	Items     []ItemEvent `json:"items"`
}

// PaymentExpiredEvent adalah payload topic payment.expired.
// Dikirim Payment Service / Booking sweeper saat TTL habis.
type PaymentExpiredEvent struct {
	PaymentID string `json:"paymentId"`
	BookingID string `json:"bookingId"`
	EventID   string `json:"eventId"`
	Email     string `json:"email"` // tujuan email expired notice
}

// ItemEvent adalah satu tiket dalam event payload.
type ItemEvent struct {
	CategoryID   string `json:"categoryId"`
	CategoryNama string `json:"categoryNama"` // nama kategori untuk e-tiket
	NIK          string `json:"nik"`
	Nama         string `json:"nama"`         // nama penonton
	Price        int64  `json:"price"`
	TicketCode   string `json:"ticketCode"`   // kode tiket unik, mis. TKT-VIP-99412-1
}
