// Package domain mendefinisikan tipe event yang dikonsumsi worker-service dari Kafka.
package domain

// BookingCreatedEvent adalah payload topic booking.created.
type BookingCreatedEvent struct {
	BookingID   string      `json:"bookingId"`
	EventID     string      `json:"eventId"`
	Email       string      `json:"email"`
	Items       []ItemEvent `json:"items"`
	TotalAmount int64       `json:"totalAmount"`
}

// PaymentSucceededEvent adalah payload topic payment.succeeded.
type PaymentSucceededEvent struct {
	PaymentID string      `json:"paymentId"`
	BookingID string      `json:"bookingId"`
	EventID   string      `json:"eventId"`
	Method    string      `json:"method"`
	Items     []ItemEvent `json:"items"`
}

// PaymentExpiredEvent adalah payload topic payment.expired.
type PaymentExpiredEvent struct {
	PaymentID string `json:"paymentId"`
	BookingID string `json:"bookingId"`
	EventID   string `json:"eventId"`
}

// ItemEvent adalah satu tiket dalam event payload.
type ItemEvent struct {
	CategoryID string `json:"categoryId"`
	NIK        string `json:"nik"`
	Nama       string `json:"nama"`
	Price      int64  `json:"price"`
}
