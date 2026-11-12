// Package mailer — template rendering.
// Tiga template email sesuai "Template Email Rusuh Booking Tiket.html":
//   1. RenderPaymentInstructions  → email #1 (booking.created)
//   2. RenderETicket              → email #2 (payment.succeeded) — e-tiket dengan boarding pass per NIK
//   3. RenderPaymentExpired       → email #3 (payment.expired)
package mailer

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"html/template"
	"net/url"
	"strings"
	"time"

	qrcode "github.com/skip2/go-qrcode"
)

// ---- Data types -------------------------------------------------------------

// PaymentInstructionsData adalah data untuk email #1.
type PaymentInstructionsData struct {
	BuyerName       string         // dari field Items[0].Name, atau "Pelanggan"
	Email            string
	BookingID        string
	EventName        string         // nama event untuk ditampilkan di email
	WaktuTransaksi   time.Time
	TotalAmount      int64          // rupiah
	Items            []TicketLineItem
	PaymentDeadline  string         // formatted, mis. "22 Oktober 2026, 21:40 WIB"
}

// ETicketData adalah data untuk email #2.
type ETicketData struct {
	Email      string
	BookingID  string
	EventName  string
	Items      []TicketLineItem
	TotalAmount int64
}

// PaymentExpiredData adalah data untuk email #3.
type PaymentExpiredData struct {
	Email     string
	BookingID string
	EventName string // nama event untuk konteks pembatalan
}

// TicketLineItem adalah satu tiket dalam email.
type TicketLineItem struct {
	Nomor        int
	CategoryName string // mis. "VIP Ultimate Experience"
	Name         string // nama penonton (UPPERCASE di e-tiket)
	NIK          string // 16 digit
	Price        int64
	TicketCode   string // mis. "TKT-VIP-99412-1"
	QRCodeBase64 string // data URL "data:image/png;base64,..." untuk inline embed di HTML
}

// ---- Template functions -----------------------------------------------------

var templateFuncs = template.FuncMap{
	"formatRupiah": formatRupiah,
	"formatTime":   formatTimeWIB,
	"upper":        strings.ToUpper,
	"seq":          func(n int) []int { s := make([]int, n); for i := range s { s[i] = i + 1 }; return s },
}

// GenerateQRDataURL membuat QR code dari content dan mengembalikan data URL
// "data:image/png;base64,..." yang bisa langsung dipakai di atribut src tag <img>.
// Size 256 cukup untuk scan di gerbang turnstile.
// Dipanggil dari notification_handler.go sebelum render template.
// CATATAN: data URI diblokir oleh Gmail dan mayoritas email client.
// Gunakan BuildQRCodeURL untuk email, GenerateQRDataURL hanya untuk non-email use case.
func GenerateQRDataURL(content string) (string, error) {
	png, err := qrcode.Encode(content, qrcode.Medium, 256)
	if err != nil {
		return "", fmt.Errorf("generate qr code: %w", err)
	}
	return "data:image/png;base64," + base64.StdEncoding.EncodeToString(png), nil
}

// BuildQRCodeURL membangun URL ke QR code API service eksternal.
// Menggunakan api.qrserver.com yang mendukung HTTPS dan tidak butuh API key.
// Email client akan fetch gambar dari URL ini saat email dibuka — tidak diblokir.
func BuildQRCodeURL(content string) string {
	encoded := url.QueryEscape(content)
	return fmt.Sprintf(
		"https://api.qrserver.com/v1/create-qr-code/?size=256x256&data=%s&format=png&margin=4",
		encoded,
	)
}

// RenderPaymentInstructions merender HTML email #1 (instruksi bayar).
func RenderPaymentInstructions(data PaymentInstructionsData) (string, error) {
	return render(paymentInstructionsTmpl, data)
}

// RenderETicket merender HTML email #2 (e-tiket boarding pass).
func RenderETicket(data ETicketData) (string, error) {
	return render(eTicketTmpl, data)
}

// RenderPaymentExpired merender HTML email #3 (booking dibatalkan).
func RenderPaymentExpired(data PaymentExpiredData) (string, error) {
	return render(paymentExpiredTmpl, data)
}

func render(tmplStr string, data any) (string, error) {
	t, err := template.New("email").Funcs(templateFuncs).Parse(tmplStr)
	if err != nil {
		return "", fmt.Errorf("render template: %w", err)
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("execute template: %w", err)
	}
	return buf.String(), nil
}

// ---- Helpers ----------------------------------------------------------------

func formatRupiah(amount int64) string {
	s := fmt.Sprintf("%d", amount)
	n := len(s)
	result := make([]byte, 0, n+(n-1)/3)
	for i, c := range s {
		if i > 0 && (n-i)%3 == 0 {
			result = append(result, '.')
		}
	result = append(result, byte(c)) // #nosec G115 -- c is ASCII digit/separator, fits in byte
	}
	return "Rp " + string(result)
}

// FormatTimeWIB memformat time.Time ke string WIB — bisa dipakai dari luar package.
func FormatTimeWIB(t time.Time) string { return formatTimeWIB(t) }

func formatTimeWIB(t time.Time) string {
	loc, _ := time.LoadLocation("Asia/Jakarta")
	t = t.In(loc)
	months := [...]string{"", "Januari", "Februari", "Maret", "April", "Mei", "Juni",
		"Juli", "Agustus", "September", "Oktober", "November", "Desember"}
	return fmt.Sprintf("%d %s %d, %02d:%02d WIB",
		t.Day(), months[t.Month()], t.Year(), t.Hour(), t.Minute())
}

// ============================================================
// Template #1 — Konfirmasi Booking / Instruksi Bayar
// Dikirim saat booking.created (sebelum bayar).
// Design: sama dengan "Email Konfirmasi Pembayaran" di template HTML
//         tapi tanpa badge PAID — badge diganti dengan countdown bayar.
// ============================================================
const paymentInstructionsTmpl = `<!DOCTYPE html>
<html lang="id"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Instruksi Pembayaran – Rusuh Booking Tiket</title></head>
<body style="margin:0;padding:0;background-color:#f1f5f9;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#f1f5f9;padding:32px 12px;">
<tr><td align="center">
<table role="presentation" width="100%" style="max-width:620px;background-color:#ffffff;border-radius:18px;overflow:hidden;color:#1e293b;box-shadow:0 20px 40px -15px rgba(0,0,0,0.15);">

<!-- HEADER -->
<tr><td style="padding:28px 32px;background:linear-gradient(135deg,#0f172a 0%,#1e1b4b 100%);">
<table role="presentation" width="100%"><tr>
<td><h1 style="color:#ffffff;font-size:20px;font-weight:800;margin:0;font-family:Arial,sans-serif;">RUSUH BOOKING TIKET</h1>
<p style="color:#818cf8;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:1.5px;margin:4px 0 0 0;font-family:Arial,sans-serif;">PT. Tiga Sekawan Angin Ribut</p></td>
<td align="right" valign="middle">
<span style="background-color:#1e40af;color:#bfdbfe;font-size:11px;font-weight:700;padding:6px 12px;border-radius:9999px;border:1px solid #3b82f6;display:inline-block;font-family:Arial,sans-serif;">MENUNGGU PEMBAYARAN</span>
</td></tr></table></td></tr>

<!-- HERO -->
<tr><td style="padding:36px 32px 24px 32px;text-align:center;background-color:#f8fafc;border-bottom:1px solid #e2e8f0;">
<div style="width:58px;height:58px;background-color:#eff6ff;border-radius:50%;border:2px solid #93c5fd;margin:0 auto 16px auto;display:flex;align-items:center;justify-content:center;">
<svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="#3b82f6" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" xmlns="http://www.w3.org/2000/svg"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>
</div>
<h2 style="color:#0f172a;font-size:22px;font-weight:800;margin-bottom:8px;font-family:Arial,sans-serif;">Booking Berhasil Dibuat!</h2>
{{if .EventName}}<p style="color:#4f46e5;font-size:15px;font-weight:700;margin-bottom:8px;font-family:Arial,sans-serif;">{{.EventName}}</p>{{end}}
<p style="color:#64748b;font-size:14px;line-height:1.5;max-width:480px;margin:0 auto;font-family:Arial,sans-serif;">
Halo <strong style="color:#0f172a;">{{.BuyerName}}</strong>, booking tiket kamu sudah tercatat. Selesaikan pembayaran sebelum batas waktu untuk mengamankan tiket.
</p></td></tr>

<!-- TRANSACTION SUMMARY -->
<tr><td style="padding:28px 32px;">
<table role="presentation" width="100%" style="font-size:13px;color:#334155;margin-bottom:24px;">
<tr><td style="padding:6px 0;color:#64748b;font-family:Arial,sans-serif;">Nomor Booking</td>
<td align="right" style="padding:6px 0;font-family:monospace;font-weight:700;color:#0f172a;">{{.BookingID}}</td></tr>
<tr><td style="padding:6px 0;color:#64748b;font-family:Arial,sans-serif;">Waktu Pemesanan</td>
<td align="right" style="padding:6px 0;font-weight:600;font-family:Arial,sans-serif;">{{formatTime .WaktuTransaksi}}</td></tr>
<tr><td style="padding:6px 0;color:#64748b;font-family:Arial,sans-serif;">Email Notifikasi</td>
<td align="right" style="padding:6px 0;font-weight:600;font-family:Arial,sans-serif;">{{.Email}}</td></tr>
</table>

<!-- ITEM BREAKDOWN -->
<div style="background-color:#f1f5f9;border-radius:14px;padding:20px;border:1px solid #e2e8f0;">
<p style="font-size:11px;font-weight:800;text-transform:uppercase;letter-spacing:1px;color:#64748b;margin-bottom:12px;font-family:Arial,sans-serif;">Rincian Tiket ({{len .Items}} Tiket)</p>
<table role="presentation" width="100%" style="font-size:13px;border-bottom:1px solid #cbd5e1;padding-bottom:10px;margin-bottom:12px;">
{{range .Items}}
<tr>
<td style="padding:8px 0;font-family:Arial,sans-serif;">
<strong style="color:#0f172a;font-size:14px;">{{.CategoryName}} &times; 1</strong>
<div style="font-size:11px;color:#64748b;margin-top:2px;">NIK: {{.NIK}}{{if .Name}} (a.n. {{.Name}}){{end}}</div>
</td>
<td align="right" valign="top" style="padding:8px 0;font-weight:700;color:#0f172a;font-family:Arial,sans-serif;">{{formatRupiah .Price}}</td>
</tr>
{{end}}
</table>
<table role="presentation" width="100%" style="font-size:13px;color:#475569;">
<tr><td style="padding:4px 0;font-family:Arial,sans-serif;">Subtotal</td><td align="right" style="font-weight:600;font-family:Arial,sans-serif;">{{formatRupiah .TotalAmount}}</td></tr>
<tr><td style="padding:4px 0;font-family:Arial,sans-serif;">Biaya Layanan &amp; Pajak</td><td align="right" style="font-weight:600;font-family:Arial,sans-serif;">Termasuk (Rp 0)</td></tr>
<tr><td style="padding:10px 0 4px 0;font-size:15px;font-weight:800;color:#0f172a;font-family:Arial,sans-serif;">Total Pembayaran</td>
<td align="right" style="padding:10px 0 4px 0;font-size:17px;font-weight:800;color:#1d4ed8;font-family:Arial,sans-serif;">{{formatRupiah .TotalAmount}}</td></tr>
</table>
</div>

<!-- DEADLINE NOTICE -->
<div style="margin-top:20px;background-color:#fef3c7;border:1px solid #fcd34d;border-radius:10px;padding:14px;font-size:12px;color:#92400e;line-height:1.5;font-family:Arial,sans-serif;">
<strong>&#9201; Batas Waktu Pembayaran:</strong> {{.PaymentDeadline}}<br>
Booking akan otomatis dibatalkan jika pembayaran tidak diterima sebelum batas waktu.
</div>
</td></tr>

<!-- FOOTER -->
<tr><td style="background-color:#0f172a;padding:24px 32px;text-align:center;font-size:11px;color:#64748b;line-height:1.6;font-family:Arial,sans-serif;">
<p style="margin-bottom:6px;color:#94a3b8;font-weight:600;">Email ini dikirim otomatis oleh Rusuh Booking Tiket Platform</p>
<p>Bantuan: <a href="mailto:support@rusuhtiket.com" style="color:#818cf8;text-decoration:none;">support@rusuhtiket.com</a><br>
&copy; 2026 PT. Tiga Sekawan Angin Ribut. All Rights Reserved.</p>
</td></tr>
</table>
</td></tr></table>
</body></html>`

// ============================================================
// Template #2 — E-Tiket Resmi (boarding pass per NIK)
// Dikirim saat payment.succeeded.
// Design: "Email E-Tiket Resmi Konser" dari template HTML asli.
// ============================================================
const eTicketTmpl = `<!DOCTYPE html>
<html lang="id"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>E-Tiket – {{.EventName}}</title></head>
<body style="margin:0;padding:0;background-color:#f1f5f9;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#f1f5f9;padding:32px 12px;">
<tr><td align="center">
<table role="presentation" width="100%" style="max-width:620px;background-color:#ffffff;border-radius:18px;overflow:hidden;color:#1e293b;box-shadow:0 20px 40px -15px rgba(0,0,0,0.15);">

<!-- HEADER CONCERT BANNER -->
<tr><td style="padding:32px 32px 28px 32px;background:#090d16;border-bottom:3px solid #6366f1;">
<table role="presentation" width="100%"><tr><td>
<span style="background-color:#4338ca;color:#e0e7ff;font-size:10px;font-weight:800;padding:4px 9px;border-radius:6px;text-transform:uppercase;letter-spacing:1px;font-family:Arial,sans-serif;">OFFICIAL CONCERT PASS</span>
<h1 style="color:#ffffff;font-size:22px;font-weight:900;margin:10px 0 4px 0;font-family:Arial,sans-serif;">{{.EventName}}</h1>
<p style="color:#818cf8;font-size:12px;font-weight:700;margin:4px 0 0 0;font-family:Arial,sans-serif;">Booking: {{.BookingID}}</p>
</td></tr></table></td></tr>

<!-- INSTRUCTION -->
<tr><td style="padding:18px 32px 10px 32px;background-color:#f8fafc;border-bottom:1px solid #e2e8f0;font-size:13px;color:#475569;font-family:Arial,sans-serif;">
Tunjukkan <strong>QR Code tiket</strong> bersama <strong>KTP Fisik Asli</strong> saat memasuki gerbang identitas.
</td></tr>

<!-- TICKET CARDS -->
<tr><td style="padding:24px 32px 16px 32px;">
{{range .Items}}
<div style="background-color:#ffffff;border:2px solid #e2e8f0;border-radius:16px;margin-bottom:24px;overflow:hidden;box-shadow:0 4px 12px rgba(0,0,0,0.05);">
<!-- Pass Header -->
<table role="presentation" width="100%" style="background:linear-gradient(90deg,#4f46e5 0%,#6366f1 100%);color:#ffffff;padding:12px 20px;">
<tr><td><span style="font-size:11px;font-weight:800;letter-spacing:1px;text-transform:uppercase;font-family:Arial,sans-serif;">TIKET #{{.Nomor}} &bull; {{.CategoryName}}</span></td>
{{if .TicketCode}}<td align="right"><span style="font-size:11px;background-color:rgba(255,255,255,0.25);padding:3px 8px;border-radius:6px;font-weight:700;font-family:Arial,sans-serif;">{{.TicketCode}}</span></td>{{end}}
</tr></table>
<!-- Pass Body -->
<div style="padding:20px;">
<table role="presentation" width="100%"><tr>
<td valign="top" style="padding-right:16px;">
<table role="presentation" width="100%" style="font-size:12px;line-height:1.6;">
<tr><td style="color:#64748b;font-family:Arial,sans-serif;">Nama Pemegang Tiket:</td></tr>
<tr><td style="color:#0f172a;font-size:15px;font-weight:800;padding-bottom:8px;font-family:Arial,sans-serif;">{{upper .Name}}</td></tr>
<tr><td style="color:#64748b;font-family:Arial,sans-serif;">Nomor Induk Kependudukan (NIK):</td></tr>
<tr><td style="color:#1e293b;font-family:monospace;font-size:14px;font-weight:700;padding-bottom:8px;">{{.NIK}}</td></tr>
<tr><td><span style="background-color:#ecfdf5;color:#065f46;font-size:10px;font-weight:700;padding:3px 8px;border-radius:6px;border:1px solid #a7f3d0;display:inline-block;font-family:Arial,sans-serif;">&#10003; DUKCAPIL VERIFIED: AKTIF</span></td></tr>
</table></td>
<!-- QR code dari BookingID — di-generate server-side sebelum render template -->
<td width="120" align="center" valign="middle" style="background-color:#0f172a;border-radius:12px;padding:12px;">
<div style="width:96px;height:96px;background-color:#ffffff;border-radius:8px;padding:4px;margin:0 auto;text-align:center;">
{{if .QRCodeBase64}}
<img src="{{.QRCodeBase64}}" width="88" height="88" alt="QR Code Tiket" style="display:block;border-radius:4px;" />
{{else}}
<svg width="84" height="84" viewBox="0 0 24 24" fill="#0f172a" xmlns="http://www.w3.org/2000/svg">
<path d="M2 2h8v8H2V2zm2 2v4h4V4H4zm10-2h8v8h-8V2zm2 2v4h4V4h-4zM2 14h8v8H2v-8zm2 2v4h4v-4H4zm14 0h4v4h-4v-4zm-4-2h2v2h-2v-2zm4 4h2v4h-2v-4zm-4 2h2v2h-2v-2zm-2-4h2v2h-2v-2zm4-2h4v2h-4v-2zM6 6h2v2H6V6zm12 0h2v2h-2V6zM6 18h2v2H6v-2z"/>
</svg>
{{end}}
</div>
{{if .TicketCode}}<div style="color:#94a3b8;font-family:monospace;font-size:9px;font-weight:700;margin-top:6px;">{{.TicketCode}}</div>{{end}}
</td></tr></table></div>
</div>
{{end}}
</td></tr>

<!-- TOTAL -->
<tr><td style="padding:0 32px 20px 32px;">
<div style="background-color:#f1f5f9;border-radius:12px;padding:16px;border:1px solid #e2e8f0;font-family:Arial,sans-serif;">
<table role="presentation" width="100%" style="font-size:13px;color:#475569;">
<tr><td>Total yang dibayarkan</td><td align="right" style="font-size:16px;font-weight:800;color:#059669;">{{formatRupiah .TotalAmount}}</td></tr>
</table></div></td></tr>

<!-- RULES -->
<tr><td style="padding:0 32px 28px 32px;">
<div style="background-color:#f8fafc;border-radius:12px;padding:16px;border:1px solid #e2e8f0;font-size:12px;color:#475569;line-height:1.6;font-family:Arial,sans-serif;">
<p style="font-weight:800;color:#0f172a;margin-bottom:6px;">Aturan Masuk &amp; Validasi Kependudukan:</p>
<ul style="padding-left:18px;margin:0;">
<li>1 QR Code hanya dapat di-scan 1 kali di gerbang turnstile.</li>
<li>Petugas akan memverifikasi kesesuaian <strong>Nama &amp; NIK</strong> dengan e-KTP fisik asli.</li>
<li>Pemalsuan data identitas ditolak tanpa pengembalian dana.</li>
</ul></div></td></tr>

<!-- FOOTER -->
<tr><td style="background-color:#0f172a;padding:24px 32px;text-align:center;font-size:11px;color:#64748b;line-height:1.6;font-family:Arial,sans-serif;">
<p style="margin-bottom:4px;color:#94a3b8;font-weight:700;">Tiket Resmi Terbitan PT. Tiga Sekawan Angin Ribut</p>
<p>&copy; 2026 PT. Tiga Sekawan Angin Ribut. All Rights Reserved.</p>
</td></tr>
</table>
</td></tr></table>
</body></html>`

// ============================================================
// Template #3 — Booking Dibatalkan (payment expired)
// ============================================================
const paymentExpiredTmpl = `<!DOCTYPE html>
<html lang="id"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Booking Dibatalkan – Rusuh Booking Tiket</title></head>
<body style="margin:0;padding:0;background-color:#f1f5f9;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#f1f5f9;padding:32px 12px;">
<tr><td align="center">
<table role="presentation" width="100%" style="max-width:620px;background-color:#ffffff;border-radius:18px;overflow:hidden;color:#1e293b;box-shadow:0 20px 40px -15px rgba(0,0,0,0.15);">

<!-- HEADER -->
<tr><td style="padding:28px 32px;background:linear-gradient(135deg,#0f172a 0%,#1e1b4b 100%);">
<table role="presentation" width="100%"><tr>
<td><h1 style="color:#ffffff;font-size:20px;font-weight:800;margin:0;font-family:Arial,sans-serif;">RUSUH BOOKING TIKET</h1>
<p style="color:#818cf8;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:1.5px;margin:4px 0 0 0;font-family:Arial,sans-serif;">PT. Tiga Sekawan Angin Ribut</p></td>
<td align="right" valign="middle">
<span style="background-color:#7f1d1d;color:#fca5a5;font-size:11px;font-weight:700;padding:6px 12px;border-radius:9999px;border:1px solid #ef4444;display:inline-block;font-family:Arial,sans-serif;">DIBATALKAN</span>
</td></tr></table></td></tr>

<!-- HERO -->
<tr><td style="padding:36px 32px 24px 32px;text-align:center;background-color:#fef2f2;border-bottom:1px solid #fecaca;">
<div style="width:58px;height:58px;background-color:#fee2e2;border-radius:50%;border:2px solid #fca5a5;margin:0 auto 16px auto;line-height:58px;font-size:28px;text-align:center;">&#10060;</div>
<h2 style="color:#0f172a;font-size:22px;font-weight:800;margin-bottom:8px;font-family:Arial,sans-serif;">Booking Dibatalkan</h2>
{{if .EventName}}<p style="color:#ef4444;font-size:15px;font-weight:700;margin-bottom:8px;font-family:Arial,sans-serif;">{{.EventName}}</p>{{end}}
<p style="color:#64748b;font-size:14px;line-height:1.5;max-width:480px;margin:0 auto;font-family:Arial,sans-serif;">
Booking <strong style="color:#0f172a;">{{.BookingID}}</strong> telah dibatalkan karena batas waktu pembayaran terlampaui. Tiket yang direservasi telah dikembalikan ke pool.
</p></td></tr>

<!-- BODY -->
<tr><td style="padding:28px 32px;">
<div style="background-color:#f8fafc;border-radius:12px;padding:16px;border:1px solid #e2e8f0;font-size:13px;color:#475569;line-height:1.7;font-family:Arial,sans-serif;">
<p style="font-weight:700;color:#0f172a;margin-bottom:8px;">Apa yang terjadi?</p>
<ul style="padding-left:18px;margin:0;">
<li>Pembayaran tidak diterima sebelum batas waktu yang ditentukan.</li>
<li>Reservasi tiket telah dilepas dan kembali tersedia untuk pembeli lain.</li>
<li>Tidak ada tagihan yang dibebankan ke akun Anda.</li>
</ul>
</div>
<div style="margin-top:20px;text-align:center;font-size:13px;color:#64748b;font-family:Arial,sans-serif;">
Ingin mencoba lagi? Kunjungi halaman event dan mulai antrian dari awal.
</div>
</td></tr>

<!-- FOOTER -->
<tr><td style="background-color:#0f172a;padding:24px 32px;text-align:center;font-size:11px;color:#64748b;line-height:1.6;font-family:Arial,sans-serif;">
<p style="margin-bottom:6px;color:#94a3b8;font-weight:600;">Email ini dikirim otomatis oleh Rusuh Booking Tiket Platform</p>
<p>Bantuan: <a href="mailto:support@rusuhtiket.com" style="color:#818cf8;text-decoration:none;">support@rusuhtiket.com</a><br>
&copy; 2026 PT. Tiga Sekawan Angin Ribut. All Rights Reserved.</p>
</td></tr>
</table>
</td></tr></table>
</body></html>`
