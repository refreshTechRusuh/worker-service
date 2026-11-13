# worker-service

Worker asinkron untuk notifikasi Rusuh Booking Tiket (**BE-2**). Worker mengonsumsi event booking dan pembayaran dari Kafka, lalu mengirim email instruksi pembayaran, e-tiket, atau booking kedaluwarsa melalui SMTP.

- Arsitektur: Kafka consumer + handler notifikasi + SMTP mailer
- Port lokal: **8080** (health check HTTP; bukan API bisnis)
- Consumer group default: `worker-service`

## Prasyarat

- Go 1.26+
- Kafka dari repo `tools`
- Kredensial SMTP yang dapat mengirim email

## Menjalankan

Jalankan infrastruktur Kafka lokal:

```bash
cd ../tools
make up
```

Lalu jalankan worker dengan credential SMTP-mu:

```bash
cd ../worker-service
APP_ENV=local APP_PORT=8080 KAFKA_BROKERS=localhost:29092 \
EMAIL_HOST=smtp.gmail.com EMAIL_PORT=587 \
EMAIL_USERNAME='<alamat-email>' EMAIL_PASSWORD='<app-password>' \
EMAIL_FROM='Rusuh Booking Tiket <noreply@example.com>' \
OTEL_ENABLED=false make run
```

`EMAIL_USERNAME` dan `EMAIL_PASSWORD` wajib. Simpan credential melalui environment/secret manager, bukan di README atau repository. Kafka pada Compose lokal dapat diakses di `localhost:29092`.

## Health check

```bash
curl -i http://localhost:8080/healthz
```

Respons sehat adalah HTTP `200` dengan body `ok`. Endpoint ini hanya untuk probe Cloud Run/orchestrator.

## Event Kafka

Worker memproses topic utama berikut:

| Topic | Efek |
|---|---|
| `booking.created` | Email instruksi pembayaran dengan nominal dan tenggat pembayaran |
| `payment.succeeded` | Email e-tiket setelah pembayaran berhasil |
| `payment.expired` | Email pemberitahuan booking dibatalkan/kedaluwarsa |

Setiap topic memiliki varian `.retry` untuk retry bertahap dan `.dlt` untuk pesan yang gagal setelah batas retry. Pemrosesan email memakai idempotensi in-memory per booking dan jenis email; state tersebut hilang saat worker restart.

## Konfigurasi

| Env | Default lokal | Keterangan |
|---|---|---|
| `APP_ENV` / `APP_PORT` | `local` / `8080` | Port health check; Cloud Run dapat mengisi `PORT` |
| `LOG_LEVEL` / `LOG_FORMAT` | `info` / `json` | Logging terstruktur |
| `KAFKA_BROKERS` | `localhost:9092` | Compose lokal: `localhost:29092` |
| `KAFKA_USERNAME` / `KAFKA_PASSWORD` | kosong | Kredensial broker bila diperlukan |
| `KAFKA_TLS` | `false` | Aktifkan TLS untuk broker |
| `KAFKA_CONSUMER_GROUP` | `worker-service` | Nama consumer group |
| `EMAIL_HOST` | `smtp.gmail.com` | SMTP server |
| `EMAIL_PORT` | `587` | Port SMTP dengan STARTTLS |
| `EMAIL_USERNAME` / `EMAIL_PASSWORD` | wajib diisi | Credential SMTP |
| `EMAIL_FROM` | `Rusuh Booking Tiket <noreply@gmail.com>` | Nama dan alamat pengirim |
| `OTEL_ENABLED` | aktif | Set `false` untuk menonaktifkan ekspor Cloud Trace saat lokal |
| `OTEL_SAMPLE_RATIO` | `1.0` | Rasio trace yang disimpan, antara `0` dan `1` |

## Pengembangan

```bash
make test
make build
make scan       # gosec
make vuln       # govulncheck
```
