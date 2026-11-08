package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/refreshTechRusuh/worker-service/internal/config"
	"github.com/refreshTechRusuh/worker-service/internal/worker/consumer"
	"github.com/refreshTechRusuh/worker-service/internal/worker/handler"
	"github.com/refreshTechRusuh/worker-service/pkg/mailer"
	pkgotel "github.com/refreshTechRusuh/worker-service/pkg/otel"
)

func main() {
	cfg, _ := config.Load()
	config.SetupLogger(cfg.Log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup OpenTelemetry TracerProvider.
	otelShutdown, err := pkgotel.SetupTracerProvider(ctx, "worker-service", "1.0.0")
	if err != nil {
		slog.Warn("otel: failed to setup tracer provider, tracing disabled", "err", err)
	} else {
		defer func() {
			shutCtx, cancelOtel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelOtel()
			_ = otelShutdown(shutCtx)
		}()
	}

	m := mailer.New(cfg.Email, slog.Default())
	notifHandler := handler.NewNotificationHandler(m, slog.Default())
	worker := consumer.New(cfg.Kafka, notifHandler, slog.Default())

	// Health server -- Cloud Run butuh HTTP endpoint untuk startup/liveness probe
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok")) // #nosec G104 //nolint:errcheck
	})
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.App.Port),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second, // prevent Slowloris
	}
	go func() {
		slog.Info("worker-service health server listening", "port", cfg.App.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("health server error", "err", err)
		}
	}()

	slog.Info("worker-service starting", "env", cfg.App.Env)
	worker.Start(ctx)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	slog.Info("worker-service shutting down...")
	cancel()
	worker.Close()
	srv.Close() // #nosec G104 //nolint:errcheck
	slog.Info("worker-service stopped")
}
