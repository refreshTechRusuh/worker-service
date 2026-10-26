package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/refreshTechRusuh/worker-service/internal/config"
	"github.com/refreshTechRusuh/worker-service/internal/worker/consumer"
	"github.com/refreshTechRusuh/worker-service/internal/worker/handler"
	"github.com/refreshTechRusuh/worker-service/pkg/mailer"
)

func main() {
	cfg, _ := config.Load()
	config.SetupLogger(cfg.Log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := mailer.New(cfg.Email, slog.Default())
	notifHandler := handler.NewNotificationHandler(m, slog.Default())
	worker := consumer.New(cfg.Kafka, notifHandler, slog.Default())

	// Health server -- Cloud Run butuh HTTP endpoint untuk startup/liveness probe
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok")) //nolint:errcheck
	})
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.App.Port),
		Handler: mux,
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
	srv.Close() //nolint:errcheck
	slog.Info("worker-service stopped")
}
