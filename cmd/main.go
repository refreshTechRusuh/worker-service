package main

import (
	"context"
	"log/slog"
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

	slog.Info("worker-service starting", "env", cfg.App.Env)
	worker.Start(ctx)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	slog.Info("worker-service shutting down...")
	cancel()
	worker.Close()
	slog.Info("worker-service stopped")
}
