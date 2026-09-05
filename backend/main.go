package main

import (
	"context"
	"embed"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/diggerhq/digger/backend/bootstrap"
	"github.com/diggerhq/digger/backend/ci_backends"
	"github.com/diggerhq/digger/backend/config"
	"github.com/diggerhq/digger/backend/controllers"
	"github.com/diggerhq/digger/backend/utils"
)

//go:embed templates
var templates embed.FS

func main() {
	ghController := controllers.DiggerController{
		CiBackendProvider:                  ci_backends.DefaultBackendProvider{},
		GithubClientProvider:               utils.DiggerGithubRealClientProvider{},
		GithubWebhookPostIssueCommentHooks: make([]controllers.IssueCommentHook, 0),
	}
	r, controlPlane, err := bootstrap.Bootstrap(templates, ghController)
	if err != nil {
		slog.Error("Backend startup failed", "error", err)
		os.Exit(1)
	}
	r.GET("/", controllers.Home)
	port := config.GetPort()
	processCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := bootstrap.RunServer(processCtx, r, port, controlPlane, 50*time.Second); err != nil {
		slog.Error("Backend server stopped with an error", "error", err)
		os.Exit(1)
	}
}
