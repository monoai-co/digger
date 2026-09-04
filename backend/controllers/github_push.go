package controllers

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/diggerhq/digger/backend/logging"
	"github.com/diggerhq/digger/backend/services"
	"github.com/diggerhq/digger/backend/utils"
	"github.com/google/go-github/v61/github"
)

func handlePushEvent(ctx context.Context, gh utils.GithubClientProvider, payload *github.PushEvent, appId int64) error {
	return handlePushEventMode(ctx, gh, payload, appId, false)
}

func handlePushEventDurable(ctx context.Context, gh utils.GithubClientProvider, payload *github.PushEvent, appId int64) error {
	return handlePushEventMode(ctx, gh, payload, appId, true)
}

func handlePushEventMode(ctx context.Context, gh utils.GithubClientProvider, payload *github.PushEvent, appId int64, durable bool) (err error) {
	slog.Debug("Handling push event", "appId", appId, "payload", payload)
	defer func() {
		if r := recover(); r != nil {
			panicErr := fmt.Errorf("panic in handlePushEvent: %v\n%s", r, debug.Stack())
			slog.Error("Recovered from panic in handlePushEvent", "error", panicErr)
			if durable {
				err = panicErr
			}
		}
	}()

	installationId := *payload.Installation.ID
	repoName := *payload.Repo.Name
	repoOwner := *payload.Repo.Owner.Login
	repoFullName := *payload.Repo.FullName
	cloneURL := *payload.Repo.CloneURL
	ref := *payload.Ref
	defaultBranch := *payload.Repo.DefaultBranch

	loadProjectsOnPush := os.Getenv("DIGGER_LOAD_PROJECTS_ON_PUSH")

	if loadProjectsOnPush == "true" {
		if strings.HasSuffix(ref, defaultBranch) {
			slog.Debug("Loading projects from GitHub repo (push event)", "loadProjectsOnPush", loadProjectsOnPush, "ref", ref, "defaultBranch", defaultBranch)
			err := services.LoadProjectsFromGithubRepo(gh, strconv.FormatInt(installationId, 10), repoFullName, repoOwner, repoName, cloneURL, defaultBranch)
			if err != nil {
				if durable {
					return fmt.Errorf("load projects from GitHub repo: %w", err)
				}
				slog.Error("Failed to load projects from GitHub repo", "error", err)
			}
		}
	} else {
		slog.Debug("Skipping loading projects from GitHub repo", "loadProjectsOnPush", loadProjectsOnPush)
	}

	repoCacheEnabled := os.Getenv("DIGGER_CONFIG_REPO_CACHE_ENABLED")
	if repoCacheEnabled == "1" && strings.HasSuffix(ref, defaultBranch) {
		if durable {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := sendProcessCacheRequest(repoFullName, defaultBranch, installationId); err != nil {
				return fmt.Errorf("process repository cache request: %w", err)
			}
		} else {
			go func(ctx context.Context) {
				defer logging.InheritRequestLogger(ctx)()
				if err := sendProcessCacheRequest(repoFullName, defaultBranch, installationId); err != nil {
					slog.Error("Failed to process cache request", "error", err, "repoFullName", repoFullName)
				}
			}(ctx)
		}
	}

	return nil
}
