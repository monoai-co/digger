package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/diggerhq/digger/backend/ci_backends"
	"github.com/diggerhq/digger/backend/logging"
	"github.com/diggerhq/digger/backend/middleware"
	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/utils"
	"github.com/gin-gonic/gin"
	"github.com/google/go-github/v61/github"
	"gorm.io/gorm"
)

type IssueCommentHook func(gh utils.GithubClientProvider, payload *github.IssueCommentEvent, ciBackendProvider ci_backends.CiBackendProvider) error

const (
	maxGithubWebhookBodyBytes      = 10 * 1024 * 1024
	maxGithubDeliveryHeaderBytes   = 128
	maxGithubEventHeaderBytes      = 128
	maxGithubSignatureHeaderBytes  = 256
	maxGithubNumericHeaderBytes    = 32
	maxGithubTargetTypeHeaderBytes = 64
)

type DiggerController struct {
	CiBackendProvider                  ci_backends.CiBackendProvider
	GithubClientProvider               utils.GithubClientProvider
	GithubWebhookPostIssueCommentHooks []IssueCommentHook
	GithubWebhookProcessor             githubWebhookAdmitter
	ControlPlaneDatabaseIdentity       string
	ControlPlaneWriterEpoch            int64
	ExecutionGrantSecrets              map[string][]byte
	ExecutionGrantSigningKeyID         string
	ExecutionIdentityVerifier          ExecutionIdentityVerifier
	// Compatibility gates; OIDC attests the protected workflow run, not an action or binary.
	TrustedActionRef string
	TrustedCLISHA256 string
	OutboxDispatcher interface{ Wake() }
}

func (d DiggerController) GithubAppWebHook(c *gin.Context) {
	if d.GithubWebhookProcessor == nil || !d.GithubWebhookProcessor.Enabled() {
		d.githubAppWebHookLegacy(c)
		return
	}

	c.Header("Content-Type", "application/json")
	slog.Info("Processing GitHub app webhook")

	deliveryID := c.GetHeader("X-GitHub-Delivery")
	if deliveryID == "" {
		slog.Warn("Rejecting GitHub app webhook without a delivery ID")
		c.JSON(http.StatusBadRequest, gin.H{"error": "X-GitHub-Delivery header is required"})
		return
	}
	if len(deliveryID) > maxGithubDeliveryHeaderBytes {
		c.JSON(http.StatusRequestHeaderFieldsTooLarge, gin.H{"error": "X-GitHub-Delivery header is too large"})
		return
	}

	webhookType := github.WebHookType(c.Request)
	if webhookType == "" {
		slog.Warn("Rejecting GitHub app webhook without an event type", "deliveryID", deliveryID)
		c.JSON(http.StatusBadRequest, gin.H{"error": "X-GitHub-Event header is required"})
		return
	}
	if len(webhookType) > maxGithubEventHeaderBytes {
		c.JSON(http.StatusRequestHeaderFieldsTooLarge, gin.H{"error": "X-GitHub-Event header is too large"})
		return
	}

	signature := c.GetHeader("X-Hub-Signature-256")
	if signature == "" {
		slog.Warn("Rejecting unsigned GitHub app webhook", "deliveryID", deliveryID)
		c.JSON(http.StatusBadRequest, gin.H{"error": "X-Hub-Signature-256 header is required"})
		return
	}
	if len(signature) > maxGithubSignatureHeaderBytes {
		c.JSON(http.StatusRequestHeaderFieldsTooLarge, gin.H{"error": "X-Hub-Signature-256 header is too large"})
		return
	}

	appID := c.GetHeader("X-GitHub-Hook-Installation-Target-ID")
	if len(appID) > maxGithubNumericHeaderBytes || len(c.GetHeader("X-GitHub-Hook-ID")) > maxGithubNumericHeaderBytes || len(c.GetHeader("X-GitHub-Hook-Installation-Target-Type")) > maxGithubTargetTypeHeaderBytes {
		c.JSON(http.StatusRequestHeaderFieldsTooLarge, gin.H{"error": "GitHub webhook identity header is too large"})
		return
	}
	appID64, err := strconv.ParseInt(appID, 10, 64)
	if err != nil || appID64 <= 0 {
		slog.Warn("Rejecting GitHub app webhook with an invalid app ID", "deliveryID", deliveryID, "appID", appID, "error", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "X-GitHub-Hook-Installation-Target-ID header is invalid"})
		return
	}

	_, _, webhookSecret, _, err := d.GithubClientProvider.FetchCredentials(appID)
	if err != nil {
		slog.Error("Failed to load GitHub app webhook credentials", "deliveryID", deliveryID, "appID", appID, "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "GitHub app webhook credentials are unavailable"})
		return
	}
	if webhookSecret == "" {
		slog.Error("GitHub app webhook secret is empty", "deliveryID", deliveryID, "appID", appID)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "GitHub app webhook credentials are unavailable"})
		return
	}

	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxGithubWebhookBodyBytes)
	payload, err := github.ValidatePayload(c.Request, []byte(webhookSecret))
	if err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "GitHub app webhook payload is too large"})
			return
		}
		slog.Warn("Error validating GitHub app webhook payload", "deliveryID", deliveryID, "appID", appID, "error", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Error validating GitHub app webhook payload"})
		return
	}

	slog.Info("Received GitHub event",
		"deliveryID", deliveryID,
		"webhookType", webhookType,
	)

	installationID, repositoryFullName, err := githubWebhookPayloadIdentity(payload)
	if err != nil {
		slog.Warn("Failed to read GitHub webhook payload identity", "deliveryID", deliveryID, "error", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to read GitHub webhook payload identity"})
		return
	}

	payloadDigest := sha256.Sum256(payload)
	now := time.Now().UTC()
	delivery := &models.GithubWebhookDelivery{
		DeliveryID:                 deliveryID,
		PayloadSHA256:              hex.EncodeToString(payloadDigest[:]),
		Payload:                    payload,
		EventType:                  webhookType,
		GithubAppID:                appID64,
		HookID:                     c.GetHeader("X-GitHub-Hook-ID"),
		HookInstallationTargetType: c.GetHeader("X-GitHub-Hook-Installation-Target-Type"),
		InstallationID:             installationID,
		RepositoryFullName:         repositoryFullName,
		ReceivedAt:                 now,
		ProcessingStatus:           models.GithubWebhookDeliveryPending,
		UpdatedAt:                  now,
	}
	receipt, created, err := d.GithubWebhookProcessor.Admit(c.Request.Context(), delivery)
	if errors.Is(err, models.ErrGithubWebhookDeliveryConflict) {
		slog.Error("Rejected conflicting GitHub webhook delivery", "deliveryID", deliveryID, "payloadSHA256", delivery.PayloadSHA256)
		c.JSON(http.StatusConflict, gin.H{"error": "delivery ID conflicts with the immutable stored request"})
		return
	}
	if err != nil {
		slog.Error("Failed to commit GitHub webhook delivery receipt", "deliveryID", deliveryID, "error", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "GitHub webhook delivery was not durably accepted"})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"delivery_id": receipt.DeliveryID,
		"duplicate":   !created,
		"status":      receipt.ProcessingStatus,
	})
}

// githubAppWebHookLegacy preserves the existing request and execution behavior
// until the durable protocol is explicitly enabled for a configured writer.
func (d DiggerController) githubAppWebHookLegacy(c *gin.Context) {
	c.Header("Content-Type", "application/json")
	gh := d.GithubClientProvider
	slog.Info("Processing GitHub app webhook")

	appID := c.GetHeader("X-GitHub-Hook-Installation-Target-ID")
	_, _, webhookSecret, _, _ := d.GithubClientProvider.FetchCredentials(appID)
	payload, err := github.ValidatePayload(c.Request, []byte(webhookSecret))
	if err != nil {
		slog.Error("Error validating GitHub app webhook's payload", "appID", appID, "error", err)
		c.String(http.StatusBadRequest, "Error validating github app webhook's payload")
		return
	}

	webhookType := github.WebHookType(c.Request)
	event, err := github.ParseWebHook(webhookType, payload)
	if err != nil {
		slog.Error("Failed to parse GitHub event", "webhookType", webhookType, "error", err)
		c.String(http.StatusInternalServerError, "Failed to parse Github Event")
		return
	}
	slog.Info("Received GitHub event", "eventType", reflect.TypeOf(event), "webhookType", webhookType)

	appID64, err := strconv.ParseInt(appID, 10, 64)
	if err != nil {
		slog.Error("Error converting appId string to int64", "appID", appID, "error", err)
		return
	}

	switch event := event.(type) {
	case *github.InstallationEvent:
		go func(ctx context.Context) {
			defer logging.InheritRequestLogger(ctx)()
			if event.GetAction() == "deleted" {
				if err := handleInstallationDeletedEvent(event, appID64); err != nil {
					slog.Error("Failed to handle installation deleted event", "error", err)
				}
			} else if event.GetAction() == "created" || event.GetAction() == "unsuspended" || event.GetAction() == "new_permissions_accepted" {
				if err := handleInstallationUpsertEvent(context.Background(), gh, event, appID64); err != nil {
					slog.Error("Failed to handle installation upsert event", "error", err)
				}
			}
		}(c.Request.Context())
	case *github.InstallationRepositoriesEvent:
		go func(ctx context.Context) {
			defer logging.InheritRequestLogger(ctx)()
			if err := handleInstallationRepositoriesEvent(context.Background(), gh, event, appID64); err != nil {
				slog.Error("Failed to handle installation repositories event", "error", err)
			}
		}(c.Request.Context())
	case *github.PushEvent:
		go func(ctx context.Context) {
			defer logging.InheritRequestLogger(ctx)()
			_ = handlePushEvent(ctx, gh, event, appID64)
		}(c.Request.Context())
	case *github.IssueCommentEvent:
		go func(ctx context.Context) {
			defer logging.InheritRequestLogger(ctx)()
			_ = handleIssueCommentEvent(gh, event, d.CiBackendProvider, appID64, d.GithubWebhookPostIssueCommentHooks)
		}(c.Request.Context())
	case *github.PullRequestEvent:
		go func(ctx context.Context) {
			defer logging.InheritRequestLogger(ctx)()
			_ = handlePullRequestEvent(gh, event, d.CiBackendProvider, appID64)
		}(c.Request.Context())
	case *github.CheckRunEvent:
		if event.GetAction() != "requested_action" || event.GetRequestedAction() == nil {
			return
		}
		identifier := event.GetRequestedAction().Identifier
		go func(ctx context.Context) {
			defer logging.InheritRequestLogger(ctx)()
			_ = handleCheckRunActionEvent(gh, identifier, event, d.CiBackendProvider, appID64)
		}(c.Request.Context())
	}

	c.JSON(http.StatusAccepted, "ok")
}

func (d DiggerController) RequeueGithubWebhookDelivery(c *gin.Context) {
	if d.GithubWebhookProcessor == nil || !d.GithubWebhookProcessor.Enabled() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "GitHub webhook processing is unavailable"})
		return
	}
	deliveryID := c.Param("deliveryID")
	if deliveryID == "" || len(deliveryID) > maxGithubDeliveryHeaderBytes {
		c.JSON(http.StatusBadRequest, gin.H{"error": "delivery ID is invalid"})
		return
	}
	userID, exists := c.Get(middleware.USER_ID_KEY)
	actor := ""
	if exists && strings.TrimSpace(fmt.Sprint(userID)) != "" {
		actor = "user:" + fmt.Sprint(userID)
	} else if organisationID, organisationExists := c.Get(middleware.ORGANISATION_ID_KEY); organisationExists && strings.TrimSpace(fmt.Sprint(organisationID)) != "" {
		actor = "organisation:" + fmt.Sprint(organisationID)
	} else {
		c.JSON(http.StatusForbidden, gin.H{"error": "operator identity is required"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4*1024)
	var request struct {
		Reason string `json:"reason" binding:"required"`
	}
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "a requeue reason is required"})
		return
	}
	if err := d.GithubWebhookProcessor.RequeueDeadLetter(c.Request.Context(), deliveryID, actor, request.Reason); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "dead-lettered delivery was not found"})
			return
		}
		slog.Error("Failed to requeue GitHub webhook delivery", "deliveryID", deliveryID, "error", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "GitHub webhook delivery could not be requeued"})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"delivery_id": deliveryID, "status": "requeued"})
}

func githubWebhookPayloadIdentity(payload []byte) (*int64, string, error) {
	var identity struct {
		Installation *struct {
			ID int64 `json:"id"`
		} `json:"installation"`
		Repository *struct {
			FullName string `json:"full_name"`
		} `json:"repository"`
	}
	if err := json.Unmarshal(payload, &identity); err != nil {
		return nil, "", err
	}

	var installationID *int64
	if identity.Installation != nil && identity.Installation.ID != 0 {
		id := identity.Installation.ID
		installationID = &id
	}
	repositoryFullName := ""
	if identity.Repository != nil {
		repositoryFullName = identity.Repository.FullName
	}
	return installationID, repositoryFullName, nil
}

func (d DiggerController) ProcessGithubWebhookDelivery(ctx context.Context, delivery *models.GithubWebhookDelivery) (GithubWebhookProcessingResult, error) {
	defer logging.InheritRequestLogger(ctx)()
	if github.EventForType(delivery.EventType) == nil {
		slog.Info("Ignoring unsupported GitHub event type", "eventType", delivery.EventType)
		return ignoredGithubWebhookResult("event_type_ignored"), nil
	}
	event, err := github.ParseWebHook(delivery.EventType, delivery.Payload)
	if err != nil {
		return GithubWebhookProcessingResult{}, fmt.Errorf("parse stored GitHub event: %w", err)
	}

	gh := d.GithubClientProvider
	appID := delivery.GithubAppID
	switch event := event.(type) {
	case *github.WorkflowRunEvent:
		err := models.DB.WakeDurableRunReconciliation(ctx, event.GetRepo().GetID(), event.GetWorkflowRun().GetID(), d.ControlPlaneDatabaseIdentity, d.ControlPlaneWriterEpoch)
		if err == nil && d.OutboxDispatcher != nil {
			d.OutboxDispatcher.Wake()
		}
		return completedGithubWebhookResult("workflow_reconciliation_woken", err)
	case *github.InstallationEvent:
		slog.Info("Processing InstallationEvent", "action", event.GetAction(), "installationId", event.GetInstallation().GetID())
		switch event.GetAction() {
		case "deleted":
			return completedGithubWebhookResult("installation_deleted", handleInstallationDeletedEventDurable(ctx, event, appID))
		case "created", "unsuspended", "new_permissions_accepted":
			return completedGithubWebhookResult("installation_upserted", handleInstallationUpsertEvent(ctx, gh, event, appID))
		default:
			return ignoredGithubWebhookResult("installation_action_ignored"), nil
		}
	case *github.InstallationRepositoriesEvent:
		slog.Info("Processing InstallationRepositoriesEvent", "action", event.GetAction(), "installationId", event.GetInstallation().GetID(), "added", len(event.RepositoriesAdded), "removed", len(event.RepositoriesRemoved))
		return completedGithubWebhookResult("installation_repositories_updated", handleInstallationRepositoriesEvent(ctx, gh, event, appID))
	case *github.PushEvent:
		slog.Info("Processing PushEvent", "repo", event.GetRepo().GetFullName())
		return completedGithubWebhookResult("push_processed", handlePushEventDurable(ctx, gh, event, appID))
	case *github.IssueCommentEvent:
		slog.Info("Processing IssueCommentEvent", "action", event.GetAction(), "repo", event.GetRepo().GetFullName(), "issueNumber", event.GetIssue().GetNumber())
		return completedGithubWebhookResult("issue_comment_processed", handleIssueCommentEventDurable(gh, event, d.CiBackendProvider, appID, d.GithubWebhookPostIssueCommentHooks))
	case *github.PullRequestEvent:
		slog.Info("Processing PullRequestEvent", "action", event.GetAction(), "repo", event.GetRepo().GetFullName(), "prNumber", event.GetPullRequest().GetNumber(), "prId", event.GetPullRequest().GetID())
		return completedGithubWebhookResult("pull_request_processed", handlePullRequestEventDurable(gh, event, d.CiBackendProvider, appID))
	case *github.CheckRunEvent:
		slog.Info("Processing CheckRunEvent", "action", event.GetAction(), "checkRunID", event.GetCheckRun().GetID())
		if event.GetAction() != "requested_action" || event.GetRequestedAction() == nil {
			return ignoredGithubWebhookResult("check_run_action_ignored"), nil
		}
		identifier := event.GetRequestedAction().Identifier
		return completedGithubWebhookResult("check_run_action_processed", handleCheckRunActionEventDurable(ctx, gh, identifier, event, d.CiBackendProvider, appID))
	default:
		slog.Debug("Ignoring unsupported GitHub event", "eventType", reflect.TypeOf(event))
		return ignoredGithubWebhookResult("event_type_ignored"), nil
	}
}

func completedGithubWebhookResult(result string, err error) (GithubWebhookProcessingResult, error) {
	if err != nil {
		return GithubWebhookProcessingResult{}, err
	}
	return succeededGithubWebhookResult(result), nil
}

func succeededGithubWebhookResult(result string) GithubWebhookProcessingResult {
	return GithubWebhookProcessingResult{Status: models.GithubWebhookDeliverySucceeded, TerminalResult: result}
}

func ignoredGithubWebhookResult(result string) GithubWebhookProcessingResult {
	return GithubWebhookProcessingResult{Status: models.GithubWebhookDeliveryIgnored, TerminalResult: result}
}

func (d DiggerController) GithubReposPage(c *gin.Context) {
	orgId, exists := c.Get(middleware.ORGANISATION_ID_KEY)
	if !exists {
		slog.Warn("Organisation ID not found in context")
		c.String(http.StatusForbidden, "Not allowed to access this resource")
		return
	}

	slog.Info("Fetching GitHub repositories for organisation", "orgId", orgId)

	link, err := models.DB.GetGithubInstallationLinkForOrg(orgId)
	if err != nil {
		slog.Error("Failed to get GitHub installation link for organisation",
			"orgId", orgId,
			"error", err,
		)
		c.String(http.StatusForbidden, "Failed to find any GitHub installations for this org")
		return
	}

	slog.Debug("Found GitHub installation link",
		"orgId", orgId,
		"installationId", link.GithubInstallationId,
	)

	installations, err := models.DB.GetGithubAppInstallations(link.GithubInstallationId)
	if err != nil {
		slog.Error("Failed to get GitHub app installations",
			"installationId", link.GithubInstallationId,
			"error", err,
		)
		c.String(http.StatusForbidden, "Failed to find any GitHub installations for this org")
		return
	}

	if len(installations) == 0 {
		slog.Warn("No GitHub installations found",
			"installationId", link.GithubInstallationId,
			"orgId", orgId,
		)
		c.String(http.StatusForbidden, "Failed to find any GitHub installations for this org")
		return
	}

	slog.Debug("Found GitHub installations",
		"count", len(installations),
		"appId", installations[0].GithubAppId,
		"installationId", installations[0].GithubInstallationId,
	)

	gh := d.GithubClientProvider
	client, _, err := gh.Get(installations[0].GithubAppId, installations[0].GithubInstallationId)
	if err != nil {
		slog.Error("Failed to create GitHub client",
			"appId", installations[0].GithubAppId,
			"installationId", installations[0].GithubInstallationId,
			"error", err,
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Error creating GitHub client"})
		return
	}

	slog.Debug("Successfully created GitHub client",
		"appId", installations[0].GithubAppId,
		"installationId", installations[0].GithubInstallationId,
	)

	opts := &github.ListOptions{}
	repos, _, err := client.Apps.ListRepos(context.Background(), opts)
	if err != nil {
		slog.Error("Failed to list GitHub repositories",
			"installationId", installations[0].GithubInstallationId,
			"error", err,
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list GitHub repos."})
		return
	}

	slog.Info("Successfully retrieved GitHub repositories",
		"orgId", orgId,
		"repoCount", len(repos.Repositories),
	)

	c.HTML(http.StatusOK, "github_repos.tmpl", gin.H{"Repos": repos.Repositories})
}
