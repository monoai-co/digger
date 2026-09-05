package controllers

import (
	"context"
	"errors"
	"time"

	"github.com/diggerhq/digger/backend/models"
	"github.com/diggerhq/digger/backend/utils"
	"github.com/diggerhq/digger/libs/operation"
	"github.com/google/go-github/v61/github"
)

var errGithubSubmissionReportsPending = errors.New("github submission reports are awaiting creation receipts")

type githubCheckSubmissionPreparation struct {
	identity models.JobCreationIdentity
	target   models.GithubDeliveryTargetIntent
	submit   func(context.Context, utils.DurableJobGraphRequest) error
}

func (d DiggerController) processGithubCheckSubmission(ctx context.Context, delivery *models.GithubWebhookDelivery, event *github.CheckRunEvent) error {
	identity := models.JobCreationIdentity{DeliveryOperationID: delivery.OperationID, DeliveryLeaseID: delivery.LeaseID,
		DatabaseIdentity: d.ControlPlaneDatabaseIdentity, WriterEpoch: d.ControlPlaneWriterEpoch, ProtocolVersion: operation.ProtocolVersion}
	stored, err := models.DB.GetGithubSubmission(ctx, identity)
	if err == nil {
		return d.resumeGithubSubmission(ctx, identity, stored)
	}
	if !errors.Is(err, models.ErrGithubSubmissionNotFound) {
		return err
	}
	target, err := prepareGithubDeliveryTarget(ctx, identity, delivery, models.DB, nil)
	if err != nil {
		return err
	}
	intent, err := models.DecodeGithubDeliveryTarget(target.Target)
	if err != nil {
		return err
	}
	preparation := &githubCheckSubmissionPreparation{identity: identity, target: intent,
		submit: func(ctx context.Context, request utils.DurableJobGraphRequest) error {
			graph, err := utils.PrepareDurableGraphIntent(request)
			if err != nil {
				return err
			}
			submission, err := utils.PrepareGithubSubmissionWithReports(models.GithubSubmissionIntent{Graph: *graph}, delivery.GithubAppID, time.Now().UTC())
			if err != nil {
				return err
			}
			stored, _, err := models.DB.RecordGithubSubmission(ctx, identity, submission)
			if err != nil {
				return err
			}
			return d.resumeGithubSubmission(ctx, identity, stored)
		}}
	return handleCheckRunActionEventMode(ctx, d.GithubClientProvider, event.GetRequestedAction().Identifier, event, d.CiBackendProvider, delivery.GithubAppID, preparation)
}

func (d DiggerController) resumeGithubSubmission(ctx context.Context, identity models.JobCreationIdentity, stored *models.GithubSubmission) error {
	intent, err := models.DecodeGithubSubmissionIntent(stored.Intent)
	if err != nil {
		return err
	}
	if _, err := models.DB.EnqueueGithubSubmissionReports(ctx, identity); err != nil {
		return err
	}
	if d.OutboxDispatcher != nil {
		d.OutboxDispatcher.Wake()
	}
	_, ready, err := models.DB.ReadGithubSubmissionReportReceipts(ctx, identity)
	if err != nil {
		return err
	}
	if !ready {
		return errGithubSubmissionReportsPending
	}
	// Report IDs stay in their receipts, outside the frozen execution graph.
	// Graph creation atomically publishes the initial workflow dispatches.
	if _, _, err := utils.CreateDurableGraphFromIntent(ctx, identity, intent.Graph); err != nil {
		return err
	}
	if d.OutboxDispatcher != nil {
		d.OutboxDispatcher.Wake()
	}
	return nil
}
