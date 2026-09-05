package models

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/google/go-github/v61/github"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrGithubCheckActionBinding = errors.New("github check action does not match its persisted batch")

func (db *Database) ResolveGithubCheckDeliveryTarget(ctx context.Context, delivery *GithubWebhookDelivery) (GithubDeliveryTargetIntent, error) {
	preparation, err := PrepareGithubDeliveryTargetIntent(delivery)
	if err != nil {
		return GithubDeliveryTargetIntent{}, err
	}
	if preparation.checkAction == nil {
		return GithubDeliveryTargetIntent{}, ErrGithubDeliveryTargetUnsupported
	}
	batch, _, err := db.ResolveLegacyGithubCheckAction(ctx, preparation.checkAction, delivery.GithubAppID)
	if err != nil {
		return GithubDeliveryTargetIntent{}, err
	}
	target := preparation.target
	target.PullRequestNumber, target.HeadRef = batch.PrNumber, batch.BranchName
	return target, nil
}

// ResolveLegacyGithubCheckAction preserves existing apply buttons without
// trusting their short batch identifier as authority to select a repository.
func (db *Database) ResolveLegacyGithubCheckAction(ctx context.Context, event *github.CheckRunEvent, appID int64) (*DiggerBatch, []DiggerJob, error) {
	if event == nil || event.GetAction() != "requested_action" || event.GetRequestedAction() == nil || event.GetRepo() == nil ||
		event.GetCheckRun().GetApp().GetID() != appID || appID <= 0 || event.GetInstallation().GetID() <= 0 {
		return nil, nil, ErrGithubCheckActionBinding
	}
	identifier := event.GetRequestedAction().Identifier
	batchID, ok := strings.CutPrefix(identifier, "abatch:")
	if !ok || batchID == "" || strings.Contains(batchID, ":") {
		return nil, nil, ErrGithubCheckActionBinding
	}
	var batches []DiggerBatch
	query := db.GormDB.WithContext(ctx)
	if query.Dialector.Name() == "postgres" {
		query = query.Clauses(clause.Locking{Strength: "SHARE"})
	}
	if err := query.Session(&gorm.Session{}).Where("digger_batch_id = ? AND vcs = ? AND repo_full_name = ? AND github_installation_id = ?",
		batchID, DiggerVCSGithub, event.GetRepo().GetFullName(), event.GetInstallation().GetID()).Limit(2).Find(&batches).Error; err != nil {
		return nil, nil, err
	}
	if len(batches) != 1 {
		return nil, nil, ErrGithubCheckActionBinding
	}
	batch := &batches[0]
	var jobs []DiggerJob
	if err := query.Session(&gorm.Session{}).Where("batch_id = ?", batch.ID.String()).Find(&jobs).Error; err != nil {
		return nil, nil, err
	}
	if err := validateLegacyGithubCheckAction(event, appID, batch, jobs); err != nil {
		return nil, nil, err
	}
	return batch, jobs, nil
}

func validateLegacyGithubCheckAction(event *github.CheckRunEvent, appID int64, batch *DiggerBatch, jobs []DiggerJob) error {
	if event == nil || batch == nil || batch.ID == uuid.Nil || batch.OperationID != nil || len(jobs) == 0 || event.GetRequestedAction() == nil ||
		event.GetAction() != "requested_action" || event.GetRequestedAction().Identifier != "abatch:"+batch.DiggerBatchID || batch.DiggerBatchID == "" ||
		appID <= 0 || event.GetCheckRun().GetApp().GetID() != appID || event.GetCheckRun().GetID() <= 0 ||
		batch.GithubInstallationId <= 0 || event.GetInstallation().GetID() != batch.GithubInstallationId || batch.VCS != DiggerVCSGithub ||
		batch.PrNumber <= 0 || !validGithubDeliveryHeadRef(batch.BranchName) || !validGithubReportPathSegment(batch.CommitSha) || event.GetCheckRun().GetHeadSHA() != batch.CommitSha {
		return ErrGithubCheckActionBinding
	}
	repo := event.GetRepo()
	if repo.GetID() <= 0 || !validGithubReportPathSegment(batch.RepoOwner) || !validGithubReportPathSegment(batch.RepoName) ||
		repo.GetOwner().GetLogin() != batch.RepoOwner || repo.GetName() != batch.RepoName || repo.GetFullName() != batch.RepoFullName || batch.RepoFullName != batch.RepoOwner+"/"+batch.RepoName {
		return ErrGithubCheckActionBinding
	}
	checkID := strconv.FormatInt(event.GetCheckRun().GetID(), 10)
	matched := batch.CheckRunId != nil && *batch.CheckRunId == checkID
	for _, job := range jobs {
		if job.BatchID == nil || *job.BatchID != batch.ID.String() || job.OperationID != nil {
			return ErrGithubCheckActionBinding
		}
		matched = matched || (job.CheckRunId != nil && *job.CheckRunId == checkID)
	}
	if !matched {
		return ErrGithubCheckActionBinding
	}
	return nil
}
