package models

import (
	"encoding/json"
	"sort"
	"strings"
	"unicode/utf8"

	configuration "github.com/diggerhq/digger/libs/digger_config"
	"gorm.io/gorm"
)

// GithubSubmissionDetection preserves existing project-history inputs alongside
// the selected submission. Database row IDs and timestamps are assigned on insert.
type GithubSubmissionDetection struct {
	DefaultBranch string                                          `json:"default_branch"`
	Labels        []string                                        `json:"labels"`
	ChangedFiles  []string                                        `json:"changed_files"`
	Projects      []configuration.Project                         `json:"projects"`
	SourceMapping map[string]configuration.ProjectToSourceMapping `json:"source_mapping"`
}

func normalizeGithubSubmissionDetection(detection *GithubSubmissionDetection) error {
	if detection == nil {
		return nil
	}
	if strings.TrimSpace(detection.DefaultBranch) == "" || !utf8.ValidString(detection.DefaultBranch) {
		return ErrGithubSubmissionIntent
	}
	seen := make(map[string]bool)
	for _, project := range detection.Projects {
		if project.Name == "" || project.Name != strings.TrimSpace(project.Name) || !utf8.ValidString(project.Name) || seen[project.Name] {
			return ErrGithubSubmissionIntent
		}
		seen[project.Name] = true
	}
	sort.Slice(detection.Projects, func(i, j int) bool { return detection.Projects[i].Name < detection.Projects[j].Name })
	for _, values := range [][]string{detection.Labels, detection.ChangedFiles} {
		for _, value := range values {
			if !utf8.ValidString(value) {
				return ErrGithubSubmissionIntent
			}
		}
	}
	return nil
}

func recordGithubSubmissionDetection(tx *gorm.DB, identity JobCreationIdentity, delivery *GithubWebhookDelivery, orgID uint, detection *GithubSubmissionDetection) error {
	if detection == nil {
		return nil
	}
	target, err := loadGithubDeliveryTargetIntentTx(tx, identity, delivery, orgID)
	if err != nil {
		return err
	}
	var event struct {
		Action string `json:"action"`
	}
	if err := json.Unmarshal(delivery.Payload, &event); err != nil {
		return err
	}
	if delivery.EventType != "pull_request" && delivery.EventType != "issue_comment" {
		return ErrGithubSubmissionIntent
	}
	action := event.Action
	if delivery.EventType == "issue_comment" {
		action = "comment"
	}
	run, err := NewDetectionRun(orgID, delivery.RepositoryFullName, target.PullRequestNumber, delivery.EventType, action, target.HeadSHA,
		detection.DefaultBranch, target.BaseRef, detection.Labels, detection.ChangedFiles, detection.Projects, detection.SourceMapping)
	if err != nil {
		return err
	}
	if err := tx.Create(run).Error; err != nil {
		return err
	}
	// Preserve project progress records without resetting previously planned or
	// applied projects. Durable submissions serialize inserts for this repository.
	if tx.Dialector.Name() == "postgres" {
		if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", "digger-impact:"+delivery.RepositoryFullName).Error; err != nil {
			return err
		}
	}
	database := &Database{GormDB: tx}
	for _, project := range detection.Projects {
		var count int64
		if err := tx.Model(&ImpactedProject{}).Where("repo_full_name = ? AND commit_sha = ? AND project_name = ?", delivery.RepositoryFullName, target.HeadSHA, project.Name).Count(&count).Error; err != nil {
			return err
		}
		if count != 0 {
			continue
		}
		if _, err := database.CreateImpactedProject(delivery.RepositoryFullName, target.HeadSHA, project.Name, &target.HeadRef, &target.PullRequestNumber); err != nil {
			return err
		}
	}
	return nil
}
