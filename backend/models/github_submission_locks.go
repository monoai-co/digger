package models

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"gorm.io/gorm"
)

var ErrGithubSubmissionLockConflict = errors.New("project is locked by a different pull request")

// GithubSubmissionLocks is committed only with a new submission. Replay must not
// reacquire a lock that a later PR-close delivery has already released.
type GithubSubmissionLocks struct {
	Acquire    []string `json:"acquire"`
	ReleaseAll bool     `json:"release_all"`
}

func normalizeGithubSubmissionLocks(locks *GithubSubmissionLocks) error {
	if locks == nil {
		return nil
	}
	if locks.ReleaseAll == (len(locks.Acquire) > 0) {
		return ErrGithubSubmissionIntent
	}
	if err := sortGithubSubmissionNames(&locks.Acquire); err != nil {
		return err
	}
	for _, name := range locks.Acquire {
		if !utf8.ValidString(name) || len(name) > 1024 || name != strings.TrimSpace(name) {
			return ErrGithubSubmissionIntent
		}
	}
	return nil
}

func applyGithubSubmissionLocks(tx *gorm.DB, identity JobCreationIdentity, delivery *GithubWebhookDelivery, orgID uint, locks *GithubSubmissionLocks) error {
	if locks == nil {
		return nil
	}
	target, err := loadGithubDeliveryTargetIntentTx(tx, identity, delivery, orgID)
	if err != nil {
		return err
	}
	namespace := target.RepoOwner + "/" + target.RepoName + "#"
	if tx.Dialector.Name() == "postgres" {
		// Missing lock rows cannot be row-locked. Serialize durable lock mutations
		// by tenant/repository, including the initial insert and whole-PR release.
		if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", fmt.Sprintf("digger-pr-lock:%d:%s", orgID, namespace)).Error; err != nil {
			return err
		}
	}
	if locks.ReleaseAll {
		var held []DiggerLock
		if err := tx.Where("organisation_id = ? AND lock_id = ?", orgID, target.PullRequestNumber).Find(&held).Error; err != nil {
			return err
		}
		for _, lock := range held {
			// Do not use LIKE: underscores in repository names are SQL wildcards,
			// and the # delimiter distinguishes repo from repo-with-a-suffix.
			if !strings.HasPrefix(lock.Resource, namespace) {
				continue
			}
			if err := tx.Where("id = ? AND organisation_id = ? AND lock_id = ?", lock.ID, orgID, target.PullRequestNumber).Delete(&DiggerLock{}).Error; err != nil {
				return err
			}
		}
		return nil
	}
	for _, project := range locks.Acquire {
		resource := namespace + project
		var existing []DiggerLock
		if err := tx.Where("organisation_id = ? AND resource = ?", orgID, resource).Find(&existing).Error; err != nil {
			return err
		}
		for _, lock := range existing {
			if lock.LockId != target.PullRequestNumber {
				return ErrGithubSubmissionLockConflict
			}
		}
		if len(existing) != 0 {
			continue
		}
		if err := tx.Omit("Organisation").Create(&DiggerLock{OrganisationID: orgID, Resource: resource, LockId: target.PullRequestNumber}).Error; err != nil {
			return err
		}
	}
	return nil
}
