package models

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"unicode/utf8"

	"gorm.io/gorm"
)

var ErrGithubSubmissionLockConflict = errors.New("project is locked by a different pull request")

// GithubSubmissionLocks is committed only with a new submission. Replay must not
// reacquire a lock that a later PR-close delivery has already released.
type GithubSubmissionLocks struct {
	Acquire      []string                    `json:"acquire"`
	ReleaseAll   bool                        `json:"release_all"`
	ClosedOwners []GithubSubmissionLockOwner `json:"closed_owners,omitempty"`
}

// GithubSubmissionLockOwner binds a closed-PR observation to one exact lock row.
// A replacement row, even for the same project or PR, requires a fresh observation.
type GithubSubmissionLockOwner struct {
	ID                uint   `json:"id"`
	Project           string `json:"project"`
	PullRequestNumber int    `json:"pull_request_number"`
}

func (db *Database) ReadGithubSubmissionLockOwners(ctx context.Context, identity JobCreationIdentity, projects []string) ([]GithubSubmissionLockOwner, error) {
	locks := &GithubSubmissionLocks{Acquire: append([]string(nil), projects...)}
	if err := normalizeGithubSubmissionLocks(locks); err != nil {
		return nil, err
	}
	owners := []GithubSubmissionLockOwner{}
	err := db.WithAuthoritativeWriteTx(ctx, identity.DatabaseIdentity, identity.WriterEpoch, false, func(tx *gorm.DB, _ *ControlPlaneFence) error {
		delivery, orgID, _, err := lockGithubPreparationDelivery(tx, identity)
		if err != nil {
			return err
		}
		target, err := loadGithubDeliveryTargetIntentTx(tx, identity, delivery, orgID)
		if err != nil {
			return err
		}
		for _, project := range locks.Acquire {
			var rows []DiggerLock
			if err := tx.Where("organisation_id = ? AND resource = ? AND lock_id <> ?", orgID, target.RepoOwner+"/"+target.RepoName+"#"+project, target.PullRequestNumber).Order("id").Find(&rows).Error; err != nil {
				return err
			}
			for _, row := range rows {
				owners = append(owners, GithubSubmissionLockOwner{ID: row.ID, Project: project, PullRequestNumber: row.LockId})
			}
		}
		return nil
	})
	return owners, err
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
	seen := make(map[uint]bool)
	for _, owner := range locks.ClosedOwners {
		if owner.ID == 0 || owner.PullRequestNumber <= 0 || seen[owner.ID] || !slices.Contains(locks.Acquire, owner.Project) {
			return ErrGithubSubmissionIntent
		}
		seen[owner.ID] = true
	}
	sort.Slice(locks.ClosedOwners, func(i, j int) bool { return locks.ClosedOwners[i].ID < locks.ClosedOwners[j].ID })
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
		owned := false
		for _, lock := range existing {
			if lock.LockId != target.PullRequestNumber {
				observation := GithubSubmissionLockOwner{ID: lock.ID, Project: project, PullRequestNumber: lock.LockId}
				if !slices.Contains(locks.ClosedOwners, observation) {
					return ErrGithubSubmissionLockConflict
				}
				if err := tx.Where("id = ? AND organisation_id = ? AND resource = ? AND lock_id = ?", lock.ID, orgID, resource, lock.LockId).Delete(&DiggerLock{}).Error; err != nil {
					return err
				}
			} else {
				owned = true
			}
		}
		if owned {
			continue
		}
		if err := tx.Omit("Organisation").Create(&DiggerLock{OrganisationID: orgID, Resource: resource, LockId: target.PullRequestNumber}).Error; err != nil {
			return err
		}
	}
	return nil
}
