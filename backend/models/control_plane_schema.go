package models

import (
	"context"
	"fmt"

	"gorm.io/gorm"
)

// CheckDurableControlPlaneSchema verifies the durable worker tables. Explicit model fields catch
// partially applied migrations without reading payloads, tokens, or evidence.
func (db *Database) CheckDurableControlPlaneSchema(ctx context.Context) error {
	checks := []struct {
		name string
		rows any
	}{
		{"writer fence", &[]ControlPlaneFence{}},
		{"ordering domains", &[]GithubWebhookOrderingDomain{}},
		{"webhook deliveries", &[]GithubWebhookDelivery{}},
		{"webhook requeues", &[]GithubWebhookDeliveryRequeue{}},
		{"control operations", &[]ControlOperation{}},
		{"batches", &[]DiggerBatch{}},
		{"jobs", &[]DiggerJob{}},
		{"job tokens", &[]JobToken{}},
		{"job dependencies", &[]DiggerJobParentLink{}},
		{"execution claims", &[]ExecutionClaimAttempt{}},
		{"grant keys", &[]ExecutionGrantKey{}},
		{"outbox effects", &[]OutboxEffect{}},
		{"status callbacks", &[]JobStatusCallback{}},
		{"apply recoveries", &[]ApplyRecovery{}},
		{"GitHub submissions", &[]GithubSubmission{}},
		{"GitHub report attempts", &[]GithubReportCreateAttempt{}},
		{"GitHub report receipts", &[]GithubReportReceipt{}},
	}
	for _, check := range checks {
		if err := db.GormDB.WithContext(ctx).Session(&gorm.Session{QueryFields: true}).Where("1 = 0").Find(check.rows).Error; err != nil {
			return fmt.Errorf("durable %s schema unavailable: %w", check.name, err)
		}
	}
	return nil
}
