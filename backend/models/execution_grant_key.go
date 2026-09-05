package models

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"gorm.io/gorm"
)

var ErrExecutionGrantKeysNotReady = errors.New("execution grant keys do not match the shared registry")

// Provisioned by the deployment operator before a key is used. Application
// writers only read these fingerprints; key IDs must never be reassigned.
type ExecutionGrantKey struct {
	KeyID        string    `gorm:"primaryKey;type:text;check:execution_grant_keys_key_id_check,length(key_id) BETWEEN 1 AND 128 AND trim(key_id) = key_id"`
	SecretSHA256 string    `gorm:"type:text;not null;check:execution_grant_keys_fingerprint_check,length(secret_sha256) = 64 AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(secret_sha256,'0',''),'1',''),'2',''),'3',''),'4',''),'5',''),'6',''),'7',''),'8',''),'9',''),'a',''),'b',''),'c',''),'d',''),'e',''),'f','')) = 0"`
	RegisteredAt time.Time `gorm:"not null"`
}

func (ExecutionGrantKey) TableName() string { return "execution_grant_keys" }

func ExecutionGrantSecretFingerprint(secret []byte) string {
	digest := sha256.New()
	digest.Write([]byte("digger-execution-grant-key\x00"))
	digest.Write(secret)
	return hex.EncodeToString(digest.Sum(nil))
}

// Readiness requires every configured key to match its pre-provisioned identity
// and every still-valid grant to remain reproducible on this instance.
func (db *Database) ValidateExecutionGrantKeys(ctx context.Context, secrets map[string][]byte, activeKeyID string) error {
	if db == nil || db.GormDB == nil || !validGrantSigningKeyID(activeKeyID) || len(secrets[activeKeyID]) < 32 {
		return ErrExecutionGrantKeysNotReady
	}
	keyIDs := make([]string, 0, len(secrets))
	for keyID, secret := range secrets {
		if !validGrantSigningKeyID(keyID) || len(secret) < 32 {
			return ErrExecutionGrantKeysNotReady
		}
		keyIDs = append(keyIDs, keyID)
	}
	return db.GormDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		now, err := databaseTransactionNow(tx, time.Now().UTC())
		if err != nil {
			return err
		}
		var registered []ExecutionGrantKey
		if err := tx.Where("key_id IN ?", keyIDs).Find(&registered).Error; err != nil {
			return err
		}
		if len(registered) != len(secrets) {
			return ErrExecutionGrantKeysNotReady
		}
		for _, key := range registered {
			if key.SecretSHA256 != ExecutionGrantSecretFingerprint(secrets[key.KeyID]) {
				return ErrExecutionGrantKeysNotReady
			}
		}
		var required []string
		if err := tx.Model(&ExecutionClaimAttempt{}).Distinct("signing_key_id").Where("state = ? AND grant_expires_at > ?", ExecutionClaimGranted, now).Pluck("signing_key_id", &required).Error; err != nil {
			return err
		}
		for _, keyID := range required {
			if len(secrets[keyID]) < 32 {
				return ErrExecutionGrantKeysNotReady
			}
		}
		return nil
	})
}
