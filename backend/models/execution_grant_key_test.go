package models

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestExecutionGrantKeysRequireSharedFingerprintsAndRetainedSecrets(t *testing.T) {
	connection, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "keys.sqlite")), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := connection.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	database := &Database{GormDB: connection}
	require.NoError(t, connection.AutoMigrate(&ExecutionGrantKey{}))
	// This query-only fixture isolates readiness from the execution-claim graph.
	// The controller integration test uses the complete PostgreSQL schema.
	require.NoError(t, connection.Exec("CREATE TABLE execution_claim_attempts (signing_key_id text, state text, grant_expires_at datetime)").Error)
	oldSecret, newSecret := bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 32)
	keys := map[string][]byte{"old": oldSecret, "new": newSecret}
	require.ErrorIs(t, database.ValidateExecutionGrantKeys(context.Background(), keys, "new"), ErrExecutionGrantKeysNotReady)
	for id, secret := range keys {
		require.NoError(t, connection.Create(&ExecutionGrantKey{KeyID: id, SecretSHA256: ExecutionGrantSecretFingerprint(secret), RegisteredAt: time.Now().UTC()}).Error)
	}
	require.NoError(t, database.ValidateExecutionGrantKeys(context.Background(), keys, "new"))
	require.ErrorIs(t, database.ValidateExecutionGrantKeys(context.Background(), map[string][]byte{"new": oldSecret}, "new"), ErrExecutionGrantKeysNotReady)
	require.NoError(t, connection.Exec("INSERT INTO execution_claim_attempts VALUES (?, ?, ?)", "old", ExecutionClaimGranted, time.Now().UTC().Add(time.Hour)).Error)
	require.ErrorIs(t, database.ValidateExecutionGrantKeys(context.Background(), map[string][]byte{"new": newSecret}, "new"), ErrExecutionGrantKeysNotReady)
	require.NoError(t, database.ValidateExecutionGrantKeys(context.Background(), keys, "new"))
	require.NoError(t, connection.Exec("UPDATE execution_claim_attempts SET grant_expires_at = ?", time.Now().UTC().Add(-time.Hour)).Error)
	require.NoError(t, database.ValidateExecutionGrantKeys(context.Background(), map[string][]byte{"new": newSecret}, "new"))
	require.ErrorIs(t, database.ValidateExecutionGrantKeys(context.Background(), keys, "unknown"), ErrExecutionGrantKeysNotReady)
}
