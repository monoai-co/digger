package models

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPostgresGithubReportOutboxCanonicalReplayAndConflict(t *testing.T) {
	database := newPostgresOutboxTestDatabase(t)
	createOutboxTestOperation(t, database, "report-operation")
	const payload = `{"organisation_id":1,"github_app_id":2,"github_installation_id":3,"repo_owner":"owner","repo_name":"repo","pull_request_number":42,"head_sha":"commit","resource_kind":"check_run","body":"","check":{"name":"digger/plan","status":"in_progress","conclusion":"","title":"Pending start...","summary":"","text":"Waiting for plan","actions":null}}`
	first := NewOutboxEffect("report-operation", GithubReportCreateEffectKind, "batch-check:plan", []byte(payload), testControlPlaneWriterEpoch, time.Now().UTC())
	stored, created, err := database.EnqueueOutboxEffect(context.Background(), first, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)
	require.True(t, created)
	var expanded any
	require.NoError(t, json.Unmarshal([]byte(strings.Replace(payload, `"actions":null`, `"actions":[]`, 1)), &expanded))
	formatted, err := json.MarshalIndent(expanded, "", "  ")
	require.NoError(t, err)
	replay := NewOutboxEffect("report-operation", GithubReportCreateEffectKind, "batch-check:plan", formatted, testControlPlaneWriterEpoch, time.Now().UTC())
	loaded, created, err := database.EnqueueOutboxEffect(context.Background(), replay, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, stored.ID, loaded.ID)
	require.Equal(t, stored.PayloadSHA256, loaded.PayloadSHA256)
	require.True(t, loaded.ValidPayloadDigest())
	changed := NewOutboxEffect("report-operation", GithubReportCreateEffectKind, "batch-check:plan", []byte(strings.Replace(payload, "Waiting for plan", "Different intent", 1)), testControlPlaneWriterEpoch, time.Now().UTC())
	_, created, err = database.EnqueueOutboxEffect(context.Background(), changed, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
	require.ErrorIs(t, err, ErrOutboxEffectConflict)
	require.False(t, created)
}

func TestPostgresGithubReportOutboxRejectsInvalidPayloadBeforeInsert(t *testing.T) {
	const valid = `{"organisation_id":1,"github_app_id":2,"github_installation_id":3,"repo_owner":"owner","repo_name":"repo","pull_request_number":42,"resource_kind":"comment","body":"Prepared report","check":null}`
	for _, invalid := range []string{
		strings.Replace(valid, `"organisation_id":1`, `"organisation_id":0`, 1),
		strings.Replace(valid, `"resource_kind":"comment"`, `"resource_kind":"unknown"`, 1),
		strings.Replace(valid, `"body":"Prepared report"`, `"body":""`, 1),
		strings.Replace(valid, `"check":null`, `"check":{"name":"digger/plan"}`, 1),
		strings.Replace(valid, `"check":null`, `"provider_id":99,"check":null`, 1),
		valid + ` {}`,
	} {
		database := newPostgresOutboxTestDatabase(t)
		createOutboxTestOperation(t, database, "report-operation")
		effect := NewOutboxEffect("report-operation", GithubReportCreateEffectKind, "summary", []byte(invalid), testControlPlaneWriterEpoch, time.Now().UTC())
		_, _, err := database.EnqueueOutboxEffect(context.Background(), effect, testControlPlaneDatabaseIdentity, testControlPlaneWriterEpoch)
		require.Error(t, err)
		var count int64
		require.NoError(t, database.GormDB.Model(&OutboxEffect{}).Count(&count).Error)
		require.Zero(t, count)
	}
}
